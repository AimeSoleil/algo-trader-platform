"""OpenAI LLM Provider — Responses API with native skill mounting."""
from __future__ import annotations

import base64
import io
import json
import zipfile
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path

from openai import AsyncOpenAI

from shared.config import get_settings
from shared.models.blueprint import LLMTradingBlueprint
from shared.models.signal import SignalFeatures
from shared.utils import get_logger, now_utc, next_trading_day as _next_trading_day

from services.analysis_service.app.llm.base import LLMProviderBase
from services.analysis_service.app.llm.prompts import SYSTEM_PROMPT, build_blueprint_prompt

logger = get_logger("openai_provider")

# Path to the trading-analysis skill bundle
_SKILL_DIR = Path(__file__).resolve().parents[1] / "skills" / "trading-analysis"
_SKILL_MD_PATH = _SKILL_DIR / "SKILL.md"


# ---------------------------------------------------------------------------
# Skill bundle helpers
# ---------------------------------------------------------------------------


@lru_cache(maxsize=1)
def _build_skill_bundle() -> str:
    """Zip the trading-analysis/ directory and return a base64-encoded string.

    The bundle is cached for the lifetime of the process.  Call
    ``_build_skill_bundle.cache_clear()`` to force a rebuild.
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(_SKILL_DIR.rglob("*")):
            if path.is_file():
                arcname = f"trading-analysis/{path.relative_to(_SKILL_DIR)}"
                zf.write(path, arcname)
    return base64.b64encode(buf.getvalue()).decode("ascii")


@lru_cache(maxsize=1)
def _load_skill_md() -> str:
    """Load SKILL.md text once for hybrid instruction mounting."""
    if not _SKILL_MD_PATH.exists():
        logger.warning("openai.skill_md_missing", path=str(_SKILL_MD_PATH))
        return ""
    return _SKILL_MD_PATH.read_text(encoding="utf-8")


def _build_hybrid_instructions() -> str:
    """Embed workflow from SKILL.md in instructions; keep references as shell files."""
    skill_md = _load_skill_md().strip()
    if not skill_md:
        return SYSTEM_PROMPT

    return (
        f"{SYSTEM_PROMPT}\n\n"
        "Embedded trading-analysis workflow (from SKILL.md):\n"
        f"{skill_md}\n\n"
        "Reference files under trading-analysis/references are mounted in the shell environment. "
        "Load them on-demand based on market context."
    )


# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------


class OpenAIProvider(LLMProviderBase):
    def __init__(self):
        settings = get_settings()
        self.client = AsyncOpenAI(api_key=settings.llm.openai.api_key)
        self.model = settings.llm.openai.model
        self.temperature = settings.llm.openai.temperature
        self.max_tokens = settings.llm.openai.max_tokens
        self.request_timeout = settings.llm.openai.request_timeout_seconds

        # Pre-build the skill bundle once
        self._skill_bundle = _build_skill_bundle()
        self._instructions = _build_hybrid_instructions()
        logger.info("openai.skill_bundle_built", size_kb=len(self._skill_bundle) // 1024)

    async def generate_blueprint(
        self,
        signal_features: list[SignalFeatures],
        current_positions: dict | None = None,
        previous_execution: dict | None = None,
        *,
        chunk_mode: bool = False,
    ) -> LLMTradingBlueprint:
        """Call OpenAI Responses API with the trading-analysis skill mounted."""
        prompt = build_blueprint_prompt(
            signal_features, current_positions, previous_execution,
            chunk_mode=chunk_mode,
        )

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = await self.client.responses.create(
                    model=self.model,
                    instructions=self._instructions,
                    input=prompt,
                    tools=[
                        {
                            "type": "shell",
                            "environment": {
                                "type": "container_auto",
                                "skills": [
                                    {
                                        "type": "inline",
                                        "bundle": self._skill_bundle,
                                    }
                                ],
                            },
                        }
                    ],
                    text={"format": {"type": "json_object"}},
                    temperature=self.temperature,
                    max_output_tokens=self.max_tokens,
                    timeout=self.request_timeout,
                )

                content = response.output_text
                blueprint_data = json.loads(content)

                # Add metadata
                blueprint_data["trading_date"] = _next_trading_day().isoformat()
                blueprint_data["generated_at"] = now_utc().isoformat()
                blueprint_data["model_provider"] = "openai"
                blueprint_data["model_version"] = self.model

                blueprint = LLMTradingBlueprint.model_validate(blueprint_data)

                logger.info(
                    "openai.blueprint_generated",
                    trading_date=str(blueprint.trading_date),
                    plans=len(blueprint.symbol_plans),
                    tokens=response.usage.total_tokens if response.usage else 0,
                )
                return blueprint

            except Exception as e:
                logger.warning(
                    "openai.generation_failed",
                    attempt=attempt + 1,
                    error=str(e),
                )
                if attempt == max_retries - 1:
                    raise

        raise RuntimeError("Failed to generate blueprint after retries")

    async def health_check(self) -> bool:
        try:
            await self.client.models.list()
            return True
        except Exception:
            return False
