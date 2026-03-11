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
from shared.utils import get_logger

from services.analysis_service.app.llm.base import LLMProviderBase
from services.analysis_service.app.llm.prompts import SYSTEM_PROMPT, build_blueprint_prompt

logger = get_logger("openai_provider")

# Path to the trading-analysis skill bundle
_SKILL_DIR = Path(__file__).resolve().parents[1] / "skills" / "trading-analysis"


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


# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------


class OpenAIProvider(LLMProviderBase):
    def __init__(self):
        settings = get_settings()
        self.client = AsyncOpenAI(api_key=settings.llm.openai_api_key)
        self.model = settings.llm.openai_model
        self.temperature = settings.llm.openai_temperature
        self.max_tokens = settings.llm.openai_max_tokens

        # Pre-build the skill bundle once
        self._skill_bundle = _build_skill_bundle()
        logger.info("openai.skill_bundle_built", size_kb=len(self._skill_bundle) // 1024)

    async def generate_blueprint(
        self,
        signal_features: list[SignalFeatures],
        current_positions: dict | None = None,
        previous_execution: dict | None = None,
    ) -> LLMTradingBlueprint:
        """Call OpenAI Responses API with the trading-analysis skill mounted."""
        prompt = build_blueprint_prompt(
            signal_features, current_positions, previous_execution
        )

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = await self.client.responses.create(
                    model=self.model,
                    instructions=SYSTEM_PROMPT,
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
                )

                content = response.output_text
                blueprint_data = json.loads(content)

                # Add metadata
                next_trading_day = date.today() + timedelta(days=1)
                while next_trading_day.weekday() >= 5:
                    next_trading_day += timedelta(days=1)

                blueprint_data["trading_date"] = next_trading_day.isoformat()
                blueprint_data["generated_at"] = datetime.now().isoformat()
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
