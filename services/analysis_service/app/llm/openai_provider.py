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


@lru_cache(maxsize=1)
def _load_always_refs() -> str:
    """Load always-needed reference docs for inlining into instructions."""
    always_load = [
        "trend-momentum.md",
        "volatility-analysis.md",
        "flow-microstructure.md",
        "risk-management.md",
    ]
    refs_dir = _SKILL_DIR / "references"
    parts: list[str] = []
    for filename in always_load:
        path = refs_dir / filename
        if path.exists():
            parts.append(f"### {filename}\n{path.read_text(encoding='utf-8').strip()}")
    return "\n\n".join(parts)


def _build_hybrid_instructions() -> str:
    """Embed workflow + always-load references in instructions; keep conditional refs as shell files.

    Always-load refs (trend-momentum, volatility-analysis, flow-microstructure,
    risk-management) are inlined to avoid shell latency and ensure the model
    always sees them.  Conditional refs (option-chain-structure, spread-arbitrage,
    cross-asset) remain as shell files to be loaded on-demand.
    """
    skill_md = _load_skill_md().strip()
    always_refs = _load_always_refs()

    parts: list[str] = [SYSTEM_PROMPT]

    if skill_md:
        parts.append(
            "## Trading Analysis Workflow (from SKILL.md)\n\n"
            f"{skill_md}"
        )

    if always_refs:
        parts.append(
            "## Core Reference Analyses (always loaded)\n\n"
            f"{always_refs}"
        )

    parts.append(
        "## Conditional References\n\n"
        "These reference files are mounted in the shell under "
        "trading-analysis/references/ — load them only when needed:\n"
        "- option-chain-structure.md — load when option liquidity data is available\n"
        "- spread-arbitrage.md — load when evaluating multi-leg strategies\n"
        "- cross-asset.md — load when cross-asset indicators are available"
    )

    return "\n\n".join(parts)


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
        import asyncio
        import random
        from time import perf_counter

        from pydantic import ValidationError

        from shared.metrics import (
            llm_request_duration,
            llm_retries_total,
            llm_tokens_total,
        )

        settings = get_settings()
        max_retries = settings.llm.max_retries
        backoff_base = settings.llm.backoff_base_seconds
        backoff_max = settings.llm.backoff_max_seconds

        prompt = build_blueprint_prompt(
            signal_features, current_positions, previous_execution,
            chunk_mode=chunk_mode,
        )

        last_exc: Exception | None = None
        for attempt in range(max_retries):
            t0 = perf_counter()
            status = "error"
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

                status = "ok"
                # Record token metrics
                if response.usage:
                    llm_tokens_total.labels(provider="openai", direction="prompt").inc(
                        response.usage.input_tokens or 0,
                    )
                    llm_tokens_total.labels(provider="openai", direction="completion").inc(
                        response.usage.output_tokens or 0,
                    )

                logger.info(
                    "openai.blueprint_generated",
                    trading_date=str(blueprint.trading_date),
                    plans=len(blueprint.symbol_plans),
                    tokens=response.usage.total_tokens if response.usage else 0,
                )
                return blueprint

            except (json.JSONDecodeError, ValidationError) as e:
                # Parse / validation errors — retrying won't help
                llm_retries_total.labels(provider="openai", error_type="parse").inc()
                logger.warning(
                    "openai.parse_error",
                    attempt=attempt + 1,
                    error=str(e),
                    error_type=type(e).__name__,
                )
                raise  # fail fast

            except Exception as e:
                last_exc = e
                # Check if retryable
                error_type = type(e).__name__
                retryable_types = (
                    "RateLimitError",
                    "APITimeoutError",
                    "APIConnectionError",
                    "InternalServerError",
                )
                is_retryable = error_type in retryable_types or (
                    hasattr(e, "status_code") and getattr(e, "status_code", 0) >= 500
                )

                if is_retryable and attempt < max_retries - 1:
                    delay = min(
                        backoff_base * (2 ** attempt) + random.uniform(0, 1),
                        backoff_max,
                    )
                    llm_retries_total.labels(provider="openai", error_type=error_type).inc()
                    logger.warning(
                        "openai.retryable_error",
                        attempt=attempt + 1,
                        error=str(e),
                        error_type=error_type,
                        retry_delay_s=round(delay, 2),
                    )
                    await asyncio.sleep(delay)
                    continue

                # Non-retryable or last attempt
                logger.warning(
                    "openai.generation_failed",
                    attempt=attempt + 1,
                    error=str(e),
                    error_type=error_type,
                    retryable=is_retryable,
                )
                raise

            finally:
                elapsed = perf_counter() - t0
                llm_request_duration.labels(
                    provider="openai", agent="blueprint", status=status,
                ).observe(elapsed)

        raise last_exc or RuntimeError("Failed to generate blueprint after retries")

    async def health_check(self) -> bool:
        try:
            await self.client.models.list()
            return True
        except Exception:
            return False
