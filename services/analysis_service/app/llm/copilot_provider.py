"""GitHub Copilot SDK LLM Provider — native skill mounting via skill_directories."""
from __future__ import annotations

import asyncio
import json
from datetime import date, datetime, timedelta
from pathlib import Path

from shared.config import get_settings
from shared.models.blueprint import LLMTradingBlueprint
from shared.models.signal import SignalFeatures
from shared.utils import get_logger

from services.analysis_service.app.llm.base import LLMProviderBase
from services.analysis_service.app.llm.prompts import build_blueprint_prompt

logger = get_logger("copilot_provider")

# Directory containing the trading-analysis/ skill subdirectory
_SKILLS_DIR = str(Path(__file__).resolve().parents[1] / "skills")

# Minimal instruction prefix — the skill itself provides the full workflow
_SYSTEM_INSTRUCTION = """\
You are a professional options quantitative strategist. The trading-analysis \
skill is loaded. Follow its SKILL.md workflow, read the relevant references, \
and output a next-day Trading Blueprint as strict JSON (no markdown fences, \
no extra text). Every symbol_plan must include stop-loss conditions.
"""


class CopilotProvider(LLMProviderBase):
    """Copilot SDK provider with native skill mounting.

    SDK docs: https://github.com/github/copilot-sdk
    """

    def __init__(self):
        settings = get_settings()
        self.cli_path = settings.llm.copilot_cli_path
        self.github_token = settings.llm.copilot_github_token
        self.model = settings.llm.copilot_model
        self.temperature = settings.llm.copilot_temperature
        self.max_tokens = settings.llm.copilot_max_tokens
        self._client = None

    async def _get_client(self):
        """Lazy-init the CopilotClient."""
        if self._client is None:
            try:
                from copilot import CopilotClient

                config = {"cli_path": self.cli_path, "auto_start": True}
                if self.github_token:
                    config["github_token"] = self.github_token
                else:
                    config["use_logged_in_user"] = True

                self._client = CopilotClient(config)
                await self._client.start()
                logger.info("copilot.client_started")
            except ImportError:
                logger.error("copilot.sdk_not_installed")
                raise ImportError(
                    "copilot-sdk not installed. Install with: pip install copilot-sdk"
                )
        return self._client

    async def generate_blueprint(
        self,
        signal_features: list[SignalFeatures],
        current_positions: dict | None = None,
        previous_execution: dict | None = None,
    ) -> LLMTradingBlueprint:
        """Generate a next-day trading blueprint via Copilot SDK.

        The ``trading-analysis`` skill is loaded via ``skill_directories``
        so the SDK injects the SKILL.md content into session context
        automatically.  The model can then navigate references/ via
        file-system tools.
        """
        client = await self._get_client()
        prompt = build_blueprint_prompt(
            signal_features, current_positions, previous_execution
        )
        full_prompt = f"{_SYSTEM_INSTRUCTION}\n\n{prompt}"

        max_retries = 3
        for attempt in range(max_retries):
            try:
                session = await client.create_session({
                    "model": self.model,
                    "temperature": self.temperature,
                    "max_tokens": self.max_tokens,
                    "skill_directories": [_SKILLS_DIR],
                })

                result = await asyncio.wait_for(
                    session.send_and_wait({"prompt": full_prompt}),
                    timeout=120,
                )
                await session.disconnect()

                # Extract text from result
                response_text = (
                    result.content
                    if hasattr(result, "content")
                    else str(result)
                )

                # Strip markdown code blocks if present
                text = response_text.strip()
                if text.startswith("```"):
                    text = text.split("\n", 1)[1]
                    text = text.rsplit("```", 1)[0]

                blueprint_data = json.loads(text)

                # Add metadata
                next_trading_day = date.today() + timedelta(days=1)
                while next_trading_day.weekday() >= 5:
                    next_trading_day += timedelta(days=1)

                blueprint_data["trading_date"] = next_trading_day.isoformat()
                blueprint_data["generated_at"] = datetime.now().isoformat()
                blueprint_data["model_provider"] = "copilot"
                blueprint_data["model_version"] = "copilot-sdk"

                blueprint = LLMTradingBlueprint.model_validate(blueprint_data)
                logger.info(
                    "copilot.blueprint_generated",
                    trading_date=str(blueprint.trading_date),
                    plans=len(blueprint.symbol_plans),
                )
                return blueprint

            except Exception as e:
                logger.warning(
                    "copilot.generation_failed",
                    attempt=attempt + 1,
                    error=str(e),
                )
                if attempt == max_retries - 1:
                    raise

        raise RuntimeError("Copilot failed to generate blueprint after retries")

    async def health_check(self) -> bool:
        try:
            client = await self._get_client()
            return client is not None
        except Exception:
            return False
