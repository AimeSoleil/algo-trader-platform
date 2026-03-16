"""GitHub Copilot SDK LLM Provider — native skill mounting via skill_directories."""
from __future__ import annotations

import asyncio
import json
import re
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path

from shared.config import get_settings
from shared.models.blueprint import LLMTradingBlueprint
from shared.models.signal import SignalFeatures
from shared.utils import get_logger, now_utc, next_trading_day as _next_trading_day

from services.analysis_service.app.llm.base import LLMProviderBase
from services.analysis_service.app.llm.prompts import SYSTEM_PROMPT, build_blueprint_prompt

logger = get_logger("copilot_provider")

# Directory containing the trading-analysis/ skill subdirectory
_SKILLS_DIR = str(Path(__file__).resolve().parents[1] / "skills")


def _resolve_cli_path(configured_cli: str) -> str:
    """Resolve configured Copilot CLI to an executable path when possible.

    This avoids PATH mismatch issues between interactive shell and Celery workers.
    """
    cli = configured_cli.strip() if configured_cli else "copilot"
    if Path(cli).is_absolute() or "/" in cli:
        return cli

    resolved = shutil.which(cli)
    if resolved:
        return resolved
    return cli


def _build_structured_prompt(user_prompt: str) -> str:
    """Separate system/user intent explicitly for providers without native system role."""
    return (
        "<system>\n"
        f"{SYSTEM_PROMPT}\n"
        "</system>\n\n"
        "<user>\n"
        f"{user_prompt}\n\n"
        "Final output: return ONLY one valid JSON object, no markdown fences and no extra text.\n"
        "</user>"
    )


def _parse_blueprint_json(response_text: str) -> dict:
    """Parse JSON robustly from Copilot responses.

    Handles plain JSON, markdown code fences, and noisy wrappers.
    """
    text = response_text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
        text = text.rsplit("```", 1)[0]

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{[\s\S]*\}", text)
    if not match:
        raise ValueError("No JSON object found in Copilot response")

    return json.loads(match.group(0))

class CopilotProvider(LLMProviderBase):
    """Copilot SDK provider with native skill mounting.

    SDK docs: https://github.com/github/copilot-sdk
    """

    def __init__(self):
        settings = get_settings()
        self.cli_path = _resolve_cli_path(settings.llm.copilot.cli_path)
        self.github_token = settings.llm.copilot.github_token
        self.model = settings.llm.copilot.model
        reasoning_effort = settings.llm.copilot.reasoning_effort.lower()
        if reasoning_effort not in {"low", "medium", "high", "xhigh"}:
            reasoning_effort = "medium"
        self.reasoning_effort = reasoning_effort
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
                logger.info("copilot.client_started", cli_path=self.cli_path)
            except ImportError:
                logger.error("copilot.sdk_not_installed")
                raise ImportError(
                    "Copilot SDK import failed. Install/upgrade with: pip install -U github-copilot-sdk"
                )
            except Exception as e:
                logger.error("copilot.client_start_failed", cli_path=self.cli_path, error=str(e))
                raise
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
        full_prompt = _build_structured_prompt(prompt)

        max_retries = 3
        for attempt in range(max_retries):
            try:
                session = await client.create_session({
                    "model": self.model,
                    "reasoning_effort": self.reasoning_effort,
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

                blueprint_data = _parse_blueprint_json(response_text)

                # Add metadata
                blueprint_data["trading_date"] = _next_trading_day().isoformat()
                blueprint_data["generated_at"] = now_utc().isoformat()
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
