"""GitHub Copilot SDK LLM Provider — native skill mounting via skill_directories."""
from __future__ import annotations

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

# Models known to support the ``reasoning_effort`` session parameter.
_REASONING_EFFORT_SUPPORTED_PREFIXES: tuple[str, ...] = (
    "claude-",
    "o1",
    "o3",
    "o4",
)


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
    KNOWN_MODELS = [
        "claude-haiku-4.5",
        "claude-opus-4.5",
        "claude-opus-4.6",
        "claude-opus-4.6-fast",
        "claude-sonnet-4",
        "claude-sonnet-4.5",
        "gemini-3-pro-preview",
        "gpt-4.1",
        "gpt-5",
        "gpt-5-mini",
        "gpt-5.1",
        "gpt-5.1-codex",
        "gpt-5.1-codex-max",
        "gpt-5.1-codex-mini",
        "gpt-5.2",
        "gpt-5.2-codex",
        "gpt-5.3-codex",
    ]
    """

    def __init__(self):
        settings = get_settings()
        self.cli_path = _resolve_cli_path(settings.analysis_service.llm.copilot.cli_path)
        self.github_token = settings.analysis_service.llm.copilot.github_token
        self.model = settings.analysis_service.llm.copilot.model
        self.request_timeout = settings.analysis_service.llm.copilot.request_timeout_seconds
        reasoning_effort = settings.analysis_service.llm.copilot.reasoning_effort.lower()
        if reasoning_effort not in {"low", "medium", "high", "xhigh"}:
            reasoning_effort = "medium"
        self.reasoning_effort = reasoning_effort
        self._client = None
        self._on_permission_request = None

    async def _get_client(self):
        """Lazy-init the CopilotClient."""
        if self._client is None:
            try:
                from copilot import CopilotClient, PermissionHandler

                config = {"cli_path": self.cli_path, "auto_start": True}
                if self.github_token:
                    config["github_token"] = self.github_token
                else:
                    config["use_logged_in_user"] = True

                self._client = CopilotClient(config)
                self._on_permission_request = PermissionHandler.approve_all
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
        *,
        chunk_mode: bool = False,
    ) -> LLMTradingBlueprint:
        """Generate a next-day trading blueprint via Copilot SDK."""
        import asyncio
        import random
        from time import perf_counter

        from pydantic import ValidationError

        from shared.metrics import llm_request_duration, llm_retries_total

        settings = get_settings()
        max_retries = settings.analysis_service.llm.max_retries
        backoff_base = settings.analysis_service.llm.backoff_base_seconds
        backoff_max = settings.analysis_service.llm.backoff_max_seconds

        client = await self._get_client()
        prompt = build_blueprint_prompt(
            signal_features, current_positions, previous_execution,
            chunk_mode=chunk_mode,
        )
        full_prompt = _build_structured_prompt(prompt)

        last_exc: Exception | None = None
        for attempt in range(max_retries):
            t0 = perf_counter()
            status = "error"
            session = None
            try:
                await self._get_client()

                session_opts: dict = {
                    "model": self.model,
                    "skill_directories": [_SKILLS_DIR],
                    "on_permission_request": self._on_permission_request,
                }

                # Only attach reasoning_effort for models known to support it
                if self.reasoning_effort and any(
                    self.model.startswith(p) for p in _REASONING_EFFORT_SUPPORTED_PREFIXES
                ):
                    session_opts["reasoning_effort"] = self.reasoning_effort

                try:
                    session = await client.create_session(session_opts)
                except Exception as exc:
                    if "reasoning effort" in str(exc).lower() and "reasoning_effort" in session_opts:
                        logger.warning(
                            "copilot.reasoning_effort_unsupported",
                            model=self.model,
                        )
                        session_opts.pop("reasoning_effort")
                        session = await client.create_session(session_opts)
                    else:
                        raise

                result = await session.send_and_wait(
                    {"prompt": full_prompt},
                    timeout=self.request_timeout,
                )

                response_text = (
                    result.content
                    if hasattr(result, "content")
                    else str(result)
                )

                blueprint_data = _parse_blueprint_json(response_text)

                blueprint_data["trading_date"] = _next_trading_day().isoformat()
                blueprint_data["generated_at"] = now_utc().isoformat()
                blueprint_data["model_provider"] = "copilot"
                blueprint_data["model_version"] = "copilot-sdk"

                blueprint = LLMTradingBlueprint.model_validate(blueprint_data)

                status = "ok"
                logger.info(
                    "copilot.blueprint_generated",
                    trading_date=str(blueprint.trading_date),
                    plans=len(blueprint.symbol_plans),
                )
                return blueprint

            except (json.JSONDecodeError, ValidationError) as e:
                llm_retries_total.labels(provider="copilot", error_type="parse").inc()
                logger.warning(
                    "copilot.parse_error",
                    attempt=attempt + 1,
                    error=str(e),
                    error_type=type(e).__name__,
                )
                raise

            except Exception as e:
                last_exc = e
                error_type = type(e).__name__
                is_retryable = hasattr(e, "status_code") and getattr(e, "status_code", 0) >= 500

                # Treat timeouts and connection errors as retryable
                if "timeout" in str(e).lower() or "connection" in str(e).lower():
                    is_retryable = True

                if is_retryable and attempt < max_retries - 1:
                    delay = min(
                        backoff_base * (2 ** attempt) + random.uniform(0, 1),
                        backoff_max,
                    )
                    llm_retries_total.labels(provider="copilot", error_type=error_type).inc()
                    logger.warning(
                        "copilot.retryable_error",
                        attempt=attempt + 1,
                        error=str(e),
                        error_type=error_type,
                        retry_delay_s=round(delay, 2),
                    )
                    await asyncio.sleep(delay)
                    continue

                logger.warning(
                    "copilot.generation_failed",
                    attempt=attempt + 1,
                    error=str(e),
                    error_type=error_type,
                    retryable=is_retryable,
                )
                raise

            finally:
                elapsed = perf_counter() - t0
                llm_request_duration.labels(
                    provider="copilot", agent="blueprint", status=status,
                ).observe(elapsed)
                # Always disconnect session to prevent resource leaks
                if session is not None:
                    try:
                        await session.disconnect()
                    except Exception:
                        pass

        raise last_exc or RuntimeError("Copilot failed to generate blueprint after retries")

    async def health_check(self) -> bool:
        try:
            client = await self._get_client()
            return client is not None
        except Exception:
            return False
