"""Copilot SDK provider for the agent pipeline.

Thin wrapper around the Copilot SDK that satisfies the
``AgentLLMProvider`` protocol.  Used by the Orchestrator when
``settings.analysis_service.llm.provider == "copilot"``.

Unlike the legacy ``CopilotProvider`` (which mounts skill_directories),
this agent-level wrapper only sends instructions + user_prompt and
expects raw JSON back — the skill content is already inlined in each
agent's ``system_prompt``.
"""
from __future__ import annotations

import shutil
from pathlib import Path

from shared.config import get_settings
from shared.utils import get_logger

from services.analysis_service.app.llm.agents.base_agent import LLMResult
from services.analysis_service.app.llm.json_utils import extract_json_str

logger = get_logger("copilot_agent_provider")

# Models known to support the ``reasoning_effort`` session parameter.
# If the configured model does not match any prefix here, we omit the
# parameter to avoid JSON-RPC -32603 errors from the Copilot backend.
_REASONING_EFFORT_SUPPORTED_PREFIXES: tuple[str, ...] = (
    "claude-",
    "o1",
    "o3",
    "o4",
)


def _resolve_cli_path(configured_cli: str) -> str:
    """Resolve configured Copilot CLI to an executable path when possible."""
    cli = configured_cli.strip() if configured_cli else "copilot"
    if Path(cli).is_absolute() or "/" in cli:
        return cli

    resolved = shutil.which(cli)
    if resolved:
        return resolved
    return cli


class CopilotAgentProvider:
    """``AgentLLMProvider`` backed by the GitHub Copilot SDK."""

    def __init__(self) -> None:
        settings = get_settings()
        self._model = settings.analysis_service.llm.copilot.model
        self._reasoning_effort = settings.analysis_service.llm.copilot.reasoning_effort
        self._timeout = settings.analysis_service.llm.copilot.request_timeout_seconds
        self._client = None
        self._on_permission_request = None

    @property
    def name(self) -> str:  # noqa: D401
        return "copilot"

    async def _get_client(self):
        """Lazy-init the CopilotClient."""
        if self._client is None:
            from copilot import CopilotClient, PermissionHandler

            settings = get_settings()
            config = {
                "cli_path": _resolve_cli_path(settings.analysis_service.llm.copilot.cli_path),
                "auto_start": True,
            }
            if settings.analysis_service.llm.copilot.github_token:
                config["github_token"] = settings.analysis_service.llm.copilot.github_token
            else:
                config["use_logged_in_user"] = True

            self._client = CopilotClient(config)
            self._on_permission_request = PermissionHandler.approve_all
            await self._client.start()
            logger.info("copilot_agent.client_started")
        return self._client

    async def generate(
        self,
        *,
        instructions: str,
        user_prompt: str,
        temperature: float | None = None,
        max_tokens: int | None = None,
        model: str | None = None,
    ) -> LLMResult:
        client = await self._get_client()

        effective_model = model or self._model

        session_opts: dict = {
            "model": effective_model,
            "on_permission_request": self._on_permission_request,
        }

        # Only attach reasoning_effort for models known to support it;
        # other models (e.g. gemini-*) will reject the parameter.
        if self._reasoning_effort and any(
            effective_model.startswith(p) for p in _REASONING_EFFORT_SUPPORTED_PREFIXES
        ):
            session_opts["reasoning_effort"] = self._reasoning_effort

        try:
            session = await client.create_session(session_opts)
        except Exception as exc:
            # Graceful fallback: if reasoning_effort was the cause, retry
            # without it so we don't lose the entire request.
            if "reasoning effort" in str(exc).lower() and "reasoning_effort" in session_opts:
                logger.warning(
                    "copilot_agent.reasoning_effort_unsupported",
                    model=self._model,
                )
                session_opts.pop("reasoning_effort")
                session = await client.create_session(session_opts)
            else:
                raise

        # Combine instructions + user prompt in a structured format
        full_prompt = (
            f"<system>\n{instructions}\n</system>\n\n"
            f"<user>\n{user_prompt}\n\n"
            "Final output: return ONLY one valid JSON object using "
            "double-quoted keys and string values (RFC 8259). "
            "No single quotes, no trailing commas, no markdown fences, "
            "no extra text.\n</user>"
        )

        # Capture token usage from the ASSISTANT_USAGE event.
        from copilot.generated.session_events import SessionEventType

        usage_data: dict = {"input_tokens": 0, "output_tokens": 0, "model": effective_model}

        def _on_usage(event) -> None:
            if event.type == SessionEventType.ASSISTANT_USAGE:
                d = event.data
                usage_data["input_tokens"] = int(d.input_tokens or 0)
                usage_data["output_tokens"] = int(d.output_tokens or 0)
                if d.model:
                    usage_data["model"] = d.model

        session.on(_on_usage)

        result = await session.send_and_wait(
            {"prompt": full_prompt},
            timeout=self._timeout,
        )

        response_text = (
            result.content if hasattr(result, "content") else str(result)
        )

        input_tok = usage_data["input_tokens"]
        output_tok = usage_data["output_tokens"]

        return LLMResult(
            content=extract_json_str(response_text),
            input_tokens=input_tok,
            output_tokens=output_tok,
            total_tokens=input_tok + output_tok,
            model=usage_data["model"],
        )
