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

import asyncio
import shutil
from pathlib import Path
from time import perf_counter
from typing import Any

from shared.config import get_settings
from shared.utils import estimate_prompt_tokens, get_logger

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


def _first_non_empty_text(*values: Any) -> str:
    """Return the first non-empty string from candidate values."""
    for value in values:
        if isinstance(value, str) and value:
            return value
    return ""


def _extract_text_from_event(event: Any) -> str:
    """Best-effort text extraction from Copilot SDK event/result objects."""
    data = getattr(event, "data", None)
    return _first_non_empty_text(
        getattr(event, "content", None),
        getattr(data, "content", None),
        getattr(data, "delta_content", None),
        getattr(data, "transformed_content", None),
        getattr(data, "partial_output", None),
        getattr(data, "output", None),
        getattr(data, "result", None),
        getattr(data, "message", None),
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
        self._bound_loop_id: int | None = None
        self._client_lock: asyncio.Lock | None = None
        self._lock_loop_id: int | None = None
        self._on_permission_request = None

    @property
    def name(self) -> str:  # noqa: D401
        return "copilot"

    async def _get_client(self):
        """Lazy-init the CopilotClient; rebuild when the event loop changes."""
        current_loop_id = id(asyncio.get_running_loop())

        # Recreate lock if bound to a stale event loop
        if self._client_lock is None or self._lock_loop_id != current_loop_id:
            self._client_lock = asyncio.Lock()
            self._lock_loop_id = current_loop_id

        async with self._client_lock:
            current_loop_id = id(asyncio.get_running_loop())

            if self._client is not None and self._bound_loop_id != current_loop_id:
                logger.info("copilot_agent.loop_changed, rebuilding client")
                try:
                    await self._client.stop()
                except Exception:
                    pass
                self._client = None
                self._bound_loop_id = None

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
                self._bound_loop_id = current_loop_id
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
        agent_name: str | None = None,
        analysis_chunk_id: str | None = None,
    ) -> LLMResult:
        client = await self._get_client()

        effective_model = model or self._model

        session_opts: dict = {
            "model": effective_model,
            "on_permission_request": self._on_permission_request,
        }

        if max_tokens:
            session_opts["max_tokens"] = max_tokens

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
        logger.info(
            "copilot_agent.request_started",
            analysis_chunk_id=analysis_chunk_id,
            agent=agent_name,
            model=effective_model,
            input_prompt_tokens=estimate_prompt_tokens(full_prompt),
        )

        # Capture token usage from the ASSISTANT_USAGE event.
        from copilot.generated.session_events import SessionEventType

        usage_data: dict = {"input_tokens": 0, "output_tokens": 0, "model": effective_model}
        delta_chunks: list[str] = []

        def _on_event(event) -> None:
            if event.type == SessionEventType.ASSISTANT_USAGE:
                d = event.data
                usage_data["input_tokens"] = int(d.input_tokens or 0)
                usage_data["output_tokens"] = int(d.output_tokens or 0)
                if d.model:
                    usage_data["model"] = d.model

            if event.type == SessionEventType.ASSISTANT_MESSAGE:
                data = getattr(event, "data", None)
                delta_text = getattr(data, "delta_content", None)
                if isinstance(delta_text, str) and delta_text:
                    delta_chunks.append(delta_text)

        session.on(_on_event)

        started = perf_counter()
        result = await session.send_and_wait(
            {"prompt": full_prompt},
            timeout=self._timeout,
        )
        api_latency_ms = round((perf_counter() - started) * 1000, 2)

        logger.debug(
            "copilot_agent.response_received",
            analysis_chunk_id=analysis_chunk_id,
            agent=agent_name,
            model=effective_model,
            api_latency_ms=api_latency_ms,
            typeof_raw_response=type(result).__name__,
            raw_response=str(result)
        )

        response_text = _extract_text_from_event(result)
        if not response_text and delta_chunks:
            response_text = "".join(delta_chunks)
        if not response_text:
            response_text = str(result)

        input_tok = usage_data["input_tokens"]
        output_tok = usage_data["output_tokens"]

        return LLMResult(
            content=extract_json_str(response_text),
            raw_content=response_text,
            input_tokens=input_tok,
            output_tokens=output_tok,
            total_tokens=input_tok + output_tok,
            model=usage_data["model"],
        )
