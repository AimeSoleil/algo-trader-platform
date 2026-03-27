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

import json
import re

from shared.config import get_settings
from shared.utils import get_logger

from services.analysis_service.app.llm.agents.base_agent import LLMResult

logger = get_logger("copilot_agent_provider")


def _extract_json(text: str) -> str:
    """Strip markdown fences / noise and return the JSON body."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
        text = text.rsplit("```", 1)[0]
    try:
        json.loads(text)
        return text
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{[\s\S]*\}", text)
    if not match:
        raise ValueError("No JSON object found in Copilot response")
    return match.group(0)


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
            from services.analysis_service.app.llm.copilot_provider import _resolve_cli_path

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
    ) -> LLMResult:
        client = await self._get_client()
        session = await client.create_session({
            "model": self._model,
            "reasoning_effort": self._reasoning_effort,
            "on_permission_request": self._on_permission_request,
            # No skill_directories — instructions already contain the rules
        })

        # Combine instructions + user prompt in a structured format
        full_prompt = (
            f"<system>\n{instructions}\n</system>\n\n"
            f"<user>\n{user_prompt}\n\n"
            "Final output: return ONLY one valid JSON object, "
            "no markdown fences and no extra text.\n</user>"
        )

        result = await session.send_and_wait(
            {"prompt": full_prompt},
            timeout=self._timeout,
        )

        response_text = (
            result.content if hasattr(result, "content") else str(result)
        )

        # Copilot SDK doesn't expose token counts
        return LLMResult(
            content=_extract_json(response_text),
            input_tokens=0,
            output_tokens=0,
            total_tokens=0,
        )
