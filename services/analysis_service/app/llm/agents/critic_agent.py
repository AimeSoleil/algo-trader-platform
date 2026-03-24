"""CriticAgent — Self-review loop for blueprint quality.

Reviews a synthesized blueprint against reference rules, risk constraints,
and logical consistency. Returns pass/revise verdict with specific issues.
"""
from __future__ import annotations

import asyncio
import json
import random
from time import perf_counter
from typing import Any

from pydantic import ValidationError

from shared.config import get_settings
from shared.metrics import llm_request_duration, llm_retries_total, llm_tokens_total
from shared.utils import get_logger

from services.analysis_service.app.llm.agents.base_agent import AgentLLMProvider, _default_provider
from services.analysis_service.app.llm.agents.models import CriticVerdict

logger = get_logger("critic_agent")


class CriticAgent:
    """Review a blueprint for rule violations, risk breaches, and logic errors.

    Returns a CriticVerdict indicating pass/revise with detailed issues.
    """

    async def review(
        self,
        blueprint_json: dict[str, Any],
        agent_outputs: dict[str, Any],
        signals_summary: list[dict[str, Any]],
        *,
        provider: AgentLLMProvider | None = None,
    ) -> CriticVerdict:
        """Review a blueprint and return verdict.

        Parameters
        ----------
        blueprint_json:
            The synthesized blueprint as a dict.
        agent_outputs:
            Original specialist agent outputs for cross-referencing.
        signals_summary:
            Compact signal summaries for context.
        provider:
            LLM provider instance injected by the Orchestrator.
        """
        if provider is None:
            provider = _default_provider()

        settings = get_settings()

        prompt = self._build_prompt(blueprint_json, agent_outputs, signals_summary)

        max_retries = settings.llm.max_retries
        backoff_base = settings.llm.backoff_base_seconds
        backoff_max = settings.llm.backoff_max_seconds

        last_exc: Exception | None = None
        for attempt in range(max_retries):
            t0 = perf_counter()
            status = "error"
            try:
                result = await provider.generate(
                    instructions=_CRITIC_SYSTEM_PROMPT,
                    user_prompt=prompt,
                    temperature=0.0,  # deterministic for review
                    max_tokens=4096,
                )

                data = json.loads(result.content)
                verdict = CriticVerdict.model_validate(data)

                status = "ok"
                llm_tokens_total.labels(
                    provider=provider.name, direction="prompt",
                ).inc(result.input_tokens)
                llm_tokens_total.labels(
                    provider=provider.name, direction="completion",
                ).inc(result.output_tokens)

                logger.info(
                    "critic.completed",
                    provider=provider.name,
                    verdict=verdict.verdict,
                    issues=len(verdict.issues),
                    tokens=result.total_tokens,
                )
                return verdict

            except (json.JSONDecodeError, ValidationError) as e:
                llm_retries_total.labels(provider=provider.name, error_type="parse").inc()
                logger.warning("critic.parse_error", provider=provider.name, attempt=attempt + 1, error=str(e))
                raise

            except Exception as e:
                last_exc = e
                error_type = type(e).__name__
                retryable = error_type in (
                    "RateLimitError", "APITimeoutError",
                    "APIConnectionError", "InternalServerError",
                ) or (hasattr(e, "status_code") and getattr(e, "status_code", 0) >= 500)

                if retryable and attempt < max_retries - 1:
                    delay = min(backoff_base * (2 ** attempt) + random.uniform(0, 1), backoff_max)
                    llm_retries_total.labels(provider=provider.name, error_type=error_type).inc()
                    logger.warning("critic.retryable_error", provider=provider.name, attempt=attempt + 1, error=str(e), delay=round(delay, 2))
                    await asyncio.sleep(delay)
                    continue

                logger.warning("critic.failed", provider=provider.name, attempt=attempt + 1, error=str(e))
                raise

            finally:
                elapsed = perf_counter() - t0
                llm_request_duration.labels(provider=provider.name, agent="critic", status=status).observe(elapsed)

        raise last_exc or RuntimeError("Critic failed after retries")

    def _build_prompt(
        self,
        blueprint_json: dict[str, Any],
        agent_outputs: dict[str, Any],
        signals_summary: list[dict[str, Any]],
    ) -> str:
        parts: list[str] = []

        parts.append("## Blueprint to Review\n")
        parts.append(json.dumps(blueprint_json, indent=2, ensure_ascii=False))

        parts.append("\n## Specialist Agent Analyses\n")
        for name, output in agent_outputs.items():
            compact = json.dumps(output, separators=(",", ":"), ensure_ascii=False)
            parts.append(f"### {name}\n{compact}")

        parts.append("\n## Signal Context\n")
        for s in signals_summary:
            parts.append(f"- {s.get('symbol', '?')}: close={s.get('close_price', '?')}")

        parts.append(
            "\n## Task\n"
            "Review the blueprint above. Check for:\n"
            "1. Rule violations (inconsistencies with agent analyses)\n"
            "2. Risk breaches (portfolio limits, missing stop-losses)\n"
            "3. Logic errors (wrong legs count, invalid conditions)\n"
            "4. Missing justification in reasoning fields\n\n"
            "Output your verdict as JSON. No markdown fences."
        )

        return "\n\n".join(parts)


_CRITIC_SYSTEM_PROMPT = """\
You are the Critic — an independent quality reviewer for trading blueprints.

## Your Role

Review the submitted blueprint for correctness, consistency, and compliance.
You are NOT generating strategies — you are AUDITING them.

## Checklist

### 1. Strategy-Legs Consistency
- single_leg: exactly 1 leg
- vertical_spread: exactly 2 legs (same expiry, different strikes)
- iron_condor: exactly 4 legs (2 puts + 2 calls, same expiry)
- iron_butterfly: exactly 4 legs (ATM straddle + OTM wings)
- butterfly: 3-4 legs
- calendar_spread: 2 legs (same strike, different expiry)
- straddle: 2 legs (same strike, call+put)
- strangle: 2 legs (different strikes, call+put)

### 2. Risk Compliance
- portfolio_delta_limit ≤ 0.5 (or 0.8 with justification)
- portfolio_gamma_limit ≤ 0.1
- max_daily_loss ≤ $2,000
- Every plan has stop_loss_amount > 0
- Every plan has max_loss_per_trade > 0
- Confidence between 0 and 1

### 3. Agent Consistency
- If Flow agent signaled hard_block for a symbol, it should NOT appear
- If Chain agent signaled hard_block, symbol should NOT appear
- Strategy types should align with Trend + Volatility agent recommendations
- Position sizes should reflect Flow agent's size modifiers

### 4. Logical Completeness
- Every leg has: expiry, strike, option_type (call/put), side (buy/sell)
- Entry conditions are mechanically evaluable (concrete thresholds)
- At least one exit condition per plan
- Reasoning references specific agent analyses

## Output Schema
```json
{
  "verdict": "pass" or "revise",
  "issues": [
    {
      "severity": "error|warning|info",
      "symbol": "AAPL" or null (portfolio-level),
      "category": "rule_violation|risk_breach|logic_error|missing_data",
      "description": "...",
      "suggested_fix": "..."
    }
  ],
  "summary": "Brief overall assessment"
}
```

- "pass": no errors (warnings/info OK)
- "revise": at least one error-severity issue found

Output ONLY valid JSON. No markdown fences.
"""
