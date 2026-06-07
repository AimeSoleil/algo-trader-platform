from __future__ import annotations

from typing import Any, Iterable


SOFT_TRADE_BLOCK_CONSENSUS_MIN_COUNT = 2

HARD_TRADE_BLOCK_REASON_TOKENS = (
    "earnings_imminent",
    "event_risk_imminent",
    "vix_extreme",
    "hard_block_spread",
    "insufficient_leg_liquidity",
    "illiquid_spread_proxy",
)

SOFT_TRADE_BLOCK_REASON_TOKENS = (
    "counter_trend_*",
    "conflicting_*",
    "divergence_*",
    "high_false_breakout_risk",
    "insufficient_flow_confirmation",
    "extreme_option_activity_unconfirmed",
    "standalone_flow",
    "no_vol_edge",
)

HARD_TRADE_BLOCK_REASONS = frozenset(HARD_TRADE_BLOCK_REASON_TOKENS)

_SOFT_TRADE_BLOCK_REASONS = frozenset({
    "standalone_flow",
    "insufficient_flow_confirmation",
    "high_false_breakout_risk",
    "extreme_option_activity_unconfirmed",
    "no_vol_edge",
})

_SOFT_TRADE_BLOCK_REASON_PREFIXES = (
    "counter_trend",
    "conflicting_",
    "divergence_",
)

_SOFT_TRADE_BLOCK_REASON_PREFIX_LABELS = (
    ("counter_trend", "counter_trend_*"),
    ("conflicting_", "conflicting_*"),
    ("divergence_", "divergence_*"),
)


def _dedupe_preserve_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in items:
        token = str(item).strip()
        if not token or token in seen:
            continue
        seen.add(token)
        deduped.append(token)
    return deduped


def canonical_trade_gate_reason(reason: str) -> str:
    token = str(reason).strip().lower()
    if token in HARD_TRADE_BLOCK_REASONS or token in _SOFT_TRADE_BLOCK_REASONS:
        return token
    for prefix, label in _SOFT_TRADE_BLOCK_REASON_PREFIX_LABELS:
        if token.startswith(prefix):
            return label
    return token


def classify_reason_token(reason: str) -> str:
    token = str(reason).strip().lower()
    if not token:
        return "unknown"
    if token in HARD_TRADE_BLOCK_REASONS:
        return "hard"
    if is_soft_trade_block_reason(token):
        return "soft"
    return "unknown"


def normalized_blocked_reasons(symbol_analysis: dict[str, Any] | None) -> tuple[str, ...]:
    if not isinstance(symbol_analysis, dict):
        return ()

    blocked_reasons = symbol_analysis.get("blocked_reasons")
    if not isinstance(blocked_reasons, list):
        return ()

    normalized: list[str] = []
    for reason in blocked_reasons:
        token = str(reason).strip().lower()
        if token:
            normalized.append(token)
    return tuple(normalized)


def is_soft_trade_block_reason(reason: str) -> bool:
    token = str(reason).strip().lower()
    if not token:
        return False
    if token in _SOFT_TRADE_BLOCK_REASONS:
        return True
    return any(token.startswith(prefix) for prefix in _SOFT_TRADE_BLOCK_REASON_PREFIXES)


def classify_trade_block(symbol_analysis: dict[str, Any] | None) -> str:
    if not isinstance(symbol_analysis, dict):
        return "none"
    if symbol_analysis.get("trade_allowed") is not False:
        return "none"

    reasons = normalized_blocked_reasons(symbol_analysis)
    if any(classify_reason_token(reason) in {"hard", "unknown"} for reason in reasons):
        return "hard"
    if reasons and all(is_soft_trade_block_reason(reason) for reason in reasons):
        return "soft"
    return "hard"


def trade_gate_taxonomy_metadata() -> dict[str, Any]:
    return {
        "hard_trade_block_reasons": list(HARD_TRADE_BLOCK_REASON_TOKENS),
        "soft_trade_block_reasons": list(SOFT_TRADE_BLOCK_REASON_TOKENS),
        "soft_trade_block_consensus_min_count": SOFT_TRADE_BLOCK_CONSENSUS_MIN_COUNT,
    }


def format_trade_gate_taxonomy_prompt_text() -> str:
    hard_tokens = ", ".join(HARD_TRADE_BLOCK_REASON_TOKENS)
    soft_tokens = ", ".join(SOFT_TRADE_BLOCK_REASON_TOKENS)
    return (
        "## Machine-Readable Trade Gate Taxonomy\n"
        f"- Only these hard-veto reasons may set trade_allowed=false directly: {hard_tokens}.\n"
        f"- Analytical caution reasons must stay trade_allowed=true and use confidence_cap, simple_structures_only, position-size caps, or blocked_reasons instead: {soft_tokens}.\n"
        f"- Do NOT invent new trade veto reason tokens. If a legacy analytical caution still emits trade_allowed=false, downstream handling treats it as advisory unless at least {SOFT_TRADE_BLOCK_CONSENSUS_MIN_COUNT} agents agree."
    )


def summarize_trade_gate_analyses(agent_analyses: dict[str, dict[str, Any] | None]) -> dict[str, Any]:
    hard_trade_blocked_agents: list[str] = []
    soft_trade_blocked_agents: list[str] = []
    hard_reasons: list[str] = []
    soft_reasons: list[str] = []
    noncanonical_reasons: list[str] = []
    raw_blocked_reasons: list[str] = []

    for agent_name, symbol_analysis in agent_analyses.items():
        reasons = normalized_blocked_reasons(symbol_analysis)
        raw_blocked_reasons.extend(reasons)
        classification = classify_trade_block(symbol_analysis)
        if classification == "hard":
            hard_trade_blocked_agents.append(agent_name)
        elif classification == "soft":
            soft_trade_blocked_agents.append(agent_name)

        for reason in reasons:
            reason_classification = classify_reason_token(reason)
            canonical_reason = canonical_trade_gate_reason(reason)
            if reason_classification == "hard":
                hard_reasons.append(canonical_reason)
            elif reason_classification == "soft":
                soft_reasons.append(canonical_reason)
            else:
                noncanonical_reasons.append(canonical_reason)

    hard_trade_blocked_agents = _dedupe_preserve_order(hard_trade_blocked_agents)
    soft_trade_blocked_agents = _dedupe_preserve_order(soft_trade_blocked_agents)
    hard_reasons = _dedupe_preserve_order(hard_reasons)
    soft_reasons = _dedupe_preserve_order(soft_reasons)
    noncanonical_reasons = _dedupe_preserve_order(noncanonical_reasons)
    raw_blocked_reasons = _dedupe_preserve_order(raw_blocked_reasons)

    soft_trade_block_consensus_met = len(soft_trade_blocked_agents) >= SOFT_TRADE_BLOCK_CONSENSUS_MIN_COUNT
    if hard_trade_blocked_agents:
        trade_gate_status = "hard_blocked"
    elif soft_trade_block_consensus_met:
        trade_gate_status = "soft_consensus_blocked"
    elif soft_trade_blocked_agents:
        trade_gate_status = "soft_caution"
    else:
        trade_gate_status = "clear"

    return {
        "trade_gate_status": trade_gate_status,
        "hard_trade_blocked_agents": hard_trade_blocked_agents,
        "hard_trade_blocked_reasons": hard_reasons,
        "soft_trade_blocked_agents": soft_trade_blocked_agents,
        "soft_trade_blocked_reasons": soft_reasons,
        "soft_trade_block_consensus_met": soft_trade_block_consensus_met,
        "noncanonical_trade_blocked_reasons": noncanonical_reasons,
        "blocked_reasons": raw_blocked_reasons,
    }


def aggregate_trade_gate_summaries(entries: Iterable[dict[str, Any]]) -> dict[str, Any]:
    hard_entries: list[dict[str, Any]] = []
    soft_consensus_entries: list[dict[str, Any]] = []
    soft_caution_entries: list[dict[str, Any]] = []
    hard_reasons: list[str] = []
    soft_reasons: list[str] = []
    noncanonical_reasons: list[str] = []

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        status = str(entry.get("trade_gate_status") or "clear")
        if status == "hard_blocked":
            hard_entries.append(entry)
        elif status == "soft_consensus_blocked":
            soft_consensus_entries.append(entry)
        elif status == "soft_caution":
            soft_caution_entries.append(entry)

        hard_reasons.extend(str(reason) for reason in entry.get("hard_trade_blocked_reasons", []) or [])
        soft_reasons.extend(str(reason) for reason in entry.get("soft_trade_blocked_reasons", []) or [])
        noncanonical_reasons.extend(str(reason) for reason in entry.get("noncanonical_trade_blocked_reasons", []) or [])

    return {
        "hard_blocked_symbol_count": len(hard_entries),
        "soft_consensus_blocked_symbol_count": len(soft_consensus_entries),
        "soft_caution_symbol_count": len(soft_caution_entries),
        "hard_trade_blocked_reasons": _dedupe_preserve_order(hard_reasons),
        "soft_trade_blocked_reasons": _dedupe_preserve_order(soft_reasons),
        "noncanonical_trade_blocked_reasons": _dedupe_preserve_order(noncanonical_reasons),
        "symbols": [*hard_entries, *soft_consensus_entries, *soft_caution_entries],
    }


def format_trade_gate_rollup_text(summary: dict[str, Any], *, max_symbols: int = 3) -> str:
    if not isinstance(summary, dict):
        return ""

    symbol_entries = summary.get("symbols")
    if not isinstance(symbol_entries, list) or not symbol_entries:
        return ""

    formatted_entries: list[str] = []
    for entry in symbol_entries[:max_symbols]:
        if not isinstance(entry, dict):
            continue
        symbol = str(entry.get("symbol") or "UNKNOWN").upper()
        status = str(entry.get("trade_gate_status") or "clear")
        if status == "hard_blocked":
            reasons = entry.get("hard_trade_blocked_reasons", []) or entry.get("noncanonical_trade_blocked_reasons", [])
            prefix = "hard"
        elif status == "soft_consensus_blocked":
            reasons = entry.get("soft_trade_blocked_reasons", [])
            prefix = "soft-consensus"
        else:
            reasons = entry.get("soft_trade_blocked_reasons", [])
            prefix = "soft"
        reason_text = ", ".join(str(reason) for reason in reasons[:3]) if reasons else "unknown_reason"
        formatted_entries.append(f"{prefix} {symbol}[{reason_text}]")

    remaining = max(0, len(symbol_entries) - len(formatted_entries))
    suffix = f", +{remaining} more" if remaining else ""
    return f"Trade-gate summary: {'; '.join(formatted_entries)}{suffix}."