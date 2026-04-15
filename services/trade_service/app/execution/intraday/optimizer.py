"""Entry optimizer — evaluates all pending symbol plans against intraday features.

Architecture (two-layer entry decision):
  Layer 1 — GATE:   Blueprint entry_conditions (non-time) evaluated as boolean AND.
                     If any fails, plan is skipped without computing features.
  Layer 2 — TIMING: Intraday feature scoring (IV, price, liquidity, time-of-day).
                     Score >= threshold → enter; otherwise wait.

  ``time`` conditions from the blueprint are treated as soft references (logged for
  auditability) because the scorer's continuous time-of-day weighting is more nuanced
  than a binary gate.
"""
from __future__ import annotations

from datetime import date
from typing import Any

from shared.config import get_settings
from shared.models.intraday import EntryDecision, EntryScore
from shared.utils import get_logger, now_market

from services.trade_service.app.execution.intraday.features import IntradayFeatureComputer
from services.trade_service.app.execution.intraday.scorer import EntryQualityScorer
from services.trade_service.app.execution.intraday.strategy_profiles import get_profile
from services.trade_service.app.execution.market_context import build_market_context
from services.trade_service.app.execution.rule_engine import BlueprintRuleEngine

logger = get_logger("entry_optimizer")

# Sentinel score used when the condition gate rejects a plan (no scoring performed).
_ZERO_SCORE = EntryScore(symbol="", total=0.0)


def _partition_conditions(
    conditions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split entry_conditions into (hard_gates, soft_time_refs).

    ``field == "time"`` conditions are treated as soft references — the
    intraday scorer's ``_score_time`` handles time-of-day with continuous
    weighting rather than a binary pass/fail gate.
    """
    hard: list[dict[str, Any]] = []
    soft_time: list[dict[str, Any]] = []
    for c in conditions:
        if c.get("field") == "time":
            soft_time.append(c)
        else:
            hard.append(c)
    return hard, soft_time


class EntryOptimizer:
    """Evaluate all pending blueprint symbol plans for optimal entry timing."""

    def __init__(self) -> None:
        self._feature_computer = IntradayFeatureComputer()
        self._scorer = EntryQualityScorer()
        self._rule_engine = BlueprintRuleEngine()

    async def evaluate_all(
        self,
        trading_date: date,
        blueprint_json: dict[str, Any],
        blueprint_id: str | None = None,
    ) -> list[EntryDecision]:
        """Score entry quality for each un-entered symbol plan.

        Returns a list of EntryDecision with action "enter", "wait", or "skip".
        """
        settings = get_settings()
        threshold = settings.trade_service.intraday_optimizer.entry_score_threshold
        symbol_plans = blueprint_json.get("symbol_plans", [])
        trading_date_iso = trading_date.isoformat()

        # Current market time as decimal hours
        mt = now_market()
        market_time = mt.hour + mt.minute / 60.0

        decisions: list[EntryDecision] = []

        for plan in symbol_plans:
            symbol = plan.get("underlying", "")
            if not symbol:
                continue

            # Skip already-entered plans
            if plan.get("is_entered", False):
                continue

            strategy_type = plan.get("strategy_type", "single_leg")
            direction = plan.get("direction", "neutral")

            # ── Layer 1: Condition gate (hard prerequisites) ──
            entry_conditions = plan.get("entry_conditions", [])
            hard_conditions, soft_time_conditions = _partition_conditions(entry_conditions)

            conditions_met = True
            conditions_failed: list[str] = []

            if hard_conditions:
                market_ctx = await build_market_context(symbol, trading_date_iso)
                if market_ctx is None:
                    # No quote data → can't evaluate conditions
                    decisions.append(EntryDecision(
                        symbol=symbol,
                        score=_ZERO_SCORE,
                        action="skip",
                        strategy_type=strategy_type,
                        conditions_met=False,
                        conditions_failed=["no market data available"],
                        reasons=["no market data available"],
                    ))
                    logger.info("optimizer.skipped_no_data", symbol=symbol)
                    continue

                # Evaluate each hard condition individually to report which failed
                for cond in hard_conditions:
                    passed = self._rule_engine._eval_single_condition(cond, market_ctx)
                    if not passed:
                        conditions_met = False
                        desc = cond.get("description") or f"{cond.get('field')} {cond.get('operator')} {cond.get('value')}"
                        conditions_failed.append(desc)

            if soft_time_conditions:
                descs = [c.get("description", f"time {c.get('operator')} {c.get('value')}") for c in soft_time_conditions]
                logger.debug("optimizer.soft_time_ref", symbol=symbol, time_conditions=descs)

            if not conditions_met:
                decisions.append(EntryDecision(
                    symbol=symbol,
                    score=_ZERO_SCORE,
                    action="skip",
                    strategy_type=strategy_type,
                    conditions_met=False,
                    conditions_failed=conditions_failed,
                    reasons=[f"entry condition gate failed: {', '.join(conditions_failed)}"],
                ))
                logger.info(
                    "optimizer.gate_failed",
                    symbol=symbol,
                    strategy=strategy_type,
                    failed=conditions_failed,
                )
                continue

            # ── Layer 2: Intraday timing score ──
            # TODO(后续考虑-2): Blueprint adjustment_rules (hedge_delta, roll_strike,
            # etc.) have their own trigger conditions but are not yet consumed in the
            # execution layer.  A future enhancement could evaluate adjustment triggers
            # in the tick loop alongside exit_conditions for open positions.

            features = await self._feature_computer.compute(symbol, trading_date)
            profile = get_profile(strategy_type, direction)
            score = self._scorer.score(features, plan, profile, market_time)

            # Decide action
            if features.bars_available < 2:
                action = "skip"
                reasons = ["insufficient intraday data"]
            elif score.total >= threshold:
                action = "enter"
                reasons = [f"score {score.total:.2f} >= threshold {threshold}"]
            else:
                action = "wait"
                reasons = [f"score {score.total:.2f} < threshold {threshold}"]

            decision = EntryDecision(
                symbol=symbol,
                score=score,
                action=action,
                strategy_type=strategy_type,
                conditions_met=True,
                conditions_failed=[],
                reasons=reasons + score.reasons,
            )
            decisions.append(decision)

            logger.info(
                "optimizer.scored",
                symbol=symbol,
                strategy=strategy_type,
                direction=direction,
                profile=profile.name,
                score=round(score.total, 3),
                iv_score=round(score.iv_score, 3),
                price_score=round(score.price_score, 3),
                liquidity_score=round(score.liquidity_score, 3),
                time_score=round(score.time_score, 3),
                action=action,
                bars=features.bars_available,
            )

        return decisions
