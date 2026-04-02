"""Analysis task helpers — LLM adapter, signal parsing, position fetching."""
from __future__ import annotations

import json
from datetime import date, timedelta

from sqlalchemy import text

from shared.config import get_settings
from shared.db.session import get_postgres_session
from shared.models.blueprint import LLMTradingBlueprint
from shared.models.signal import DataQuality, SignalFeatures
from shared.utils import get_logger

logger = get_logger("analysis_tasks")


# ── LLM adapter singleton ─────────────────────────────────

_adapter = None


def _get_adapter():
    """Return a cached LLMAdapter instance (reuses OpenAI client + skill bundle)."""
    global _adapter
    if _adapter is None:
        from services.analysis_service.app.llm.adapter import LLMAdapter
        _adapter = LLMAdapter()
    return _adapter


# ── Signal parsing ─────────────────────────────────────────

def _parse_signal_features(raw: object) -> SignalFeatures:
    """Parse SignalFeatures from DB value that may be JSON string or Python dict.

    Postgres/SQLAlchemy may return JSONB as ``dict`` directly, while some drivers
    may return serialized JSON text/bytes.
    """
    if isinstance(raw, (str, bytes, bytearray)):
        return SignalFeatures.model_validate_json(raw)
    return SignalFeatures.model_validate(raw)


# ── Position fetching with fallback ──────────────────────────


async def _fetch_current_positions(td: date) -> dict:
    """Fetch current positions from Portfolio Service.

    Fallback priority:
      1. Live open positions from ``positions`` table (via portfolio service logic).
      2. If none found — derive positions from yesterday's *completed* blueprint
         (i.e. the plans that were entered but not yet exited).
      3. If neither available — return an empty-positions dict.
    """
    logger.debug(
        "blueprint.fetch_positions.start",
        log_event="positions_fetch",
        stage="start",
        trading_date=str(td),
    )
    # ── Attempt 1: live positions from trade service portfolio module ──
    try:
        from services.trade_service.app.portfolio.service import get_positions

        positions_data = await get_positions()
        logger.debug(
            "blueprint.fetch_positions.portfolio_result",
            log_event="positions_fetch",
            stage="trade_service_portfolio",
            count=positions_data.get("count", 0),
        )
        if positions_data.get("count", 0) > 0:
            logger.info(
                "blueprint.positions_from_portfolio",
                count=positions_data["count"],
            )
            return {
                "source": "trade_service_portfolio",
                **positions_data,
            }
    except Exception as e:
        logger.warning("blueprint.portfolio_fetch_failed", error=str(e))

    # ── Attempt 2: infer from recent blueprint ──
    # Walk back up to 3 days for weekends / holidays
    for lookback in range(4):
        check_date = td - timedelta(days=1 + lookback)
        logger.debug(
            "blueprint.fetch_positions.previous_blueprint_check",
            log_event="positions_fetch",
            stage="previous_blueprint_lookup",
            trading_date=str(td),
            check_date=str(check_date),
        )
        try:
            async with get_postgres_session() as session:
                result = await session.execute(
                    text(
                        "SELECT blueprint_json FROM llm_trading_blueprint "
                        "WHERE trading_date = :date AND status IN ('completed', 'active')"
                    ),
                    {"date": check_date},
                )
                row = result.fetchone()
                if row and row[0]:
                    import json as _json

                    bp_data = _json.loads(row[0]) if isinstance(row[0], str) else row[0]
                    inferred = _infer_positions_from_blueprint(bp_data)
                    if inferred["count"] > 0:
                        logger.info(
                            "blueprint.positions_from_previous_blueprint",
                            blueprint_date=str(check_date),
                            count=inferred["count"],
                        )
                        return {
                            "source": "previous_blueprint",
                            "blueprint_date": str(check_date),
                            **inferred,
                        }
        except Exception as e:
            logger.warning(
                "blueprint.prev_blueprint_fetch_failed",
                date=str(check_date),
                error=str(e),
            )

    logger.info("blueprint.no_existing_positions")
    return {
        "source": "none",
        "count": 0,
        "positions": [],
        "aggregates": {},
    }


def _infer_positions_from_blueprint(bp_data: dict) -> dict:
    """Extract entered-but-not-exited plans from a completed blueprint.

    These serve as a proxy for "current positions" when the portfolio
    service has no live data.
    """
    positions: list[dict] = []
    for plan in bp_data.get("symbol_plans", []):
        if plan.get("is_entered") and not plan.get("is_exited"):
            legs_summary = []
            for leg in plan.get("legs", []):
                legs_summary.append({
                    "expiry": leg.get("expiry"),
                    "strike": leg.get("strike"),
                    "option_type": leg.get("option_type"),
                    "side": leg.get("side"),
                    "quantity": leg.get("quantity", 1),
                })
            positions.append({
                "underlying": plan.get("underlying"),
                "strategy_type": plan.get("strategy_type"),
                "direction": plan.get("direction"),
                "legs": legs_summary,
                "confidence": plan.get("confidence", 0),
                "entry_fill_prices": plan.get("entry_fill_prices", []),
                "realized_pnl": plan.get("realized_pnl", 0),
            })
    logger.debug(
        "blueprint.positions_inferred",
        log_event="positions_infer",
        stage="completed",
        count=len(positions),
    )
    return {"count": len(positions), "positions": positions}
