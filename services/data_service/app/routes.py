"""Data Service — REST API 路由"""
from __future__ import annotations

from datetime import date, datetime, time
from typing import Literal

from celery.result import AsyncResult
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text

from shared.celery_app import celery_app
from shared.config import get_settings
from shared.db.session import get_timescale_session
from shared.utils import (
    before_market_open,
    get_logger,
    is_market_open,
    now_market,
    previous_trading_day,
    today_trading,
)

router = APIRouter(tags=["data"])
logger = get_logger("data_service_routes")


# ── Pydantic response models ──────────────────────────────

class PaginationInfo(BaseModel):
    page: int
    page_size: int
    total_items: int
    total_pages: int


class StockBarItem(BaseModel):
    symbol: str
    trading_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int


class StockDailyResponse(BaseModel):
    symbol: str
    data: list[StockBarItem]
    pagination: PaginationInfo


class OptionDailyItem(BaseModel):
    underlying: str
    symbol: str
    snapshot_date: date
    expiry: date
    strike: float
    option_type: str
    last_price: float
    bid: float
    ask: float
    volume: int
    open_interest: int
    iv: float
    delta: float
    gamma: float
    theta: float
    vega: float
    underlying_price: float | None = None


class OptionPaginationInfo(PaginationInfo):
    snapshot_date: str | None = None


class OptionDailyResponse(BaseModel):
    symbol: str
    data: list[OptionDailyItem]
    pagination: OptionPaginationInfo


class DatesResponse(BaseModel):
    symbol: str
    dates: list[date]
    total: int


class CollectRequest(BaseModel):
    """Manual stock data collection request."""
    symbols: list[str]
    start_date: date
    end_date: date
    data_types: list[str] = ["bars_1m", "bars_daily"]


class CollectResponse(BaseModel):
    task_id: str
    status: str = "queued"
    message: str = ""


def _default_post_market_collection_date() -> date:
    trading_date = today_trading()
    if now_market().weekday() >= 5:
        return previous_trading_day(trading_date)
    return trading_date


def _build_collect_suggested_body(
    req: CollectRequest,
    *,
    start_date: date,
    end_date: date,
) -> dict:
    """Build a copy-pasteable suggested request body for collect API."""
    return {
        "symbols": req.symbols,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "data_types": req.data_types,
    }


# ── Health / config endpoints ──────────────────────────────


@router.get("/health")
async def health_check():
    return {
        "status": "ok",
        "service": "data_service",
    }

# ── Stock endpoints ────────────────────────────────────────


@router.get("/data/{symbol}/stock", response_model=StockDailyResponse)
async def list_stock_daily(
    symbol: str,
    start_date: date | None = Query(None, description="Filter by trading_date >= start_date"),
    end_date: date | None = Query(None, description="Filter by trading_date <= end_date"),
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(100, ge=1, le=1000, description="Items per page"),
):
    """查询 stock_daily 日线列表，支持日期范围过滤 + 分页"""
    sym = symbol.upper()
    offset = (page - 1) * page_size

    conditions = ["symbol = :symbol"]
    params: dict = {"symbol": sym}
    if start_date is not None:
        conditions.append("trading_date >= :start_date")
        params["start_date"] = start_date
    if end_date is not None:
        conditions.append("trading_date <= :end_date")
        params["end_date"] = end_date

    where = " AND ".join(conditions)

    async with get_timescale_session() as session:
        total_count = (
            await session.execute(
                text(f"SELECT COUNT(*) FROM stock_daily WHERE {where}"),
                params,
            )
        ).scalar() or 0

        rows = (
            await session.execute(
                text(
                    f"""
                    SELECT symbol, trading_date, open, high, low, close, volume
                    FROM stock_daily
                    WHERE {where}
                    ORDER BY trading_date DESC
                    LIMIT :limit OFFSET :offset
                    """
                ),
                {**params, "limit": page_size, "offset": offset},
            )
        ).mappings().all()

    if total_count == 0:
        raise HTTPException(status_code=404, detail=f"No stock data for {sym}")

    total_pages = (total_count + page_size - 1) // page_size

    return StockDailyResponse(
        symbol=sym,
        data=[StockBarItem(**dict(r)) for r in rows],
        pagination=PaginationInfo(
            page=page,
            page_size=page_size,
            total_items=total_count,
            total_pages=total_pages,
        ),
    )


@router.get("/data/{symbol}/stock/dates", response_model=DatesResponse)
async def list_stock_dates(symbol: str):
    """返回该标的已有数据的所有 trading_date（去重、降序）"""
    sym = symbol.upper()

    async with get_timescale_session() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT DISTINCT trading_date FROM stock_daily "
                    "WHERE symbol = :symbol ORDER BY trading_date DESC"
                ),
                {"symbol": sym},
            )
        ).scalars().all()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No stock data for {sym}")

    return DatesResponse(symbol=sym, dates=list(rows), total=len(rows))


# ── Option endpoints ───────────────────────────────────────


@router.get("/data/{symbol}/options", response_model=OptionDailyResponse)
async def list_option_daily(
    symbol: str,
    snapshot_date: date | None = Query(None, description="Filter by snapshot_date (default: latest)"),
    start_date: date | None = Query(None, description="Filter snapshot_date >= start_date (range mode)"),
    end_date: date | None = Query(None, description="Filter snapshot_date <= end_date (range mode)"),
    expiry: date | None = Query(None, description="Filter by expiry date"),
    option_type: Literal["call", "put"] | None = Query(None, description="Filter by option type"),
    min_strike: float | None = Query(None, description="Filter strike >= min_strike"),
    max_strike: float | None = Query(None, description="Filter strike <= max_strike"),
    min_volume: int | None = Query(None, ge=0, description="Filter volume >= min_volume"),
    min_open_interest: int | None = Query(None, ge=0, description="Filter open_interest >= min_open_interest"),
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(500, ge=1, le=5000, description="Items per page"),
):
    """查询 option_daily 期权链列表，支持多维过滤 + 日期范围 + 分页

    - 若指定 ``snapshot_date``，则精确查询该天
    - 若指定 ``start_date``/``end_date``，则范围查询
    - 若三者均未指定，默认取最新 snapshot_date
    """
    sym = symbol.upper()
    offset = (page - 1) * page_size

    async with get_timescale_session() as session:
        # Determine date filter mode
        effective_snapshot_date: date | None = None

        if snapshot_date is not None:
            # Exact date mode
            conditions = ["underlying = :symbol", "snapshot_date = :snap_date"]
            params: dict = {"symbol": sym, "snap_date": snapshot_date}
            effective_snapshot_date = snapshot_date
        elif start_date is not None or end_date is not None:
            # Range mode
            conditions = ["underlying = :symbol"]
            params = {"symbol": sym}
            if start_date is not None:
                conditions.append("snapshot_date >= :start_date")
                params["start_date"] = start_date
            if end_date is not None:
                conditions.append("snapshot_date <= :end_date")
                params["end_date"] = end_date
        else:
            # Default: latest snapshot_date
            latest = (
                await session.execute(
                    text("SELECT MAX(snapshot_date) FROM option_daily WHERE underlying = :symbol"),
                    {"symbol": sym},
                )
            ).scalar()
            if latest is None:
                raise HTTPException(status_code=404, detail=f"No option data for {sym}")
            effective_snapshot_date = latest
            conditions = ["underlying = :symbol", "snapshot_date = :snap_date"]
            params = {"symbol": sym, "snap_date": latest}

        # Additional filters
        if expiry is not None:
            conditions.append("expiry = :expiry")
            params["expiry"] = expiry
        if option_type is not None:
            conditions.append("option_type = :option_type")
            params["option_type"] = option_type
        if min_strike is not None:
            conditions.append("strike >= :min_strike")
            params["min_strike"] = min_strike
        if max_strike is not None:
            conditions.append("strike <= :max_strike")
            params["max_strike"] = max_strike
        if min_volume is not None:
            conditions.append("volume >= :min_volume")
            params["min_volume"] = min_volume
        if min_open_interest is not None:
            conditions.append("open_interest >= :min_oi")
            params["min_oi"] = min_open_interest

        where = " AND ".join(conditions)

        total_count = (
            await session.execute(
                text(f"SELECT COUNT(*) FROM option_daily WHERE {where}"),
                params,
            )
        ).scalar() or 0

        rows = (
            await session.execute(
                text(
                    f"""
                    SELECT underlying, symbol, snapshot_date, expiry, strike, option_type,
                           last_price, bid, ask, volume, open_interest, iv,
                           delta, gamma, theta, vega, underlying_price
                    FROM option_daily
                    WHERE {where}
                    ORDER BY snapshot_date DESC, expiry ASC, strike ASC, option_type ASC
                    LIMIT :limit OFFSET :offset
                    """
                ),
                {**params, "limit": page_size, "offset": offset},
            )
        ).mappings().all()

    if total_count == 0:
        raise HTTPException(status_code=404, detail=f"No option data for {sym}")

    total_pages = (total_count + page_size - 1) // page_size

    return OptionDailyResponse(
        symbol=sym,
        data=[OptionDailyItem(**dict(r)) for r in rows],
        pagination=OptionPaginationInfo(
            snapshot_date=str(effective_snapshot_date) if effective_snapshot_date else None,
            page=page,
            page_size=page_size,
            total_items=total_count,
            total_pages=total_pages,
        ),
    )


@router.get("/data/{symbol}/options/dates", response_model=DatesResponse)
async def list_option_dates(symbol: str):
    """返回该标的已有数据的所有 snapshot_date（去重、降序）"""
    sym = symbol.upper()

    async with get_timescale_session() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT DISTINCT snapshot_date FROM option_daily "
                    "WHERE underlying = :symbol ORDER BY snapshot_date DESC"
                ),
                {"symbol": sym},
            )
        ).scalars().all()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No option data for {sym}")

    return DatesResponse(symbol=sym, dates=list(rows), total=len(rows))


# ── Manual collection endpoints ────────────────────────────


@router.post("/data/collect/stock", status_code=202, response_model=CollectResponse)
async def trigger_collection(req: CollectRequest):
    """Trigger manual stock data collection for specific symbols and date range.

    Date validation rules (for ``end_date``):
    - Future dates → 422
    - Today + pre-market → 422, suggest previous trading day
    - Today + market open → 422, suggest run after market close
    - Today + post-market → proceed normally
    - Past dates → proceed normally
    """
    if not req.symbols:
        raise HTTPException(status_code=422, detail="symbols list must not be empty")

    today = today_trading()
    settings = get_settings()
    mkt_start = settings.common.market_hours.start
    mkt_end = settings.common.market_hours.end

    # ── Future check ──
    if req.end_date > today:
        suggested_end = today
        suggested_start = min(req.start_date, suggested_end)
        raise HTTPException(
            status_code=422,
            detail={
                "error": "end_date cannot be in the future",
                "suggested_request_body": _build_collect_suggested_body(
                    req, start_date=suggested_start, end_date=suggested_end,
                ),
            },
        )

    # ── Today market-hours check ──
    if req.end_date == today:
        if before_market_open():
            prev_day = previous_trading_day(today)
            raise HTTPException(
                status_code=422,
                detail={
                    "error": (
                        f"Market has not opened yet ({now_market().strftime('%H:%M')} < {mkt_start}). "
                        f"Today's stock data is not yet available."
                    ),
                    "suggested_request_body": _build_collect_suggested_body(
                        req,
                        start_date=min(req.start_date, prev_day),
                        end_date=prev_day,
                    ),
                },
            )
        elif is_market_open():
            raise HTTPException(
                status_code=422,
                detail={
                    "error": (
                        f"Market is currently open ({now_market().strftime('%H:%M')}, closes {mkt_end}). "
                        f"Today's stock data is only complete after market close. "
                        f"Suggestion: run collection after {mkt_end}."
                    ),
                },
            )
        # else: after_market_close → proceed normally

    # ── Date order check ──
    if req.start_date > req.end_date:
        suggested_start = min(req.start_date, req.end_date)
        suggested_end = max(req.start_date, req.end_date)
        raise HTTPException(
            status_code=422,
            detail={
                "error": "start_date must be <= end_date",
                "suggested_request_body": _build_collect_suggested_body(
                    req, start_date=suggested_start, end_date=suggested_end,
                ),
            },
        )

    # ── Data types validation ──
    valid_types = {"bars_1m", "bars_daily"}
    invalid = set(req.data_types) - valid_types
    if invalid:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid data_types: {invalid}. Must be subset of {valid_types}",
        )
    if not req.data_types:
        raise HTTPException(status_code=422, detail="data_types must not be empty")

    symbols = [s.upper() for s in req.symbols]

    # Expand "WATCHLIST" keyword → merge with configured watchlist symbols
    if "WATCHLIST" in symbols:
        settings_watchlist = settings.common.watchlist.all
        symbols = list(dict.fromkeys(
            s for s in (symbols + settings_watchlist) if s != "WATCHLIST"
        ))

    task = celery_app.send_task(
        "data_service.tasks.manual_collect",
        args=[symbols, req.start_date.isoformat(), req.end_date.isoformat(), req.data_types],
        queue="data",
    )

    logger.info(
        "manual_collect.queued",
        task_id=task.id,
        queue="data",
        symbols=len(symbols),
        start_date=req.start_date.isoformat(),
        end_date=req.end_date.isoformat(),
        data_types=req.data_types,
    )

    message = (
        f"Stock data collection queued for {len(symbols)} symbols, "
        f"{req.start_date} to {req.end_date}, types={req.data_types}"
    )

    return CollectResponse(
        task_id=task.id,
        status="queued",
        message=message,
    )


@router.post("/data/collect/post-market", status_code=202, response_model=CollectResponse)
async def trigger_post_market_collection():
    """Trigger post-market options aggregation + stock capture only.

    This endpoint intentionally does not dispatch signal or analysis tasks.
    """
    market_now = now_market()
    settings = get_settings()
    market_close = settings.common.market_hours.end

    if market_now.weekday() < 5 and (before_market_open() or is_market_open()):
        raise HTTPException(
            status_code=422,
            detail={
                "error": (
                    f"Post-market collection can only be triggered after market close ({market_close}). "
                    f"Current market time: {market_now.strftime('%H:%M')}."
                ),
            },
        )

    trading_date = _default_post_market_collection_date()
    task = celery_app.send_task(
        "data_service.tasks.run_post_market_collection_only",
        args=[trading_date.isoformat()],
        queue="data",
    )

    logger.info(
        "manual_post_market_collection.queued",
        task_id=task.id,
        queue="data",
        trading_date=trading_date.isoformat(),
        downstream="skipped",
    )

    return CollectResponse(
        task_id=task.id,
        status="queued",
        message=(
            f"Post-market data collection queued for {trading_date} "
            f"(options aggregation + stock capture only; signal and analysis not triggered)"
        ),
    )


@router.get("/data/collect/{task_id}")
async def get_collection_status(task_id: str):
    """Poll the status of a manual collection task."""
    result = AsyncResult(task_id, app=celery_app)

    response = {
        "task_id": task_id,
        "state": result.state,
    }

    if result.state == "PROGRESS":
        response["progress"] = result.info
    elif result.state == "SUCCESS":
        response["result"] = result.result
    elif result.state == "FAILURE":
        response["error"] = str(result.result)

    return response


# ── Earnings endpoints ─────────────────────────────────────


class EarningsRequest(BaseModel):
    symbols: list[str]


class EarningsItem(BaseModel):
    symbol: str
    next_earnings_date: date | None = None
    days_until_earnings: int | None = None


class EarningsResponse(BaseModel):
    as_of: date
    results: list[EarningsItem]


@router.post("/data/earnings", response_model=EarningsResponse)
async def get_earnings_dates(req: EarningsRequest):
    """Fetch next earnings dates for the given symbols.

    Returns each symbol's next earnings date and how many calendar days
    away it is from today.  Results are cached in Redis until midnight ET.
    """
    if not req.symbols:
        raise HTTPException(status_code=422, detail="symbols list must not be empty")

    symbols = [s.strip().upper() for s in req.symbols if s.strip()]
    if not symbols:
        raise HTTPException(status_code=422, detail="symbols list must not be empty")

    from services.data_service.app.tasks.earnings import fetch_and_cache_earnings

    results = await fetch_and_cache_earnings(symbols)
    today = today_trading()

    items = []
    for sym in symbols:
        earn_date = results.get(sym)
        days = (earn_date - today).days if earn_date is not None else None
        items.append(EarningsItem(
            symbol=sym,
            next_earnings_date=earn_date,
            days_until_earnings=days,
        ))

    return EarningsResponse(as_of=today, results=items)

