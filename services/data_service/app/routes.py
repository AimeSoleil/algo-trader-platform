"""Data Service — REST API 路由"""
from __future__ import annotations

from datetime import date, datetime, time
from typing import Literal

from celery.result import AsyncResult
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text

from services.data_service.app.scheduler import (
    get_current_mode,
    get_data_service_config,
    set_intraday_enabled,
)
from shared.celery_app import celery_app
from shared.config import get_settings
from shared.db.session import get_timescale_session
from shared.utils import market_tz, previous_trading_day, today_trading

router = APIRouter(tags=["data"])


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


class ModeSwitchRequest(BaseModel):
    intraday_enabled: bool


class CollectRequest(BaseModel):
    """Manual data collection request."""
    symbols: list[str]
    start_date: date
    end_date: date
    data_types: list[str] = ["bars_1m", "bars_daily", "options_daily"]


class CollectResponse(BaseModel):
    task_id: str
    status: str = "queued"
    message: str = ""


def _normalize_manual_end_date(end_date: date) -> tuple[date, str | None]:
    """Normalize end_date for pre-market manual collection.

    Rule:
    - if end_date == today_trading() and current market time is before market open,
      shift end_date to previous trading day and emit a warning message.
    """
    today = today_trading()
    if end_date != today:
        return end_date, None

    settings = get_settings()
    open_hour, open_minute = map(int, settings.data_service.market_hours.start.split(":"))
    tz = market_tz()
    now_market = datetime.now(tz)
    market_open_dt = datetime.combine(today, time(open_hour, open_minute), tzinfo=tz)

    if now_market < market_open_dt:
        normalized = previous_trading_day(today)
        warning = (
            f"end_date {end_date} adjusted to {normalized}: current market time "
            f"{now_market.strftime('%H:%M')} is before market open "
            f"{settings.data_service.market_hours.start}"
        )
        return normalized, warning

    return end_date, None


# ── Health / config endpoints ──────────────────────────────


@router.get("/health")
async def health_check():
    return {
        "status": "ok",
        "service": "data_service",
        "mode": get_current_mode(),
    }


@router.get("/data/config")
async def get_mode_config():
    return {
        "current_mode": get_current_mode(),
        "config": get_data_service_config(),
    }


@router.post("/data/config")
async def update_mode_config(req: ModeSwitchRequest):
    try:
        enabled = set_intraday_enabled(req.intraday_enabled)
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    return {
        "status": "success",
        "intraday_enabled": enabled,
        "effective_mode": get_current_mode(),
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


@router.post("/collect", status_code=202, response_model=CollectResponse)
async def trigger_collection(req: CollectRequest):
    """Trigger manual data collection for specific symbols and date range."""
    normalized_end_date, normalization_warning = _normalize_manual_end_date(req.end_date)

    if not req.symbols:
        raise HTTPException(status_code=422, detail="symbols list must not be empty")
    if req.start_date > normalized_end_date:
        raise HTTPException(status_code=422, detail="start_date must be <= end_date")
    if normalized_end_date > today_trading():
        raise HTTPException(status_code=422, detail="end_date cannot be in the future")

    valid_types = {"bars_1m", "bars_daily", "options_daily"}
    invalid = set(req.data_types) - valid_types
    if invalid:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid data_types: {invalid}. Must be subset of {valid_types}",
        )
    if not req.data_types:
        raise HTTPException(status_code=422, detail="data_types must not be empty")

    symbols = [s.upper() for s in req.symbols]

    task = celery_app.send_task(
        "data_service.tasks.manual_collect",
        args=[symbols, req.start_date.isoformat(), normalized_end_date.isoformat(), req.data_types],
        queue="data",
    )

    message = (
        f"Collection queued for {len(symbols)} symbols, "
        f"{req.start_date} to {normalized_end_date}, types={req.data_types}"
    )
    if normalization_warning:
        message = f"{message}. Warning: {normalization_warning}"

    return CollectResponse(
        task_id=task.id,
        status="queued",
        message=message,
    )


@router.get("/collect/{task_id}")
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
