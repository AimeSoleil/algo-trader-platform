"""Data Service — REST API 路由"""
from __future__ import annotations

from datetime import date
from typing import Literal

from celery.result import AsyncResult
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text

from services.data_service.app.cache import cache
from services.data_service.app.scheduler import (
    get_current_mode,
    get_data_service_config,
    set_intraday_enabled,
)
from shared.celery_app import celery_app
from shared.db.session import get_timescale_session

router = APIRouter(tags=["data"])


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


@router.get("/realtime/{symbol}/quote")
async def get_realtime_quote(symbol: str):
    """获取最新行情（L1 内存缓存，盘后由 pipeline 更新）"""
    quote = cache.get_realtime_quote(symbol.upper())
    if not quote:
        raise HTTPException(status_code=404, detail=f"No quote cached for {symbol}")
    return quote


@router.get("/realtime/{symbol}/option-chain")
async def get_realtime_option_chain(symbol: str):
    """获取最新期权链（盘中从 L1 内存缓存读取）"""
    chain = cache.get_realtime_option_chain(symbol.upper())
    if chain is None:
        raise HTTPException(status_code=404, detail=f"No option chain cached for {symbol}")
    return chain


@router.get("/health")
async def health_check():
    return {
        "status": "ok",
        "service": "data_service",
        "mode": get_current_mode(),
        "cached_option_symbols": list(cache.latest_option_chains.keys()),
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


@router.get("/data/{symbol}/stock")
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

    return {
        "symbol": sym,
        "data": [dict(r) for r in rows],
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total_items": total_count,
            "total_pages": total_pages,
        },
    }


@router.get("/data/{symbol}/options")
async def list_option_daily(
    symbol: str,
    snapshot_date: date | None = Query(None, description="Filter by snapshot_date (default: latest)"),
    expiry: date | None = Query(None, description="Filter by expiry date"),
    option_type: Literal["call", "put"] | None = Query(None, description="Filter by option type"),
    min_strike: float | None = Query(None, description="Filter strike >= min_strike"),
    max_strike: float | None = Query(None, description="Filter strike <= max_strike"),
    min_volume: int | None = Query(None, ge=0, description="Filter volume >= min_volume"),
    min_open_interest: int | None = Query(None, ge=0, description="Filter open_interest >= min_open_interest"),
    page: int = Query(1, ge=1, description="Page number (1-based)"),
    page_size: int = Query(500, ge=1, le=5000, description="Items per page"),
):
    """查询 option_daily 期权链列表，支持多维过滤 + 分页"""
    sym = symbol.upper()
    offset = (page - 1) * page_size

    async with get_timescale_session() as session:
        # Resolve snapshot_date: default to latest
        effective_date = snapshot_date
        if effective_date is None:
            latest = (
                await session.execute(
                    text("SELECT MAX(snapshot_date) FROM option_daily WHERE underlying = :symbol"),
                    {"symbol": sym},
                )
            ).scalar()
            if latest is None:
                raise HTTPException(status_code=404, detail=f"No option data for {sym}")
            effective_date = latest

        # Build dynamic WHERE
        conditions = ["underlying = :symbol", "snapshot_date = :snap_date"]
        params: dict = {"symbol": sym, "snap_date": effective_date}

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
                           delta, gamma, theta, vega
                    FROM option_daily
                    WHERE {where}
                    ORDER BY expiry ASC, strike ASC, option_type ASC
                    LIMIT :limit OFFSET :offset
                    """
                ),
                {**params, "limit": page_size, "offset": offset},
            )
        ).mappings().all()

    if total_count == 0:
        raise HTTPException(status_code=404, detail=f"No option data for {sym}")

    total_pages = (total_count + page_size - 1) // page_size

    return {
        "symbol": sym,
        "data": [dict(r) for r in rows],
        "pagination": {
            "snapshot_date": str(effective_date),
            "page": page,
            "page_size": page_size,
            "total_items": total_count,
            "total_pages": total_pages,
        },
    }


# ── Manual collection endpoints ───────────────────────────


@router.post("/collect", status_code=202, response_model=CollectResponse)
async def trigger_collection(req: CollectRequest):
    """Trigger manual data collection for specific symbols and date range.

    Returns a task_id that can be polled via GET /collect/{task_id}.
    """
    # ── Validation ──
    if not req.symbols:
        raise HTTPException(status_code=422, detail="symbols list must not be empty")
    if req.start_date > req.end_date:
        raise HTTPException(status_code=422, detail="start_date must be <= end_date")
    if req.end_date > date.today():
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
        args=[symbols, req.start_date.isoformat(), req.end_date.isoformat(), req.data_types],
    )

    return CollectResponse(
        task_id=task.id,
        status="queued",
        message=f"Collection queued for {len(symbols)} symbols, "
                f"{req.start_date} to {req.end_date}, types={req.data_types}",
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
