"""Signal Service — REST API routes."""
from __future__ import annotations

from datetime import date

from celery.result import AsyncResult
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from shared.celery_app import celery_app
from shared.utils import today_trading

router = APIRouter(tags=["signal"])


class SignalComputeRequest(BaseModel):
    """Manual signal generation trigger request."""
    trading_date: date | None = Field(None, description="Target date (ISO format). Defaults to today.")
    symbols: list[str] | None = Field(
        None,
        description="Specific symbols to compute. Defaults to full watchlist.",
    )


class SignalComputeResponse(BaseModel):
    task_id: str
    status: str = "queued"
    message: str = ""


@router.get("/signals/batch")
async def get_batch_signal_features(
    trading_date: str | None = Query(None, description="Filter by trading_date (YYYY-MM-DD)"),
    symbols: list[str] | None = Query(None, description="Filter by symbols"),
):
    """查询当日所有标的的信号特征（Analysis Service 调用），支持按 symbols 过滤"""
    from services.signal_service.app.queries import query_batch_signal_features
    return await query_batch_signal_features(trading_date, symbols=symbols)


@router.get("/signals/{symbol}")
async def get_signal_features(
    symbol: str,
    trading_date: str | None = Query(None, description="Target trading_date (YYYY-MM-DD)"),
    by_pass_cache: bool = False,
):
    """查询某标的的信号特征"""
    from services.signal_service.app.queries import query_signal_features
    return await query_signal_features(symbol, trading_date, by_pass_cache=by_pass_cache)


@router.post("/signals/compute", status_code=202, response_model=SignalComputeResponse)
async def trigger_signal_compute(req: SignalComputeRequest):
    """手动触发当日或指定交易日的批量信号计算任务。

    可指定 symbols 仅计算特定标的，否则使用完整 watchlist。
    """
    td = req.trading_date or today_trading()
    if td > today_trading():
        raise HTTPException(status_code=422, detail="trading_date cannot be in the future")

    clean_symbols: list[str] | None = None
    if req.symbols:
        clean_symbols = list(dict.fromkeys(s.strip().upper() for s in req.symbols if s.strip()))
        if not clean_symbols:
            raise HTTPException(status_code=422, detail="symbols list must not be empty when provided")

    task = celery_app.send_task(
        "signal_service.tasks.compute_daily_signals",
        args=[td.isoformat()],
        kwargs={"symbols": clean_symbols},
        queue="signal",
    )

    symbols_msg = f", symbols={clean_symbols}" if clean_symbols else " (full watchlist)"
    return SignalComputeResponse(
        task_id=task.id,
        status="queued",
        message=f"Signal generation queued for trading_date={td}{symbols_msg}",
    )


@router.get("/signals/compute/{task_id}")
async def get_signal_compute_status(task_id: str):
    """查询手动触发信号计算任务状态。"""
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
