"""Signal Service — REST API routes."""
from __future__ import annotations

from datetime import date

from celery.result import AsyncResult
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from shared.celery_app import celery_app
from shared.utils import today_trading

router = APIRouter(tags=["signal"])


# ── Request / Response models ──────────────────────────────


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


# ── Signal query (unified) ────────────────────────────────


@router.get("/signals")
async def query_signals(
    symbols: str | None = Query(None, description="Comma-separated symbols, e.g. 'AAPL,MSFT'"),
    start_date: date | None = Query(None, description="Start trading date (YYYY-MM-DD). Defaults to today."),
    end_date: date | None = Query(None, description="End trading date (YYYY-MM-DD). Defaults to start_date."),
    bypass_cache: bool = Query(False, description="Skip Redis cache and read directly from DB"),
    volatility_regime: str | None = Query(None, description="Filter: high / normal / low"),
    trend: str | None = Query(None, description="Filter stock trend: bullish / bearish / neutral"),
    sort_by: str | None = Query(None, description="Sort field name inside features (e.g. close_price, daily_return)"),
    sort_order: str = Query("asc", description="Sort order: asc / desc"),
    limit: int = Query(500, ge=1, le=2000, description="Max results per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """统一信号查询 — 支持批量标的、日期范围、缓存控制、波动率/趋势过滤和分页。

    **示例**:
    - ``GET /signals`` — 查询今日全部标的
    - ``GET /signals?symbols=AAPL&symbols=MSFT`` — 今日指定标的
    - ``GET /signals?start_date=2026-03-10&end_date=2026-03-14`` — 日期范围
    - ``GET /signals?symbols=AAPL&bypass_cache=true`` — 跳过缓存
    - ``GET /signals?volatility_regime=high&trend=bullish`` — 条件过滤
    - ``GET /signals?sort_by=daily_return&sort_order=desc&limit=20`` — 排序分页
    """
    from services.signal_service.app.queries import query_signals as _query
    symbols_list = None
    if symbols:
        symbols_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    return await _query(
        symbols=symbols_list,
        start_date=start_date,
        end_date=end_date,
        bypass_cache=bypass_cache,
        volatility_regime=volatility_regime,
        trend=trend,
        sort_by=sort_by,
        sort_order=sort_order,
        limit=limit,
        offset=offset,
    )


# ── Backward-compatible single-symbol alias ────────────────


@router.get("/signals/{symbol}")
async def get_signal_features(
    symbol: str,
    trading_date: date | None = Query(None, description="Trading date (YYYY-MM-DD). Defaults to today."),
    bypass_cache: bool = Query(False, alias="by_pass_cache", description="Skip Redis cache"),
    volatility_regime: str | None = Query(None, description="Filter: high / normal / low"),
    trend: str | None = Query(None, description="Filter stock trend: bullish / bearish / neutral"),
    sort_by: str | None = Query(None, description="Sort field name inside features (e.g. close_price, daily_return)"),
    sort_order: str = Query("asc", description="Sort order: asc / desc"),
):
    """查询单个标的的信号特征（向后兼容快捷入口，内部代理到 /signals）。支持更多过滤。"""
    from services.signal_service.app.queries import query_signals as _query
    result = await _query(
        symbols=[symbol],
        start_date=trading_date,
        end_date=trading_date,
        bypass_cache=bypass_cache,
        volatility_regime=volatility_regime,
        trend=trend,
        sort_by=sort_by,
        sort_order=sort_order,
        limit=1,
        offset=0,
    )
    data = result.get("data", [])
    if not data:
        return {"error": f"No signals for {symbol} on {trading_date or today_trading()}", "_from_cache": False}
    return data[0]


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
