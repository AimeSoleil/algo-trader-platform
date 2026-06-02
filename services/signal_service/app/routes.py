"""Signal Service — REST API routes."""
from __future__ import annotations

from datetime import date

from celery.result import AsyncResult
from fastapi import APIRouter, HTTPException, Query
import pandas_market_calendars as mcal
from pydantic import BaseModel, Field, model_validator

from shared.celery_app import celery_app
from shared.utils import today_trading

router = APIRouter(tags=["signal"])


# ── Request / Response models ──────────────────────────────


class SignalComputeRequest(BaseModel):
    """Manual signal generation trigger request."""
    trading_date: date | None = Field(None, description="Target date (ISO format). Defaults to today.")
    start_date: date | None = Field(
        None,
        description="Start trading date (ISO format). Use with end_date for a market-day range.",
    )
    end_date: date | None = Field(
        None,
        description="End trading date (ISO format). Defaults to start_date when omitted.",
    )
    symbols: list[str] | None = Field(
        None,
        description="Specific symbols to compute. Defaults to the full data/signal watchlist.",
    )

    @model_validator(mode="after")
    def validate_dates(self) -> SignalComputeRequest:
        has_range = self.start_date is not None or self.end_date is not None
        if self.trading_date is not None and has_range:
            raise ValueError("Provide either trading_date or start_date/end_date, not both")
        if self.end_date is not None and self.start_date is None:
            raise ValueError("start_date is required when end_date is provided")
        if self.start_date is not None and self.end_date is not None and self.start_date > self.end_date:
            raise ValueError("start_date cannot be after end_date")
        return self


class SignalComputeResponse(BaseModel):
    task_id: str | None = None
    task_ids: list[str] = Field(default_factory=list)
    trading_dates: list[date] = Field(default_factory=list)
    status: str = "queued"
    message: str = ""


_NYSE_CALENDAR = mcal.get_calendar("NYSE")


def _market_days_in_range(start_date: date, end_date: date) -> list[date]:
    schedule = _NYSE_CALENDAR.schedule(start_date=start_date, end_date=end_date)
    return [session.date() for session in schedule.index.to_pydatetime()]


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
        queried_start = result.get("filters_applied", {}).get("start_date", str(trading_date or today_trading()))
        return {
            "error": f"No signals for {symbol} on {queried_start}",
            "hint": "Use GET /signals?symbols={symbol} (no date) to auto-resolve the latest available date.",
            "_from_cache": False,
        }
    return data[0]


@router.post("/signals/compute", status_code=202, response_model=SignalComputeResponse)
async def trigger_signal_compute(req: SignalComputeRequest):
    """手动触发当日、单日期或日期范围的批量信号计算任务。

    可指定 symbols 仅计算特定标的，否则使用完整 data/signal watchlist。
    日期范围请求会按 NYSE 市场日展开，并为每个交易日分别派发一个 Celery task。
    """
    today = today_trading()
    if req.start_date is not None:
        range_end = req.end_date or req.start_date
        clean_dates = _market_days_in_range(req.start_date, range_end)
        if not clean_dates:
            raise HTTPException(status_code=422, detail="No market days found in the requested date range")
    else:
        clean_dates = [req.trading_date or today]

    future_dates = [td.isoformat() for td in clean_dates if td > today]
    if future_dates:
        raise HTTPException(
            status_code=422,
            detail=f"trading_date cannot be in the future: {future_dates}",
        )

    clean_symbols: list[str] | None = None
    if req.symbols:
        clean_symbols = list(dict.fromkeys(s.strip().upper() for s in req.symbols if s.strip()))
        if not clean_symbols:
            raise HTTPException(status_code=422, detail="symbols list must not be empty when provided")

    task_ids: list[str] = []
    for td in clean_dates:
        task = celery_app.send_task(
            "signal_service.tasks.compute_daily_signals",
            args=[td.isoformat()],
            kwargs={"symbols": clean_symbols},
            queue="signal",
        )
        task_ids.append(task.id)

    symbols_msg = f", symbols={clean_symbols}" if clean_symbols else " (full data/signal watchlist)"
    task_id = task_ids[0] if len(task_ids) == 1 else None
    if req.start_date is not None:
        dates_text = ", ".join(td.isoformat() for td in clean_dates)
        message = f"Signal generation queued for market_days=[{dates_text}]{symbols_msg}"
    elif len(clean_dates) == 1:
        message = f"Signal generation queued for trading_date={clean_dates[0]}{symbols_msg}"
    else:
        dates_text = ", ".join(td.isoformat() for td in clean_dates)
        message = f"Signal generation queued for trading_dates=[{dates_text}]{symbols_msg}"

    return SignalComputeResponse(
        task_id=task_id,
        task_ids=task_ids,
        trading_dates=clean_dates,
        status="queued",
        message=message,
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
