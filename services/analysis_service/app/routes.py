"""Analysis Service — REST API routes."""
from __future__ import annotations

from celery.result import AsyncResult
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from shared.celery_app import celery_app

router = APIRouter(tags=["analysis"])


# ---------------------------------------------------------------------------
# Blueprint query
# ---------------------------------------------------------------------------


@router.get("/analysis/blueprint/{trading_date}")
async def get_blueprint(
    trading_date: str,
    symbols: str | None = Query(None, description="Comma-separated symbols to filter, e.g. AAPL,NVDA"),
    by_pass_cache: bool = False,
):
    """查询某天的蓝图，可按 symbols 过滤 symbol_plans"""
    from services.analysis_service.app.queries import query_blueprint

    result = await query_blueprint(trading_date, by_pass_cache=by_pass_cache)

    # Apply symbol filter if requested
    if symbols and "blueprint" in result and result["blueprint"]:
        upper_symbols = {s.strip().upper() for s in symbols.split(",") if s.strip()}
        bp = result["blueprint"]
        # blueprint may be dict or JSON-parsed object
        if isinstance(bp, dict) and "symbol_plans" in bp:
            bp["symbol_plans"] = [
                plan for plan in bp["symbol_plans"]
                if plan.get("underlying", "").upper() in upper_symbols
            ]
        result["_symbol_filter"] = sorted(upper_symbols)

    return result


# ---------------------------------------------------------------------------
# Manual analysis
# ---------------------------------------------------------------------------


class AnalyzeRequest(BaseModel):
    symbols: list[str] | str = Field(
        ...,
        description="Ticker symbols to analyze, either ['AAPL', 'MSFT'] or 'AAPL,MSFT'",
    )
    signal_date: str | None = Field(
        None,
        description=(
            "The date of the signal data to analyze (ISO format, e.g. '2026-03-26'). "
            "The generated blueprint will target the next trading day. "
            "Defaults to today."
        ),
    )


class AnalyzeResponse(BaseModel):
    task_id: str = Field(
        description="Celery task ID — poll via GET /analysis/{task_id}",
    )
    symbols: list[str] = Field(
        description="Symbols included in the analysis",
    )
    status: str
    message: str


@router.post("/analysis", status_code=202, response_model=AnalyzeResponse)
async def trigger_analysis(req: AnalyzeRequest):
    """Trigger manual LLM analysis for one or more symbols.

    Dispatches a single Celery task that runs the full agentic pipeline
    (same as auto-triggered generate_daily_blueprint) for the specified
    symbols.  Returns a task_id that can be polled via GET /analysis/{task_id}.
    Signal features for the symbols must already exist in the DB.
    """
    raw_symbols = req.symbols
    if isinstance(raw_symbols, str):
        symbol_items = [s.strip() for s in raw_symbols.split(",")]
    else:
        symbol_items = [s.strip() for s in raw_symbols]

    clean_symbols = list(dict.fromkeys(s.upper() for s in symbol_items if s))
    if not clean_symbols:
        raise HTTPException(status_code=422, detail="symbols must not be empty")

    task = celery_app.send_task(
        "analysis_service.tasks.manual_analyze",
        args=[clean_symbols, req.signal_date],
        queue="analysis",
    )

    date_suffix = f" (signal_date={req.signal_date})" if req.signal_date else ""
    return AnalyzeResponse(
        task_id=task.id,
        symbols=clean_symbols,
        status="queued",
        message=f"Analysis queued for {len(clean_symbols)} symbol(s): {', '.join(clean_symbols)}{date_suffix}",
    )


# ---------------------------------------------------------------------------
# Reasoning context query
# ---------------------------------------------------------------------------


@router.get("/analysis/blueprint/reasoning/{blueprint_id}")
async def get_blueprint_reasoning(
    blueprint_id: str,
    symbols: str | None = Query(None, description="Comma-separated symbols to filter, e.g. AAPL,NVDA"),
):
    """查询蓝图的完整 LLM 推理上下文（agent outputs、critic 反馈、原始响应等）。

    可用于审查 LLM 分析结论是否合理。可选按 symbols 过滤只返回相关 symbol 的分析结果。
    """
    from services.analysis_service.app.queries import query_reasoning

    symbol_filter: set[str] | None = None
    if symbols:
        symbol_filter = {s.strip().upper() for s in symbols.split(",") if s.strip()}

    result = await query_reasoning(blueprint_id, symbol_filter=symbol_filter)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


# ---------------------------------------------------------------------------
# Task status polling
# ---------------------------------------------------------------------------


@router.get("/analysis/{task_id}")
async def get_analysis_status(task_id: str):
    """Poll the status of a manual analysis task."""
    result = AsyncResult(task_id, app=celery_app)

    response: dict = {
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
