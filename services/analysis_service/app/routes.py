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
    symbols: list[str] = Field(
        ...,
        min_length=1,
        description="Ticker symbols to analyze, e.g. ['AAPL', 'MSFT']",
    )
    trading_date: str | None = Field(
        None,
        description="Signal date (ISO format). Defaults to today.",
    )


class AnalyzeResponse(BaseModel):
    task_ids: list[dict] = Field(
        default_factory=list,
        description="List of {symbol, task_id} for each queued analysis",
    )
    status: str
    message: str


@router.post("/analysis", status_code=202, response_model=AnalyzeResponse)
async def trigger_analysis(req: AnalyzeRequest):
    """Trigger manual LLM analysis for one or more symbols.

    Each symbol is dispatched as a separate Celery task.
    Returns task_ids that can be polled via GET /analyze/{task_id}.
    Signal features for the symbols must already exist in the DB.
    """
    clean_symbols = list(dict.fromkeys(s.strip().upper() for s in req.symbols if s.strip()))
    if not clean_symbols:
        raise HTTPException(status_code=422, detail="symbols list must not be empty")

    task_ids: list[dict] = []
    for symbol in clean_symbols:
        task = celery_app.send_task(
            "analysis_service.tasks.manual_analyze",
            args=[symbol, req.trading_date],
            queue="analysis",
        )
        task_ids.append({"symbol": symbol, "task_id": task.id})

    date_suffix = f" (date={req.trading_date})" if req.trading_date else ""
    return AnalyzeResponse(
        task_ids=task_ids,
        status="queued",
        message=f"Analysis queued for {len(clean_symbols)} symbol(s): {', '.join(clean_symbols)}{date_suffix}",
    )


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
