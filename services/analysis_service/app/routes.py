"""Analysis Service — REST API routes."""
from __future__ import annotations

from celery.result import AsyncResult
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from shared.celery_app import celery_app

router = APIRouter(tags=["analysis"])


# ---------------------------------------------------------------------------
# Blueprint query
# ---------------------------------------------------------------------------


@router.get("/blueprint/{trading_date}")
async def get_blueprint(trading_date: str):
    """查询某天的蓝图"""
    from services.analysis_service.app.queries import query_blueprint
    return await query_blueprint(trading_date)


# ---------------------------------------------------------------------------
# Manual analysis
# ---------------------------------------------------------------------------


class AnalyzeRequest(BaseModel):
    symbol: str = Field(..., description="Ticker symbol to analyze, e.g. AAPL")
    trading_date: str | None = Field(
        None,
        description="Signal date (ISO format). Defaults to today.",
    )


class AnalyzeResponse(BaseModel):
    task_id: str
    status: str
    message: str


@router.post("/analyze", status_code=202, response_model=AnalyzeResponse)
async def trigger_analysis(req: AnalyzeRequest):
    """Trigger manual LLM analysis for a single symbol.

    Returns a task_id that can be polled via GET /analyze/{task_id}.
    Signal features for the symbol must already exist in the DB.
    """
    symbol = req.symbol.strip().upper()
    if not symbol:
        raise HTTPException(status_code=422, detail="symbol must not be empty")

    task = celery_app.send_task(
        "analysis_service.tasks.manual_analyze",
        args=[symbol, req.trading_date],
    )

    return AnalyzeResponse(
        task_id=task.id,
        status="queued",
        message=f"Analysis queued for {symbol}"
                + (f" (date={req.trading_date})" if req.trading_date else ""),
    )


@router.get("/analyze/{task_id}")
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
