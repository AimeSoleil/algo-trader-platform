"""Portfolio Service — REST API 路由"""
from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Query

from services.trade_service.app.portfolio.service import (
    get_performance,
    get_portfolio_snapshot,
    get_positions,
)

router = APIRouter(tags=["portfolio"])


@router.get("/portfolio/snapshot")
async def portfolio_snapshot():
    return await get_portfolio_snapshot()


@router.get("/portfolio/positions")
async def portfolio_positions():
    return await get_positions()


@router.get("/portfolio/performance")
async def portfolio_performance(
    trading_date: date = Query(..., description="Target trading_date (YYYY-MM-DD)"),
):
    return await get_performance(trading_date)
