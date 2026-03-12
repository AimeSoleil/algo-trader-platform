"""Signal Service — REST API routes."""
from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(tags=["signal"])


@router.get("/signals/batch")
async def get_batch_signal_features(date: str | None = None):
    """查询当日所有标的的信号特征（Analysis Service 调用）"""
    from services.signal_service.app.queries import query_batch_signal_features
    return await query_batch_signal_features(date)


@router.get("/signals/{symbol}")
async def get_signal_features(symbol: str, date: str | None = None):
    """查询某标的的信号特征"""
    from services.signal_service.app.queries import query_signal_features
    return await query_signal_features(symbol, date)
