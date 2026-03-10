"""Data Service — REST API 路由"""
from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy import text

from services.data_service.app.main import cache
from services.data_service.app.scheduler import (
    get_current_mode,
    get_data_service_config,
    set_intraday_enabled,
)
from shared.db.session import get_timescale_session

router = APIRouter(tags=["data"])


class ModeSwitchRequest(BaseModel):
    intraday_enabled: bool


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


@router.get("/data/{symbol}")
async def get_symbol_data(
    symbol: str,
    source: Literal["db", "intraday"] | None = None,
):
    """获取标的数据

    source=intraday : 返回盘中 L1 缓存的期权链快照
    source=db / 默认 : 返回 DB 中最新 daily 股票 + 期权数据
    """
    normalized_symbol = symbol.upper()

    if source is None:
        current_mode = get_current_mode()
        source = "intraday" if "intraday" in current_mode else "db"

    if source == "intraday":
        chain = cache.get_realtime_option_chain(normalized_symbol) or []
        if not chain:
            raise HTTPException(
                status_code=404,
                detail=f"No intraday option chain cached for {normalized_symbol}",
            )
        return {
            "symbol": normalized_symbol,
            "source": "intraday",
            "option_chain": chain,
        }

    # source == "db" — 从 TimescaleDB 获取最新数据
    async with get_timescale_session() as session:
        stock_row = (
            await session.execute(
                text(
                    """
                    SELECT symbol, trading_date, open, high, low, close, volume
                    FROM stock_daily
                    WHERE symbol = :symbol
                    ORDER BY trading_date DESC
                    LIMIT 1
                    """
                ),
                {"symbol": normalized_symbol},
            )
        ).mappings().first()

        option_rows = (
            await session.execute(
                text(
                    """
                    SELECT underlying, symbol, snapshot_date, expiry, strike, option_type,
                           last_price, bid, ask, volume, open_interest, iv, delta, gamma, theta, vega
                    FROM option_daily
                    WHERE underlying = :symbol
                    ORDER BY snapshot_date DESC, expiry ASC, strike ASC
                    LIMIT 200
                    """
                ),
                {"symbol": normalized_symbol},
            )
        ).mappings().all()

    if stock_row is None and not option_rows:
        raise HTTPException(status_code=404, detail=f"No data for {normalized_symbol}")

    return {
        "symbol": normalized_symbol,
        "source": "db",
        "daily_stock": dict(stock_row) if stock_row else None,
        "daily_option_chain": [dict(row) for row in option_rows],
    }
