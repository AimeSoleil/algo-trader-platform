"""期权链采集器 — 修复设计文档中所有已知问题"""
from __future__ import annotations

import asyncio
from datetime import date, datetime

import pandas as pd
import yfinance as yf

from shared.models.option import OptionChainSnapshot, OptionContract, OptionGreeks, OptionType
from shared.utils import get_logger

logger = get_logger("option_fetcher")

# Minimum acceptable values for filtering
MIN_VOLUME = 0
MIN_OPEN_INTEREST = 0
MIN_DAYS_TO_EXPIRY = 1
MAX_IV = 5.0  # Filter out unreasonable IV


def _fetch_option_chain_sync(symbol: str) -> OptionChainSnapshot | None:
    """同步获取期权链（在线程池中执行）"""
    try:
        ticker = yf.Ticker(symbol)

        # 1) 获取标的物当前价格 — 只调用一次
        hist = ticker.history(period="1d")
        if hist.empty:
            logger.warning("option_fetcher.no_history", symbol=symbol)
            return None
        underlying_price = float(hist["Close"].iloc[-1])

        # 2) 获取所有到期日
        expiries = ticker.options
        if not expiries:
            logger.warning("option_fetcher.no_expiries", symbol=symbol)
            return None

        contracts: list[OptionContract] = []
        now = datetime.now()

        for expiry_str in expiries:
            expiry_date = pd.to_datetime(expiry_str).date()
            days_to_expiry = (expiry_date - date.today()).days

            # 跳过已过期或当天到期的合约（避免除零）
            if days_to_expiry < MIN_DAYS_TO_EXPIRY:
                continue

            try:
                chain = ticker.option_chain(expiry_str)
            except Exception as e:
                logger.warning("option_fetcher.chain_error", symbol=symbol, expiry=expiry_str, error=str(e))
                continue

            # 处理 calls 和 puts（yfinance 分开返回两个 DataFrame）
            for option_type, df in [("call", chain.calls), ("put", chain.puts)]:
                if df.empty:
                    continue

                for _, row in df.iterrows():
                    try:
                        iv = float(row.get("impliedVolatility", 0.0))
                        # 过滤不合理的 IV
                        if iv <= 0 or iv > MAX_IV:
                            continue

                        contract = OptionContract(
                            symbol=str(row.get("contractSymbol", "")),
                            underlying=symbol,
                            expiry=expiry_date,
                            strike=float(row.get("strike", 0)),
                            option_type=OptionType.CALL if option_type == "call" else OptionType.PUT,
                            last_price=float(row.get("lastPrice", 0)),
                            bid=float(row.get("bid", 0)),
                            ask=float(row.get("ask", 0)),
                            volume=int(row.get("volume", 0) or 0),
                            open_interest=int(row.get("openInterest", 0) or 0),
                            greeks=OptionGreeks(iv=iv),
                            timestamp=now,
                        )
                        contracts.append(contract)
                    except Exception as e:
                        logger.debug(
                            "option_fetcher.contract_parse_error",
                            symbol=symbol,
                            expiry=expiry_str,
                            error=str(e),
                        )
                        continue

        snapshot = OptionChainSnapshot(
            underlying=symbol,
            underlying_price=underlying_price,
            timestamp=now,
            contracts=contracts,
        )
        logger.info(
            "option_fetcher.success",
            symbol=symbol,
            contracts_count=len(contracts),
            expiries_count=len(expiries),
        )
        return snapshot

    except Exception as e:
        logger.error("option_fetcher.failed", symbol=symbol, error=str(e))
        return None


async def fetch_option_chain(symbol: str) -> OptionChainSnapshot | None:
    """异步获取期权链（包装同步 yfinance 调用）"""
    return await asyncio.to_thread(_fetch_option_chain_sync, symbol)


async def fetch_multiple_option_chains(symbols: list[str]) -> dict[str, OptionChainSnapshot]:
    """批量异步获取多个标的的期权链"""
    results = {}
    # Use semaphore to limit concurrent yfinance calls (rate limiting)
    semaphore = asyncio.Semaphore(3)

    async def _fetch_with_limit(sym: str):
        async with semaphore:
            snapshot = await fetch_option_chain(sym)
            if snapshot:
                results[sym] = snapshot

    await asyncio.gather(*[_fetch_with_limit(s) for s in symbols])
    return results
