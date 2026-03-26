"""YFinance 期权链采集器 — OptionFetcherProtocol 实现"""
from __future__ import annotations

import asyncio
from datetime import date, datetime
import time

import pandas as pd
import yfinance as yf

from shared.models.option import (
    OptionChainSnapshot,
    OptionContract,
    OptionGreeks,
    OptionType,
)
from shared.utils import get_logger, now_utc, today_trading
from services.data_service.app.fetchers.greeks import enrich_snapshot_greeks

logger = get_logger("option_fetcher")

# Minimum acceptable values for filtering
MIN_VOLUME = 0
MIN_OPEN_INTEREST = 0
MIN_DAYS_TO_EXPIRY = 1
MAX_IV = 5.0  # Filter out unreasonable IV
MAX_DAYS_TO_EXPIRY = 730  # ~2 years — skip far-dated low-liquidity expiries
_CHAIN_FETCH_MAX_RETRIES = 3
_CHAIN_FETCH_BACKOFF_BASE = 1.0  # seconds; retry delays: 1s, 2s, 4s


def _safe_int(val, default: int = 0) -> int:
    """Convert to int, treating NaN / None / non-numeric as *default*."""
    if val is None:
        return default
    try:
        f = float(val)
        if pd.isna(f):
            return default
        return int(f)
    except (ValueError, TypeError):
        return default


def _fetch_option_chain_sync(symbol: str) -> OptionChainSnapshot | None:
    """同步获取期权链（在线程池中执行）"""
    try:
        logger.debug("option_fetcher.fetch_start", symbol=symbol)
        ticker = yf.Ticker(symbol)

        # 1) 获取标的物当前价格 — 只调用一次
        logger.debug("option_fetcher.underlying_fetch_start", symbol=symbol, period="1d")
        hist = ticker.history(period="1d")
        logger.debug("option_fetcher.underlying_fetch_done", symbol=symbol, rows=len(hist))
        if hist.empty:
            logger.warning("option_fetcher.no_history", symbol=symbol)
            return None
        underlying_price = float(hist["Close"].iloc[-1])

        # 2) 获取所有到期日
        logger.debug("option_fetcher.expiries_fetch_start", symbol=symbol)
        expiries = ticker.options
        logger.debug("option_fetcher.expiries_fetch_done", symbol=symbol, expiries_count=len(expiries))
        if not expiries:
            logger.warning("option_fetcher.no_expiries", symbol=symbol)
            return None

        contracts: list[OptionContract] = []
        now = now_utc()

        logger.debug("option_fetcher.contract_transform_start", symbol=symbol, expiries_count=len(expiries))
        for expiry_str in expiries:
            expiry_date = pd.to_datetime(expiry_str).date()
            days_to_expiry = (expiry_date - today_trading()).days

            # 跳过已过期或当天到期的合约（避免除零）
            if days_to_expiry < MIN_DAYS_TO_EXPIRY:
                continue

            # 跳过超远期到期日（流动性极低，数据质量差）
            if days_to_expiry > MAX_DAYS_TO_EXPIRY:
                logger.debug(
                    "option_fetcher.expiry_too_far",
                    symbol=symbol,
                    expiry=expiry_str,
                    days_to_expiry=days_to_expiry,
                )
                continue

            # 请求间限流，避免触发 Yahoo Finance 速率限制
            time.sleep(0.5)

            # 带重试的期权链获取
            chain = None
            for attempt in range(1, _CHAIN_FETCH_MAX_RETRIES + 1):
                try:
                    logger.debug("option_fetcher.chain_fetch_start", symbol=symbol, expiry=expiry_str, attempt=attempt)
                    chain = ticker.option_chain(expiry_str)
                    logger.debug(
                        "option_fetcher.chain_fetch_done",
                        symbol=symbol,
                        expiry=expiry_str,
                        calls_rows=len(chain.calls),
                        puts_rows=len(chain.puts),
                    )
                    break  # 成功，退出重试循环
                except Exception as e:
                    if attempt < _CHAIN_FETCH_MAX_RETRIES:
                        backoff = _CHAIN_FETCH_BACKOFF_BASE * (2 ** (attempt - 1))
                        logger.warning(
                            "option_fetcher.chain_retry",
                            symbol=symbol,
                            expiry=expiry_str,
                            attempt=attempt,
                            backoff_s=backoff,
                            error=str(e),
                        )
                        time.sleep(backoff)
                    else:
                        logger.warning(
                            "option_fetcher.chain_error",
                            symbol=symbol,
                            expiry=expiry_str,
                            attempts_exhausted=_CHAIN_FETCH_MAX_RETRIES,
                            error=str(e),
                        )
            if chain is None:
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
                            option_type=(
                                OptionType.CALL if option_type == "call" else OptionType.PUT
                            ),
                            last_price=float(row.get("lastPrice", 0)),
                            bid=float(row.get("bid", 0)),
                            ask=float(row.get("ask", 0)),
                            volume=_safe_int(row.get("volume")),
                            open_interest=_safe_int(row.get("openInterest")),
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
        logger.debug("option_fetcher.contract_transform_done", symbol=symbol, contracts_count=len(contracts))
        logger.debug("option_fetcher.greeks_enrich_start", symbol=symbol, contracts_count=len(contracts))
        enrich_snapshot_greeks(snapshot)
        logger.debug("option_fetcher.greeks_enrich_done", symbol=symbol, contracts_count=len(snapshot.contracts))
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


class YFinanceOptionFetcher:
    """yfinance-backed option fetcher implementing OptionFetcherProtocol."""

    async def fetch_current(self, symbol: str) -> OptionChainSnapshot | None:
        """Fetch the current option chain snapshot."""
        return await asyncio.to_thread(_fetch_option_chain_sync, symbol)

    async def fetch_current_multiple(
        self, symbols: list[str]
    ) -> dict[str, OptionChainSnapshot]:
        """Fetch current snapshots for multiple symbols concurrently."""
        results: dict[str, OptionChainSnapshot] = {}
        semaphore = asyncio.Semaphore(3)

        async def _fetch_with_limit(sym: str) -> None:
            async with semaphore:
                snapshot = await self.fetch_current(sym)
                if snapshot:
                    results[sym] = snapshot

        await asyncio.gather(*[_fetch_with_limit(s) for s in symbols])
        return results

    async def fetch_historical(
        self, symbol: str, target_date: date
    ) -> OptionChainSnapshot | None:
        """yfinance does not support historical option chains — always returns None."""
        logger.info(
            "option_fetcher.historical_not_supported",
            symbol=symbol,
            target_date=str(target_date),
        )
        return None


# ── Backward-compatible module-level helpers ───────────────

_default = YFinanceOptionFetcher()


async def fetch_option_chain(symbol: str) -> OptionChainSnapshot | None:
    """Backward-compatible async fetch (wraps class method)."""
    return await _default.fetch_current(symbol)


async def fetch_multiple_option_chains(
    symbols: list[str],
) -> dict[str, OptionChainSnapshot]:
    """Backward-compatible batch fetch."""
    return await _default.fetch_current_multiple(symbols)
