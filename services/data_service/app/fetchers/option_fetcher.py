"""YFinance 期权链采集器 — OptionFetcherProtocol 实现

Retry / rate-limit / concurrency 由 resilience 模块统一处理，
本模块仅关注 yfinance 数据格式转换。
"""
from __future__ import annotations

import asyncio
from datetime import date, datetime

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
from services.data_service.app.fetchers.resilience import (
    gather_with_concurrency,
    rate_limit_sync,
    retry_sync,
)

logger = get_logger("option_fetcher")

# ── Filtering constants ────────────────────────────────────
MIN_DAYS_TO_EXPIRY = 1
MAX_IV = 5.0  # Filter out unreasonable IV


def _get_option_settings():
    from shared.config import get_settings
    return get_settings().data_service.options


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


# ── Core sync fetch (runs in thread pool) ──────────────────


def _fetch_option_chain_sync(symbol: str) -> OptionChainSnapshot | None:
    """同步获取期权链（在线程池中执行）"""
    try:
        opt_cfg = _get_option_settings()
        logger.debug("option_fetcher.fetch_start", symbol=symbol)
        ticker = yf.Ticker(symbol)

        # 1) 获取标的物当前价格
        hist = retry_sync(
            lambda: ticker.history(period="1d"),
            label="option.underlying",
            symbol=symbol,
        )
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
        now = now_utc()

        for expiry_str in expiries:
            expiry_date = pd.to_datetime(expiry_str).date()
            days_to_expiry = (expiry_date - today_trading()).days

            if days_to_expiry < MIN_DAYS_TO_EXPIRY:
                continue

            # 跳过超远期到期日（流动性极低，数据质量差）
            if days_to_expiry > opt_cfg.max_days_to_expiry:
                logger.debug(
                    "option_fetcher.expiry_too_far",
                    symbol=symbol,
                    expiry=expiry_str,
                    days_to_expiry=days_to_expiry,
                )
                continue

            # 请求间限流（provider 无关）
            rate_limit_sync()

            # 带重试的期权链获取
            try:
                chain = retry_sync(
                    lambda _e=expiry_str: ticker.option_chain(_e),
                    label="option.chain",
                    symbol=symbol,
                )
            except Exception as e:
                logger.warning(
                    "option_fetcher.chain_error",
                    symbol=symbol,
                    expiry=expiry_str,
                    error=str(e),
                )
                continue

            # 处理 calls 和 puts
            for option_type, df in [("call", chain.calls), ("put", chain.puts)]:
                if df.empty:
                    continue
                for _, row in df.iterrows():
                    try:
                        iv = float(row.get("impliedVolatility", 0.0))
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

        snapshot = OptionChainSnapshot(
            underlying=symbol,
            underlying_price=underlying_price,
            timestamp=now,
            contracts=contracts,
        )
        enrich_snapshot_greeks(snapshot)
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


# ── Async class ────────────────────────────────────────────


class YFinanceOptionFetcher:
    """yfinance-backed option fetcher implementing OptionFetcherProtocol."""

    async def fetch_current(self, symbol: str) -> OptionChainSnapshot | None:
        return await asyncio.to_thread(_fetch_option_chain_sync, symbol)

    async def fetch_current_multiple(
        self, symbols: list[str]
    ) -> dict[str, OptionChainSnapshot]:
        """Fetch snapshots for multiple symbols (concurrency + rate-limit via resilience)."""
        results: dict[str, OptionChainSnapshot] = {}

        async def _fetch(sym: str) -> None:
            snapshot = await self.fetch_current(sym)
            if snapshot:
                results[sym] = snapshot

        await gather_with_concurrency([_fetch(s) for s in symbols])
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
