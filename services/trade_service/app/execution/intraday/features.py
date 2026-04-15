"""Compute IntradayFeatures from option snapshots and stock bars."""
from __future__ import annotations

import math
import statistics
from datetime import date, datetime

from sqlalchemy import text

from shared.config import get_settings
from shared.db.session import get_timescale_session
from shared.models.intraday import IntradayFeatures
from shared.utils import get_logger

logger = get_logger("intraday.features")


# ── helpers ────────────────────────────────────────────────


def _iv_momentum(series: list[float]) -> float:
    """(latest - previous) / previous"""
    if len(series) < 2 or series[-2] == 0:
        return 0.0
    return (series[-1] - series[-2]) / series[-2]


def _iv_zscore(current: float, all_ivs: list[float]) -> float:
    if len(all_ivs) < 2:
        return 0.0
    try:
        std = statistics.stdev(all_ivs)
    except statistics.StatisticsError:
        return 0.0
    if std == 0:
        return 0.0
    return (current - statistics.mean(all_ivs)) / std


def _realized_vol(closes: list[float]) -> float:
    """Annualised realized vol from 5-min close prices (log returns)."""
    if len(closes) < 2:
        return 0.0
    log_rets = [math.log(closes[i] / closes[i - 1]) for i in range(1, len(closes)) if closes[i - 1] > 0]
    if len(log_rets) < 2:
        return 0.0
    try:
        return statistics.stdev(log_rets) * math.sqrt(252 * 78)
    except statistics.StatisticsError:
        return 0.0


def _ema(values: list[float], span: int) -> float:
    """EMA of *values* with given span. Returns last EMA value."""
    if not values:
        return 0.0
    k = 2.0 / (span + 1)
    ema = values[0]
    for v in values[1:]:
        ema = v * k + ema * (1 - k)
    return ema


# ── main class ─────────────────────────────────────────────


class IntradayFeatureComputer:
    def __init__(self) -> None:
        settings = get_settings()
        cfg = settings.trade_service.intraday_optimizer
        self._lookback_bars: int = getattr(cfg, "lookback_bars", 6)
        self._stock_lookback_bars: int = getattr(cfg, "stock_lookback_bars", 6)

    async def compute(self, symbol: str, trading_date: date) -> IntradayFeatures:
        try:
            return await self._compute(symbol, trading_date)
        except Exception:
            logger.exception("features.error", symbol=symbol, trading_date=str(trading_date))
            return IntradayFeatures(symbol=symbol, timestamp=datetime.utcnow())

    # ── internal ───────────────────────────────────────────

    async def _compute(self, symbol: str, trading_date: date) -> IntradayFeatures:
        async with get_timescale_session() as session:
            # --- IV features ---
            atm_iv_series, latest_ts, all_day_ivs = await self._fetch_iv_series(session, symbol, trading_date)

            atm_iv = atm_iv_series[-1] if atm_iv_series else 0.0
            iv_momentum_5m = _iv_momentum(atm_iv_series)
            iv_zscore = _iv_zscore(atm_iv, all_day_ivs)

            # --- liquidity / OI ---
            avg_spread_pct = 0.0
            option_volume_ratio = 0.0
            top_strike_oi = 0.0
            if latest_ts is not None:
                avg_spread_pct, option_volume_ratio = await self._fetch_liquidity(session, symbol, latest_ts)
                top_strike_oi = await self._fetch_oi_concentration(session, symbol, latest_ts)

            # --- price features ---
            bars = await self._fetch_stock_bars(session, symbol, trading_date)

            closes = [float(r["close"]) for r in bars]
            volumes = [float(r["volume"]) for r in bars]
            vwaps = [float(r["vwap"]) for r in bars if r["vwap"] is not None]

            vwap_dev = 0.0
            if closes and vwaps:
                latest_vwap = vwaps[-1]
                if latest_vwap != 0:
                    vwap_dev = (closes[-1] - latest_vwap) / latest_vwap

            price_mom = 0.0
            if len(closes) >= 2 and closes[-2] != 0:
                price_mom = (closes[-1] - closes[-2]) / closes[-2]

            vol_surge = 1.0
            if len(volumes) >= 2:
                avg_vol = statistics.mean(volumes[:-1])
                if avg_vol > 0:
                    vol_surge = volumes[-1] / avg_vol

            ema20_dev = 0.0
            if closes:
                ema20 = _ema(closes, 20)
                if ema20 != 0:
                    ema20_dev = (closes[-1] - ema20) / ema20

            # --- IV-HV spread ---
            iv_hv_spread = 0.0
            if atm_iv and closes:
                hv = _realized_vol(closes)
                iv_hv_spread = atm_iv - hv

            # --- term structure placeholder ---
            ts_slope_delta = 0.0

            features = IntradayFeatures(
                symbol=symbol,
                timestamp=datetime.utcnow(),
                atm_iv=atm_iv,
                iv_momentum_5m=iv_momentum_5m,
                iv_zscore_intraday=iv_zscore,
                iv_hv_spread=iv_hv_spread,
                term_structure_slope_delta=ts_slope_delta,
                vwap_deviation=vwap_dev,
                price_momentum_5m=price_mom,
                volume_surge_ratio=vol_surge,
                ema20_deviation=ema20_dev,
                avg_bid_ask_spread_pct=avg_spread_pct,
                option_volume_ratio=option_volume_ratio,
                top_strike_oi_concentration=top_strike_oi,
                bars_available=len(atm_iv_series),
            )

        logger.info(
            "features.computed",
            symbol=symbol,
            bars=len(atm_iv_series),
            atm_iv=round(atm_iv, 4),
            iv_mom=round(iv_momentum_5m, 4),
        )
        return features

    # ── DB helpers ─────────────────────────────────────────

    async def _fetch_iv_series(
        self, session, symbol: str, trading_date: date
    ) -> tuple[list[float], datetime | None, list[float]]:
        """Return (atm_iv_series_asc, latest_timestamp, all_day_ivs)."""

        # distinct timestamps for lookback window
        result = await session.execute(
            text(
                "SELECT DISTINCT timestamp "
                "FROM option_5min_snapshots "
                "WHERE underlying = :symbol "
                "  AND timestamp::date = :trading_date "
                "  AND iv > 0 AND iv < 5.0 "
                "ORDER BY timestamp DESC "
                "LIMIT :lookback_bars"
            ),
            {"symbol": symbol, "trading_date": trading_date, "lookback_bars": self._lookback_bars},
        )
        ts_rows = result.mappings().all()
        if not ts_rows:
            return [], None, []

        timestamps = [r["timestamp"] for r in ts_rows]
        latest_ts = timestamps[0]

        # ATM IV per timestamp
        result = await session.execute(
            text(
                "SELECT timestamp, AVG(iv) as atm_iv, underlying_price "
                "FROM option_5min_snapshots "
                "WHERE underlying = :symbol "
                "  AND timestamp IN :timestamps "
                "  AND ABS(strike - underlying_price) / NULLIF(underlying_price, 0) < 0.05 "
                "  AND iv > 0 AND iv < 5.0 "
                "  AND is_tradeable = true "
                "GROUP BY timestamp, underlying_price "
                "ORDER BY timestamp ASC"
            ),
            {"symbol": symbol, "timestamps": tuple(timestamps)},
        )
        iv_rows = result.mappings().all()
        atm_iv_series = [float(r["atm_iv"]) for r in iv_rows]

        # full-day IVs for z-score
        result = await session.execute(
            text(
                "SELECT AVG(iv) as atm_iv "
                "FROM option_5min_snapshots "
                "WHERE underlying = :symbol "
                "  AND timestamp::date = :trading_date "
                "  AND ABS(strike - underlying_price) / NULLIF(underlying_price, 0) < 0.05 "
                "  AND iv > 0 AND iv < 5.0 "
                "  AND is_tradeable = true "
                "GROUP BY timestamp "
                "ORDER BY timestamp ASC"
            ),
            {"symbol": symbol, "trading_date": trading_date},
        )
        all_day_rows = result.mappings().all()
        all_day_ivs = [float(r["atm_iv"]) for r in all_day_rows]

        return atm_iv_series, latest_ts, all_day_ivs

    async def _fetch_liquidity(
        self, session, symbol: str, latest_ts: datetime
    ) -> tuple[float, float]:
        """Return (avg_spread_pct, total_option_volume)."""
        result = await session.execute(
            text(
                "SELECT "
                "  AVG(CASE WHEN bid > 0 AND ask > 0 "
                "      THEN (ask - bid) / ((ask + bid) / 2.0) ELSE NULL END) as avg_spread_pct, "
                "  SUM(volume) as total_opt_volume "
                "FROM option_5min_snapshots "
                "WHERE underlying = :symbol "
                "  AND timestamp = :latest_ts "
                "  AND ABS(strike - underlying_price) / NULLIF(underlying_price, 0) < 0.10 "
                "  AND is_tradeable = true"
            ),
            {"symbol": symbol, "latest_ts": latest_ts},
        )
        row = result.mappings().first()
        if row is None:
            return 0.0, 0.0
        return (
            float(row["avg_spread_pct"] or 0.0),
            float(row["total_opt_volume"] or 0.0),
        )

    async def _fetch_oi_concentration(self, session, symbol: str, latest_ts: datetime) -> float:
        result = await session.execute(
            text(
                "WITH oi_data AS ( "
                "    SELECT open_interest "
                "    FROM option_5min_snapshots "
                "    WHERE underlying = :symbol AND timestamp = :latest_ts AND open_interest > 0 "
                "    ORDER BY open_interest DESC "
                ") "
                "SELECT "
                "    (SELECT SUM(open_interest) FROM (SELECT open_interest FROM oi_data LIMIT 5) t) as top5_oi, "
                "    (SELECT SUM(open_interest) FROM oi_data) as total_oi"
            ),
            {"symbol": symbol, "latest_ts": latest_ts},
        )
        row = result.mappings().first()
        if row is None or not row["total_oi"]:
            return 0.0
        return float(row["top5_oi"] or 0) / float(row["total_oi"])

    async def _fetch_stock_bars(self, session, symbol: str, trading_date: date) -> list:
        """Return 5-min stock bars in ascending order."""
        result = await session.execute(
            text(
                "SELECT timestamp, open, high, low, close, volume, vwap "
                "FROM stock_5min_bars "
                "WHERE symbol = :symbol "
                "  AND timestamp::date = :trading_date "
                "ORDER BY timestamp DESC "
                "LIMIT :stock_lookback_bars"
            ),
            {"symbol": symbol, "trading_date": trading_date, "stock_lookback_bars": self._stock_lookback_bars},
        )
        rows = result.mappings().all()
        return list(reversed(rows))
