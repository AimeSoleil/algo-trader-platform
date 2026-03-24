"""Data Service 存储适配层（双模式统一写入）"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from sqlalchemy import text

from shared.db.session import get_timescale_session
from shared.utils import get_logger

logger = get_logger("data_storage")


async def write_intraday_stock(rows: Sequence[dict]) -> int:
    if not rows:
        return 0

    stmt = text(
        """
        INSERT INTO stock_1min_bars (symbol, timestamp, open, high, low, close, volume)
        VALUES (:symbol, :timestamp, :open, :high, :low, :close, :volume)
        ON CONFLICT (symbol, timestamp)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
        """
    )

    async with get_timescale_session() as session:
        await session.execute(stmt, list(rows))
        await session.commit()

    return len(rows)


async def write_intraday_options(rows: Sequence[dict]) -> int:
    if not rows:
        return 0

    stmt = text(
        """
        INSERT INTO option_5min_snapshots (
            underlying, symbol, timestamp, expiry, strike, option_type,
            last_price, bid, ask, volume, open_interest, iv, delta, gamma, theta, vega,
            underlying_price
        )
        VALUES (
            :underlying, :symbol, :timestamp, :expiry, :strike, :option_type,
            :last_price, :bid, :ask, :volume, :open_interest, :iv, :delta, :gamma, :theta, :vega,
            :underlying_price
        )
        ON CONFLICT (symbol, timestamp)
        DO UPDATE SET
            last_price = EXCLUDED.last_price,
            bid = EXCLUDED.bid,
            ask = EXCLUDED.ask,
            volume = EXCLUDED.volume,
            open_interest = EXCLUDED.open_interest,
            iv = EXCLUDED.iv,
            delta = EXCLUDED.delta,
            gamma = EXCLUDED.gamma,
            theta = EXCLUDED.theta,
            vega = EXCLUDED.vega,
            underlying_price = EXCLUDED.underlying_price
        """
    )

    async with get_timescale_session() as session:
        await session.execute(stmt, list(rows))
        await session.commit()

    return len(rows)


async def write_swing_stock(rows: Sequence[dict]) -> int:
    if not rows:
        return 0

    stmt = text(
        """
        INSERT INTO stock_daily (symbol, trading_date, open, high, low, close, volume)
        VALUES (:symbol, :trading_date, :open, :high, :low, :close, :volume)
        ON CONFLICT (symbol, trading_date)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume
        """
    )

    async with get_timescale_session() as session:
        await session.execute(stmt, list(rows))
        await session.commit()

    return len(rows)


async def write_swing_options(rows: Sequence[dict]) -> int:
    if not rows:
        return 0

    stmt = text(
        """
        INSERT INTO option_daily (
            underlying, symbol, snapshot_date, expiry, strike, option_type,
            last_price, bid, ask, volume, open_interest, iv, delta, gamma, theta, vega,
            underlying_price
        )
        VALUES (
            :underlying, :symbol, :snapshot_date, :expiry, :strike, :option_type,
            :last_price, :bid, :ask, :volume, :open_interest, :iv, :delta, :gamma, :theta, :vega,
            :underlying_price
        )
        ON CONFLICT (symbol, snapshot_date)
        DO UPDATE SET
            last_price = EXCLUDED.last_price,
            bid = EXCLUDED.bid,
            ask = EXCLUDED.ask,
            volume = EXCLUDED.volume,
            open_interest = EXCLUDED.open_interest,
            iv = EXCLUDED.iv,
            delta = EXCLUDED.delta,
            gamma = EXCLUDED.gamma,
            theta = EXCLUDED.theta,
            vega = EXCLUDED.vega,
            underlying_price = EXCLUDED.underlying_price
        """
    )

    async with get_timescale_session() as session:
        await session.execute(stmt, list(rows))
        await session.commit()

    return len(rows)


async def apply_intraday_retention(stock_days: int, option_days: int) -> None:
    if stock_days <= 0 and option_days <= 0:
        return

    async with get_timescale_session() as session:
        if stock_days > 0:
            await session.execute(
                text(
                    f"SELECT add_retention_policy("
                    f"'stock_1min_bars', INTERVAL '{int(stock_days)} days', "
                    f"if_not_exists => TRUE)"
                )
            )
        if option_days > 0:
            await session.execute(
                text(
                    f"SELECT add_retention_policy("
                    f"'option_5min_snapshots', INTERVAL '{int(option_days)} days', "
                    f"if_not_exists => TRUE)"
                )
            )
        await session.commit()

    logger.info("storage.retention_applied", stock_1min_days=stock_days, option_5min_days=option_days)


async def aggregate_daily_from_snapshots(trading_date: date) -> dict:
    """Backfill ``option_daily`` from the last intraday 5-min snapshot of the day.

    For each (symbol), pick the row with the latest ``timestamp`` on *trading_date*
    and upsert it into ``option_daily``.  This replaces post-market yfinance data
    (which has bid=ask=0 and unreliable IV) with the last real market snapshot.

    Returns {"rows_upserted": int, "symbols_covered": int}.
    """
    from datetime import datetime, time, timezone

    day_start = datetime.combine(trading_date, time.min, tzinfo=timezone.utc)
    day_end = datetime.combine(trading_date, time(23, 59, 59), tzinfo=timezone.utc)

    # CTE: rank snapshots per symbol, keep only the last one
    upsert_sql = text(
        """
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY "timestamp" DESC) AS rn
            FROM option_5min_snapshots
            WHERE "timestamp" >= :day_start
              AND "timestamp" <= :day_end
        )
        INSERT INTO option_daily (
            underlying, symbol, snapshot_date, expiry, strike, option_type,
            last_price, bid, ask, volume, open_interest,
            iv, delta, gamma, theta, vega, underlying_price
        )
        SELECT
            underlying, symbol, :snapshot_date, expiry, strike, option_type,
            last_price, bid, ask, volume, open_interest,
            iv, delta, gamma, theta, vega, underlying_price
        FROM ranked
        WHERE rn = 1
        ON CONFLICT (symbol, snapshot_date)
        DO UPDATE SET
            last_price       = EXCLUDED.last_price,
            bid              = EXCLUDED.bid,
            ask              = EXCLUDED.ask,
            volume           = EXCLUDED.volume,
            open_interest    = EXCLUDED.open_interest,
            iv               = EXCLUDED.iv,
            delta            = EXCLUDED.delta,
            gamma            = EXCLUDED.gamma,
            theta            = EXCLUDED.theta,
            vega             = EXCLUDED.vega,
            underlying_price = EXCLUDED.underlying_price
        """
    )

    async with get_timescale_session() as session:
        result = await session.execute(
            upsert_sql,
            {
                "day_start": day_start,
                "day_end": day_end,
                "snapshot_date": trading_date,
            },
        )
        await session.commit()
        rows_upserted = result.rowcount

    # Count distinct symbols written
    async with get_timescale_session() as session:
        count_result = await session.execute(
            text(
                "SELECT COUNT(DISTINCT symbol) FROM option_daily "
                "WHERE snapshot_date = :sd"
            ),
            {"sd": trading_date},
        )
        symbols_covered = count_result.scalar() or 0

    logger.info(
        "storage.aggregate_daily_done",
        trading_date=str(trading_date),
        rows_upserted=rows_upserted,
        symbols_covered=symbols_covered,
    )
    return {"rows_upserted": rows_upserted, "symbols_covered": symbols_covered}


async def aggregate_iv_daily(trading_date: date) -> dict:
    """Compute per-underlying IV summary from intraday snapshots and write to ``option_iv_daily``.

    Metrics:
    - avg_iv: mean IV of all contracts with 0 < iv < 5
    - atm_iv: mean IV of contracts whose strike is within 5% of underlying_price
    - call_iv / put_iv: mean IV split by option_type
    - sample_size: number of distinct contracts used

    Returns {"underlyings_written": int}.
    """
    from datetime import datetime, time, timezone

    day_start = datetime.combine(trading_date, time.min, tzinfo=timezone.utc)
    day_end = datetime.combine(trading_date, time(23, 59, 59), tzinfo=timezone.utc)

    # Use the *last* snapshot per symbol (same CTE as aggregate_daily_from_snapshots)
    # then aggregate per underlying
    agg_sql = text(
        """
        WITH last_snap AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY "timestamp" DESC) AS rn
            FROM option_5min_snapshots
            WHERE "timestamp" >= :day_start
              AND "timestamp" <= :day_end
              AND iv > 0 AND iv < 5
        ),
        filtered AS (
            SELECT * FROM last_snap WHERE rn = 1
        )
        INSERT INTO option_iv_daily (
            underlying, trading_date, avg_iv, atm_iv, call_iv, put_iv,
            sample_size, underlying_price
        )
        SELECT
            underlying,
            :trading_date,
            AVG(iv),
            AVG(iv) FILTER (
                WHERE underlying_price IS NOT NULL
                  AND ABS(strike - underlying_price) / underlying_price <= 0.05
            ),
            AVG(iv) FILTER (WHERE option_type = 'call'),
            AVG(iv) FILTER (WHERE option_type = 'put'),
            COUNT(DISTINCT symbol),
            MAX(underlying_price)
        FROM filtered
        GROUP BY underlying
        ON CONFLICT (underlying, trading_date)
        DO UPDATE SET
            avg_iv           = EXCLUDED.avg_iv,
            atm_iv           = EXCLUDED.atm_iv,
            call_iv          = EXCLUDED.call_iv,
            put_iv           = EXCLUDED.put_iv,
            sample_size      = EXCLUDED.sample_size,
            underlying_price = EXCLUDED.underlying_price
        """
    )

    async with get_timescale_session() as session:
        result = await session.execute(
            agg_sql,
            {
                "day_start": day_start,
                "day_end": day_end,
                "trading_date": trading_date,
            },
        )
        await session.commit()
        underlyings_written = result.rowcount

    logger.info(
        "storage.aggregate_iv_daily_done",
        trading_date=str(trading_date),
        underlyings_written=underlyings_written,
    )
    return {"underlyings_written": underlyings_written}
