"""Data Service 存储适配层（双模式统一写入）"""
from __future__ import annotations

from collections.abc import Sequence

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
            last_price, bid, ask, volume, open_interest, iv, delta, gamma, theta, vega
        )
        VALUES (
            :underlying, :symbol, :timestamp, :expiry, :strike, :option_type,
            :last_price, :bid, :ask, :volume, :open_interest, :iv, :delta, :gamma, :theta, :vega
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
            vega = EXCLUDED.vega
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
            last_price, bid, ask, volume, open_interest, iv, delta, gamma, theta, vega
        )
        VALUES (
            :underlying, :symbol, :snapshot_date, :expiry, :strike, :option_type,
            :last_price, :bid, :ask, :volume, :open_interest, :iv, :delta, :gamma, :theta, :vega
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
            vega = EXCLUDED.vega
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
                    """
                    SELECT add_retention_policy(
                        'stock_1min_bars',
                        INTERVAL :window,
                        if_not_exists => TRUE
                    )
                    """
                ),
                {"window": f"{stock_days} days"},
            )
        if option_days > 0:
            await session.execute(
                text(
                    """
                    SELECT add_retention_policy(
                        'option_5min_snapshots',
                        INTERVAL :window,
                        if_not_exists => TRUE
                    )
                    """
                ),
                {"window": f"{option_days} days"},
            )
        await session.commit()

    logger.info("storage.retention_applied", stock_days=stock_days, option_days=option_days)
