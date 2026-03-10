"""初始化数据库结构并创建 Timescale hypertable。"""
from __future__ import annotations

import asyncio

from sqlalchemy import text

from shared.config import get_settings
from shared.db.session import (
    close_all_engines,
    get_postgres_engine,
    get_timescale_engine,
)
from shared.db.tables import BusinessBase, TimescaleBase


async def init_timescale_schema() -> None:
    print("[init_db] 初始化 TimescaleDB 表结构...")
    engine = get_timescale_engine()

    async with engine.begin() as conn:
        await conn.run_sync(TimescaleBase.metadata.create_all)
    print("[init_db] TimescaleDB 表创建完成")


async def init_business_schema() -> None:
    print("[init_db] 初始化 PostgreSQL 业务表结构...")
    engine = get_postgres_engine()

    async with engine.begin() as conn:
        await conn.run_sync(BusinessBase.metadata.create_all)
    print("[init_db] PostgreSQL 业务表创建完成")


async def create_hypertables() -> None:
    print("[init_db] 创建/确认 Timescale hypertable...")
    engine = get_timescale_engine()

    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))
        await conn.execute(
            text(
                """
                SELECT create_hypertable(
                    'stock_1min_bars',
                    'timestamp',
                    if_not_exists => TRUE,
                    chunk_time_interval => INTERVAL '1 day'
                );
                """
            )
        )
        await conn.execute(
            text(
                """
                SELECT create_hypertable(
                    'option_5min_snapshots',
                    'timestamp',
                    if_not_exists => TRUE,
                    chunk_time_interval => INTERVAL '1 day'
                );
                """
            )
        )

    print("[init_db] hypertable 创建/确认完成")


async def apply_retention_policies() -> None:
    settings = get_settings()
    stock_days = settings.data_service.intraday.hot_storage_retention_days.stock_1min
    option_days = settings.data_service.intraday.hot_storage_retention_days.option_5min

    print(f"[init_db] 应用 retention policy: stock_1min={stock_days}d option_5min={option_days}d")
    engine = get_timescale_engine()

    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))
        if stock_days > 0:
            await conn.execute(
                text(
                    f"""
                    SELECT add_retention_policy(
                        'stock_1min_bars',
                        INTERVAL '{stock_days} days',
                        if_not_exists => TRUE
                    );
                    """
                )
            )
        if option_days > 0:
            await conn.execute(
                text(
                    f"""
                    SELECT add_retention_policy(
                        'option_5min_snapshots',
                        INTERVAL '{option_days} days',
                        if_not_exists => TRUE
                    );
                    """
                )
            )

    print("[init_db] retention policy 应用完成")


async def main() -> None:
    print("[init_db] 开始数据库初始化")
    try:
        await init_timescale_schema()
        await init_business_schema()
        await create_hypertables()
        await apply_retention_policies()
        print("[init_db] 数据库初始化完成（可重复执行）")
    finally:
        await close_all_engines()
        print("[init_db] 已关闭数据库连接")


if __name__ == "__main__":
    asyncio.run(main())
