"""初始化数据库结构并创建 Timescale hypertable。"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from sqlalchemy import text

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

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
        # 兼容旧 schema：若存在仅基于 id 的主键，Timescale 会拒绝创建 hypertable。
        # 需要先去掉不含分区列 timestamp 的主键/唯一索引。
        await conn.execute(text("ALTER TABLE IF EXISTS stock_1min_bars DROP CONSTRAINT IF EXISTS stock_1min_bars_pkey"))
        await conn.execute(text("ALTER TABLE IF EXISTS option_5min_snapshots DROP CONSTRAINT IF EXISTS option_5min_snapshots_pkey"))
        await conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_stock_1min_bars_symbol_timestamp "
                "ON stock_1min_bars(symbol, \"timestamp\")"
            )
        )
        await conn.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ux_option_5min_snapshots_symbol_timestamp "
                "ON option_5min_snapshots(symbol, \"timestamp\")"
            )
        )
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
