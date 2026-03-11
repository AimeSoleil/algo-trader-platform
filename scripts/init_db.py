"""Initialize DB schema + Timescale hypertables (idempotent).

Features:
1) Safe repeated execution.
2) Legacy schema reconciliation for hypertable compatibility.
3) Optional destructive operations for reset/troubleshooting.

Usage examples:
    uv run python -m scripts.init_db
    uv run python -m scripts.init_db --truncate-all --yes
    uv run python -m scripts.init_db --drop-all --yes
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from sqlalchemy import text

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from shared.config import get_settings
from shared.db.session import close_all_engines, get_postgres_engine, get_timescale_engine
from shared.db.tables import BusinessBase, TimescaleBase


TIMESCALE_TABLES = [
    "stock_1min_bars",
    "option_5min_snapshots",
    "stock_daily",
    "option_daily",
]

BUSINESS_TABLES = [
    "llm_trading_blueprint",
    "orders",
    "positions",
    "signal_features",
    "backfill_logs",
    "watchlist_symbols",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize or reset project databases")
    parser.add_argument(
        "--truncate-all",
        action="store_true",
        help="Truncate all known tables (keep schema), then re-init",
    )
    parser.add_argument(
        "--drop-all",
        action="store_true",
        help="Drop all known tables, then re-init",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Required for destructive operations",
    )
    return parser.parse_args()


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


async def reconcile_timeseries_constraints() -> None:
    """Normalize legacy constraints so hypertable conversion always succeeds."""
    print("[init_db] 对齐时序表约束（兼容旧 schema）...")
    engine = get_timescale_engine()
    async with engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS timescaledb"))

        # 1) 去重，避免后续添加复合主键失败
        await conn.execute(
            text(
                """
                DELETE FROM stock_1min_bars a
                USING stock_1min_bars b
                WHERE a.ctid < b.ctid
                  AND a.symbol = b.symbol
                  AND a."timestamp" = b."timestamp";
                """
            )
        )
        await conn.execute(
            text(
                """
                DELETE FROM option_5min_snapshots a
                USING option_5min_snapshots b
                WHERE a.ctid < b.ctid
                  AND a.symbol = b.symbol
                  AND a."timestamp" = b."timestamp";
                """
            )
        )

        # 2) 清理旧主键（若存在 id 主键会与 hypertable 规则冲突）
        await conn.execute(text("ALTER TABLE IF EXISTS stock_1min_bars DROP CONSTRAINT IF EXISTS stock_1min_bars_pkey"))
        await conn.execute(text("ALTER TABLE IF EXISTS option_5min_snapshots DROP CONSTRAINT IF EXISTS option_5min_snapshots_pkey"))

        # 3) 强制复合主键（包含分区列 timestamp）
        await conn.execute(
            text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conname = 'stock_1min_bars_pkey'
                    ) THEN
                        ALTER TABLE stock_1min_bars
                        ADD CONSTRAINT stock_1min_bars_pkey PRIMARY KEY (symbol, "timestamp");
                    END IF;
                END$$;
                """
            )
        )
        await conn.execute(
            text(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conname = 'option_5min_snapshots_pkey'
                    ) THEN
                        ALTER TABLE option_5min_snapshots
                        ADD CONSTRAINT option_5min_snapshots_pkey PRIMARY KEY (symbol, "timestamp");
                    END IF;
                END$$;
                """
            )
        )
    print("[init_db] 时序表约束已对齐")


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

        # 幂等更新策略：先移除旧策略（若存在）再添加目标值
        await conn.execute(text("SELECT remove_retention_policy('stock_1min_bars', if_exists => TRUE)"))
        await conn.execute(text("SELECT remove_retention_policy('option_5min_snapshots', if_exists => TRUE)"))

        if stock_days > 0:
            await conn.execute(
                text(
                    f"""
                    SELECT add_retention_policy(
                        'stock_1min_bars',
                        INTERVAL '{int(stock_days)} days',
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
                        INTERVAL '{int(option_days)} days',
                        if_not_exists => TRUE
                    );
                    """
                )
            )

    print("[init_db] retention policy 应用完成")


async def truncate_all_tables() -> None:
    print("[init_db] TRUNCATE 所有已知表...")

    t_engine = get_timescale_engine()
    async with t_engine.begin() as conn:
        await conn.execute(text(
            "TRUNCATE TABLE "
            "stock_1min_bars, option_5min_snapshots, stock_daily, option_daily "
            "RESTART IDENTITY"
        ))

    p_engine = get_postgres_engine()
    async with p_engine.begin() as conn:
        await conn.execute(text(
            "TRUNCATE TABLE "
            "llm_trading_blueprint, orders, positions, signal_features, backfill_logs, watchlist_symbols "
            "RESTART IDENTITY"
        ))

    print("[init_db] TRUNCATE 完成")


async def drop_all_tables() -> None:
    print("[init_db] DROP 所有已知表...")

    t_engine = get_timescale_engine()
    async with t_engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS option_5min_snapshots CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS stock_1min_bars CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS option_daily CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS stock_daily CASCADE"))

    p_engine = get_postgres_engine()
    async with p_engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS backfill_logs CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS signal_features CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS positions CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS orders CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS llm_trading_blueprint CASCADE"))
        await conn.execute(text("DROP TABLE IF EXISTS watchlist_symbols CASCADE"))

    print("[init_db] DROP 完成")


async def main() -> None:
    args = parse_args()
    print("[init_db] 开始数据库初始化")

    if (args.truncate_all or args.drop_all) and not args.yes:
        raise RuntimeError("Destructive operation requires --yes")

    try:
        if args.drop_all:
            await drop_all_tables()
        elif args.truncate_all:
            await truncate_all_tables()

        await init_timescale_schema()
        await init_business_schema()
        await reconcile_timeseries_constraints()
        await create_hypertables()
        await apply_retention_policies()
        print("[init_db] 数据库初始化完成（幂等，可重复执行）")
    finally:
        await close_all_engines()
        print("[init_db] 已关闭数据库连接")


if __name__ == "__main__":
    asyncio.run(main())
