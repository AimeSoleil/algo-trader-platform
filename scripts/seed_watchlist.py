"""将配置中的 watchlist 写入/更新到 PostgreSQL。"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from sqlalchemy import text

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from shared.config.settings import get_settings
from shared.db.session import close_all_engines, get_postgres_engine


CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS watchlist_symbols (
    symbol TEXT PRIMARY KEY,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

UPSERT_SQL = """
INSERT INTO watchlist_symbols(symbol, enabled, updated_at)
VALUES (:symbol, TRUE, NOW())
ON CONFLICT (symbol)
DO UPDATE SET
    enabled = EXCLUDED.enabled,
    updated_at = NOW();
"""


async def main() -> None:
    settings = get_settings()
    symbols = sorted({symbol.strip().upper() for symbol in settings.common.watchlist.all if symbol and symbol.strip()})

    print("[seed_watchlist] 开始同步 watchlist_symbols")
    print(f"[seed_watchlist] 配置中读取到 {len(symbols)} 个 symbol")

    if not symbols:
        print("[seed_watchlist] watchlist 为空，跳过写入")
        return

    engine = get_postgres_engine()
    try:
        async with engine.begin() as conn:
            await conn.execute(text(CREATE_TABLE_SQL))
            for symbol in symbols:
                await conn.execute(text(UPSERT_SQL), {"symbol": symbol})

        print(f"[seed_watchlist] 写入/更新完成，共处理 {len(symbols)} 个 symbol")
        print("[seed_watchlist] 可重复执行（幂等）")
    finally:
        await close_all_engines()
        print("[seed_watchlist] 已关闭数据库连接")


if __name__ == "__main__":
    asyncio.run(main())
