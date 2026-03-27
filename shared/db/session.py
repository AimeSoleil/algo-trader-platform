"""SQLAlchemy 异步会话管理 — 双数据库（TimescaleDB + PostgreSQL）

在 Celery prefork worker 中，每次 asyncio.run() 都会新建 event loop。
AsyncEngine 内部连接池绑定在创建时的 loop 上，跨 loop 复用会触发
"Future attached to a different loop"。

解法：引擎工厂记录创建时的 loop id，若检测到 loop 变化则自动丢弃旧
引擎（dispose(close=False) 避免在错误 loop 上关闭连接），然后重建。
同一 loop 内的 DB 调用仍享受连接池。
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from shared.config.settings import get_settings

# ── 引擎缓存 ──────────────────────────────────────────────
_timescale_engine: AsyncEngine | None = None
_timescale_loop_id: int | None = None

_postgres_engine: AsyncEngine | None = None
_postgres_loop_id: int | None = None


def _current_loop_id() -> int:
    """Return id of the running event loop, or 0 if none."""
    try:
        return id(asyncio.get_running_loop())
    except RuntimeError:
        return 0


def get_timescale_engine() -> AsyncEngine:
    """TimescaleDB 引擎（期权/股票时序数据）"""
    global _timescale_engine, _timescale_loop_id

    loop_id = _current_loop_id()
    if _timescale_engine is not None and _timescale_loop_id != loop_id:
        # loop 已切换，旧连接池不可用 — 丢弃引用即可（close=False 不触发旧 loop IO）
        _timescale_engine.sync_engine.dispose(close=False)
        _timescale_engine = None

    if _timescale_engine is None:
        settings = get_settings()
        _timescale_engine = create_async_engine(
            settings.infra.database.timescale_url,
            echo=False,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
        )
        _timescale_loop_id = loop_id
    return _timescale_engine


def get_postgres_engine() -> AsyncEngine:
    """PostgreSQL 引擎（业务数据：蓝图、订单、持仓）"""
    global _postgres_engine, _postgres_loop_id

    loop_id = _current_loop_id()
    if _postgres_engine is not None and _postgres_loop_id != loop_id:
        _postgres_engine.sync_engine.dispose(close=False)
        _postgres_engine = None

    if _postgres_engine is None:
        settings = get_settings()
        _postgres_engine = create_async_engine(
            settings.infra.database.postgres_url,
            echo=False,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )
        _postgres_loop_id = loop_id
    return _postgres_engine


def _make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@asynccontextmanager
async def get_timescale_session() -> AsyncGenerator[AsyncSession, None]:
    factory = _make_session_factory(get_timescale_engine())
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


@asynccontextmanager
async def get_postgres_session() -> AsyncGenerator[AsyncSession, None]:
    factory = _make_session_factory(get_postgres_engine())
    async with factory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def close_all_engines() -> None:
    """优雅关闭所有数据库连接"""
    global _timescale_engine, _postgres_engine
    if _timescale_engine:
        await _timescale_engine.dispose()
        _timescale_engine = None
    if _postgres_engine:
        await _postgres_engine.dispose()
        _postgres_engine = None
