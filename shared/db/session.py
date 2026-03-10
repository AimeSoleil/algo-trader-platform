"""SQLAlchemy 异步会话管理 — 双数据库（TimescaleDB + PostgreSQL）"""
from __future__ import annotations

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
_postgres_engine: AsyncEngine | None = None


def get_timescale_engine() -> AsyncEngine:
    """TimescaleDB 引擎（期权/股票时序数据）"""
    global _timescale_engine
    if _timescale_engine is None:
        settings = get_settings()
        _timescale_engine = create_async_engine(
            settings.database.timescale_url,
            echo=False,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
        )
    return _timescale_engine


def get_postgres_engine() -> AsyncEngine:
    """PostgreSQL 引擎（业务数据：蓝图、订单、持仓）"""
    global _postgres_engine
    if _postgres_engine is None:
        settings = get_settings()
        _postgres_engine = create_async_engine(
            settings.database.postgres_url,
            echo=False,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
        )
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
