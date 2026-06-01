"""Celery ↔ asyncio bridge — shared across all services."""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import TypeVar

from shared.utils import get_logger

logger = get_logger("async_bridge")

_T = TypeVar("_T")


async def _close_async_resources() -> None:
    from shared.db.session import close_all_engines
    from shared.redis_pool import close_redis_pool

    await close_all_engines()
    await close_redis_pool()


async def _run_and_cleanup(coro: Awaitable[_T]) -> _T:
    original_error: BaseException | None = None
    try:
        return await coro
    except BaseException as exc:
        original_error = exc
        raise
    finally:
        try:
            await _close_async_resources()
        except Exception as cleanup_exc:
            if original_error is None:
                raise
            logger.warning("async_bridge.cleanup_failed", error=str(cleanup_exc))


def run_async(coro: Awaitable[_T]) -> _T:
    """Run an async coroutine safely — works whether or not an event loop exists.

    If called from within a running event loop (e.g. nested Celery workers),
    spawns a new thread to avoid ``RuntimeError: This event loop is already running``.

    The coroutine always runs inside a short-lived loop and closes shared async
    DB/Redis clients before that loop exits. This prevents Celery tasks that use
    successive ``asyncio.run()`` loops from accumulating stale connections.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, _run_and_cleanup(coro)).result()
    return asyncio.run(_run_and_cleanup(coro))
