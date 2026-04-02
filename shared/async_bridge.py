"""Celery ↔ asyncio bridge — shared across all services."""
from __future__ import annotations

import asyncio


def run_async(coro):
    """Run an async coroutine safely — works whether or not an event loop exists.

    If called from within a running event loop (e.g. nested Celery workers),
    spawns a new thread to avoid ``RuntimeError: This event loop is already running``.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor() as pool:
            return pool.submit(asyncio.run, coro).result()
    return asyncio.run(coro)
