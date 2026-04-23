"""Provider-agnostic resilience layer — retry, rate-limit, concurrency.

All data fetcher implementations delegate to these primitives so that
switching from yfinance to another provider requires ZERO resilience changes.
"""
from __future__ import annotations

import asyncio
import time
from typing import Callable, TypeVar

from shared.utils import get_logger

logger = get_logger("fetcher_resilience")

T = TypeVar("T")


def _get_resilience_settings():
    """Lazy-load to avoid circular imports at module level."""
    from shared.config import get_settings
    return get_settings().data_service.resilience


def _get_option_settings():
    """Lazy-load option-specific settings."""
    from shared.config import get_settings
    return get_settings().data_service.filters.options.cleaning


# ── Synchronous retry (runs inside thread pool) ───────────


def retry_sync(
    fn: Callable[[], T],
    *,
    label: str,
    symbol: str,
) -> T:
    """Execute *fn()* with exponential-backoff retry.

    Config is read from ``data_service.resilience``.
    This function is **blocking** — meant to be called inside
    ``asyncio.to_thread`` or a synchronous code path.

    Raises the last exception if all retries are exhausted.
    """
    cfg = _get_resilience_settings()
    last_exc: Exception | None = None
    total_attempts = max(1, cfg.max_retries + 1)

    import traceback
    for attempt in range(1, total_attempts + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            tb_str = traceback.format_exc()
            if attempt < total_attempts:
                backoff = cfg.backoff_base_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "resilience.retry",
                    label=label,
                    symbol=symbol,
                    attempt=attempt,
                    total_attempts=total_attempts,
                    retries_remaining=total_attempts - attempt,
                    backoff_s=backoff,
                    error=str(exc),
                    traceback=tb_str,
                )
                time.sleep(backoff)
            else:
                logger.warning(
                    "resilience.exhausted",
                    label=label,
                    symbol=symbol,
                    attempts=total_attempts,
                    attempts_exhausted=cfg.max_retries,
                    error=str(exc),
                    traceback=tb_str,
                )
    # Defensive: ensure only BaseException is raised, and log details if not
    import traceback
    if last_exc is not None and isinstance(last_exc, BaseException):
        logger.error(
            "resilience.raise_final_exception",
            label=label,
            symbol=symbol,
            last_exc_type=type(last_exc).__name__,
            last_exc_repr=repr(last_exc),
            traceback=traceback.format_exception(type(last_exc), last_exc, last_exc.__traceback__),
            msg="retry_sync raising final exception"
        )
        raise last_exc
    else:
        logger.error(
            "resilience.raise_non_exception",
            label=label,
            symbol=symbol,
            last_exc_type=type(last_exc).__name__,
            last_exc_repr=repr(last_exc),
            msg="retry_sync exhausted but last_exc is not an exception object"
        )
        raise RuntimeError(f"retry_sync exhausted but last_exc is not an exception: {last_exc!r}")


def rate_limit_sync() -> None:
    """Sleep for the configured per-call rate-limit interval (blocking)."""
    cfg = _get_resilience_settings()
    time.sleep(cfg.rate_limit_per_call_seconds)


# ── Async multi-symbol helpers ─────────────────────────────


async def gather_with_concurrency(
    coros,
    *,
    concurrency: int | None = None,
    inter_symbol_delay: float | None = None,
):
    """Run awaitables with bounded concurrency + inter-symbol rate-limit.

    Parameters
    ----------
    coros : iterable of coroutines
    concurrency : override ``resilience.concurrent_symbols`` if given
    inter_symbol_delay : override ``resilience.rate_limit_per_symbol_seconds``
    """
    cfg = _get_resilience_settings()
    sem = asyncio.Semaphore(concurrency or cfg.concurrent_symbols)
    delay = (
        inter_symbol_delay
        if inter_symbol_delay is not None
        else cfg.rate_limit_per_symbol_seconds
    )

    async def _wrap(coro):
        async with sem:
            result = await coro
            if delay > 0:
                await asyncio.sleep(delay)
            return result

    return await asyncio.gather(*[_wrap(c) for c in coros])
