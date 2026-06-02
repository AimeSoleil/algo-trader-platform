from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.data_service.app.tasks import earnings
from shared.notifier.base import EventType


def test_refresh_earnings_cache_notifies_start_and_finish(monkeypatch):
    settings = SimpleNamespace(
        common=SimpleNamespace(
            watchlist=SimpleNamespace(all=["AAPL", "SPY", "^VIX", "MSFT"])
        )
    )
    events = []

    monkeypatch.setattr(earnings, "get_settings", lambda: settings)
    monkeypatch.setattr(earnings, "notify_sync", events.append)

    def _run_async(awaitable):
        awaitable.close()
        return {"updated": 2, "failed": 0, "elapsed_s": 1.75}

    monkeypatch.setattr(earnings, "run_async", _run_async)

    result = earnings.refresh_earnings_cache.run()

    assert result == {"updated": 2, "failed": 0, "elapsed_s": 1.75}
    assert [event.event_type for event in events] == [
        EventType.EARNINGS_CACHE_REFRESH_STARTED,
        EventType.EARNINGS_CACHE_REFRESH_FINISHED,
    ]
    assert events[0].payload == {
        "cache_key": earnings.EARNINGS_HASH_KEY,
        "symbols": "2",
    }
    assert events[1].payload == {
        "cache_key": earnings.EARNINGS_HASH_KEY,
        "symbols": "2",
        "updated": "2",
        "failed": "0",
        "elapsed_s": "1.75",
    }
    assert "Refreshing next-earnings cache for 2 symbols." == events[0].message
    assert "2 updated, 0 unavailable, 1.75s elapsed." in events[1].message


def test_refresh_earnings_cache_notifies_failure_before_retry(monkeypatch):
    settings = SimpleNamespace(
        common=SimpleNamespace(
            watchlist=SimpleNamespace(all=["AAPL", "SPY", "^VIX", "MSFT"])
        )
    )
    events = []
    retried: dict[str, object] = {}

    class _RetryTriggered(Exception):
        pass

    monkeypatch.setattr(earnings, "get_settings", lambda: settings)
    monkeypatch.setattr(earnings, "notify_sync", events.append)

    def _run_async(awaitable):
        awaitable.close()
        raise RuntimeError("upstream API timeout")

    def _retry(*, exc: Exception, countdown: int):
        retried["exc"] = exc
        retried["countdown"] = countdown
        raise _RetryTriggered()

    monkeypatch.setattr(earnings, "run_async", _run_async)
    monkeypatch.setattr(earnings.refresh_earnings_cache, "retry", _retry)

    with pytest.raises(_RetryTriggered):
        earnings.refresh_earnings_cache.run()

    assert [event.event_type for event in events] == [
        EventType.EARNINGS_CACHE_REFRESH_STARTED,
        EventType.EARNINGS_CACHE_REFRESH_FAILED,
    ]
    assert events[1].severity == "error"
    assert events[1].payload == {
        "cache_key": earnings.EARNINGS_HASH_KEY,
        "symbols": "2",
        "error": "upstream API timeout",
        "retry_in_s": str(earnings._RETRY_COUNTDOWN_SECONDS),
    }
    assert "upstream API timeout" in events[1].message
    assert retried == {
        "exc": retried["exc"],
        "countdown": earnings._RETRY_COUNTDOWN_SECONDS,
    }
    assert isinstance(retried["exc"], RuntimeError)
    assert str(retried["exc"]) == "upstream API timeout"