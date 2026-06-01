from __future__ import annotations

import pytest

from shared import async_bridge


def test_run_async_closes_resources_after_success(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    async def fake_cleanup() -> None:
        calls.append("cleanup")

    async def sample() -> int:
        calls.append("coro")
        return 42

    monkeypatch.setattr(async_bridge, "_close_async_resources", fake_cleanup)

    result = async_bridge.run_async(sample())

    assert result == 42
    assert calls == ["coro", "cleanup"]


def test_run_async_preserves_original_error_when_cleanup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    async def fake_cleanup() -> None:
        calls.append("cleanup")
        raise RuntimeError("cleanup failed")

    async def sample() -> None:
        calls.append("coro")
        raise ValueError("boom")

    monkeypatch.setattr(async_bridge, "_close_async_resources", fake_cleanup)

    with pytest.raises(ValueError, match="boom"):
        async_bridge.run_async(sample())

    assert calls == ["coro", "cleanup"]


@pytest.mark.asyncio
async def test_run_async_from_running_loop_still_cleans_up(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    async def fake_cleanup() -> None:
        calls.append("cleanup")

    async def sample() -> str:
        calls.append("coro")
        return "ok"

    monkeypatch.setattr(async_bridge, "_close_async_resources", fake_cleanup)

    result = async_bridge.run_async(sample())

    assert result == "ok"
    assert calls == ["coro", "cleanup"]