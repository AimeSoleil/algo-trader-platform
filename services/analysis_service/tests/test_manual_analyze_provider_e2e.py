from __future__ import annotations

from datetime import date

import pytest

from shared.models.blueprint import LLMTradingBlueprint, OptionLeg, SymbolPlan
from shared.models.signal import (
    CrossAssetIndicators,
    DataQuality,
    OptionIndicators,
    SignalFeatures,
    StockIndicators,
)

import services.analysis_service.app.tasks.analyze as analyze_task


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeSession:
    def __init__(self, calls: list[tuple[str, dict | None]]):
        self._calls = calls

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def execute(self, query, params=None):
        sql = str(query)
        self._calls.append((sql, params))
        if "SELECT features_json FROM signal_features" in sql:
            return _FakeResult([({"mock": "signal"},)])
        return _FakeResult([])


class _FakeTask:
    def __init__(self):
        self.updates: list[tuple[str, dict]] = []

    def update_state(self, *, state: str, meta: dict):
        self.updates.append((state, meta))


def _make_signal(symbol: str = "AAPL") -> SignalFeatures:
    return SignalFeatures(
        symbol=symbol,
        date="2026-03-24",
        computed_at="2026-03-24T20:00:00",
        close_price=185.0,
        daily_return=0.01,
        volume=5_000_000,
        volatility_regime="normal",
        stock_indicators=StockIndicators(),
        option_indicators=OptionIndicators(),
        cross_asset_indicators=CrossAssetIndicators(),
        data_quality=DataQuality(),
    )


def _make_blueprint(provider: str) -> LLMTradingBlueprint:
    return LLMTradingBlueprint(
        trading_date="2026-03-24",
        generated_at="2026-03-24T20:00:00",
        market_regime="neutral",
        model_provider=provider,
        model_version="unit-test-model",
        symbol_plans=[
            SymbolPlan(
                underlying="AAPL",
                strategy_type="single_leg",
                direction="bullish",
                legs=[OptionLeg(expiry="2026-04-17", strike=150, option_type="call", side="buy")],
                max_loss_per_trade=500,
            )
        ],
    )


@pytest.mark.asyncio
async def test_manual_analyze_provider_override_persists_to_db(monkeypatch):
    db_calls: list[tuple[str, dict | None]] = []
    task = _FakeTask()

    monkeypatch.setattr(analyze_task, "get_postgres_session", lambda: _FakeSession(db_calls))
    monkeypatch.setattr(analyze_task, "_parse_signal_features", lambda _raw: _make_signal())

    async def _fake_run_blueprint_pipeline(signal_features, td, progress_cb=None, llm_provider=None):
        assert llm_provider == "closeai"
        assert td == date(2026, 3, 24)
        assert len(signal_features) == 1
        return _make_blueprint(provider=llm_provider)

    monkeypatch.setattr(analyze_task, "_run_blueprint_pipeline", _fake_run_blueprint_pipeline)

    async def _fake_invalidate_blueprint_cache(_trading_date):
        return None

    monkeypatch.setattr(
        "services.analysis_service.app.cache.invalidate_blueprint_cache",
        _fake_invalidate_blueprint_cache,
    )

    result = await analyze_task._manual_analyze_async(
        task,
        symbols=["AAPL"],
        trading_date_str="2026-03-24",
        llm_provider="closeai",
    )

    insert_calls = [
        (sql, params) for sql, params in db_calls
        if "INSERT INTO llm_trading_blueprint" in sql
    ]
    assert len(insert_calls) == 1

    _, insert_params = insert_calls[0]
    assert insert_params is not None
    assert insert_params["model_provider"] == "closeai"
    assert result["provider"] == "closeai"
