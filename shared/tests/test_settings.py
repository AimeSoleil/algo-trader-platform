from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from shared.config.settings import Settings, SignalOptionLegLiquidityFloorSettings, SignalOptionTradingFilterSettings


def test_env_overrides_yaml_for_infra_urls(tmp_path: Path, monkeypatch) -> None:
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text(
        """
infra:
  database:
    timescale_user: trader
    timescale_password: trader_dev
    timescale_host: localhost
    timescale_port: 5432
    timescale_db: algo_trader
    postgres_user: trader
    postgres_password: trader_dev
    postgres_host: localhost
    postgres_port: 5433
    postgres_db: algo_trader_biz
  redis:
    password: redis_trader_dev_secure
    host: localhost
    port: 6379
    db: 0
  rabbitmq:
    user: trader
    password: trader_dev
    host: localhost
    port: 5672
    vhost: /
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setenv("INFRA__DATABASE__TIMESCALE_HOST", "algo_timescaledb")
    monkeypatch.setenv("INFRA__DATABASE__TIMESCALE_PASSWORD", "docker_timescale_pw")
    monkeypatch.setenv("INFRA__DATABASE__POSTGRES_HOST", "algo_postgres")
    monkeypatch.setenv("INFRA__DATABASE__POSTGRES_PORT", "5432")
    monkeypatch.setenv("INFRA__DATABASE__POSTGRES_PASSWORD", "docker_postgres_pw")
    monkeypatch.setenv("INFRA__REDIS__HOST", "algo_redis")
    monkeypatch.setenv("INFRA__REDIS__PASSWORD", "redis_docker_pw")
    monkeypatch.setenv("INFRA__RABBITMQ__HOST", "algo_rabbitmq")
    monkeypatch.setenv("INFRA__RABBITMQ__PASSWORD", "rabbitmq_docker_pw")

    settings = Settings.from_yaml(yaml_path)

    assert settings.infra.database.timescale_url == "postgresql+asyncpg://trader:docker_timescale_pw@algo_timescaledb:5432/algo_trader"
    assert settings.infra.database.postgres_url == "postgresql+asyncpg://trader:docker_postgres_pw@algo_postgres:5432/algo_trader_biz"
    assert settings.infra.redis.url == "redis://:redis_docker_pw@algo_redis:6379/0"
    assert settings.infra.rabbitmq.url == "amqp://trader:rabbitmq_docker_pw@algo_rabbitmq:5672/"


def test_precision_first_strategy_scope_loads_from_yaml(tmp_path: Path) -> None:
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text(
        """
analysis_service:
  llm:
    precision_first:
      enabled: true
      allowed_strategy_types:
        - single_leg
        - vertical_spread
        - iron_condor
        - calendar_spread
""".strip(),
        encoding="utf-8",
    )

    settings = Settings.from_yaml(yaml_path)

    assert settings.analysis_service.llm.precision_first.enabled is True
    assert settings.analysis_service.llm.precision_first.allowed_strategy_types == ["single_leg", "vertical_spread", "iron_condor", "calendar_spread"]


def test_coarse_ranking_weights_load_from_yaml(tmp_path: Path) -> None:
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text(
        """
analysis_service:
  llm:
    coarse_ranking:
      weights:
        data_quality: 0.25
        option_coverage: 0.15
        liquidity: 0.35
        strategy_eligibility: 0.15
        earnings_buffer: 0.10
""".strip(),
        encoding="utf-8",
    )

    settings = Settings.from_yaml(yaml_path)

    assert settings.analysis_service.llm.coarse_ranking.weights.data_quality == 0.25
    assert settings.analysis_service.llm.coarse_ranking.weights.option_coverage == 0.15
    assert settings.analysis_service.llm.coarse_ranking.weights.liquidity == 0.35
    assert settings.analysis_service.llm.coarse_ranking.weights.strategy_eligibility == 0.15
    assert settings.analysis_service.llm.coarse_ranking.weights.earnings_buffer == 0.10


def test_max_output_plans_loads_from_yaml(tmp_path: Path) -> None:
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text(
        """
analysis_service:
  llm:
    max_output_plans: 12
""".strip(),
        encoding="utf-8",
    )

    settings = Settings.from_yaml(yaml_path)

    assert settings.analysis_service.llm.max_output_plans == 12


def test_watchlist_all_uses_data_signal_universe_plus_benchmarks(tmp_path: Path) -> None:
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text(
        """
common:
  watchlist:
    for_data_signal:
      - SPY
      - AAPL
      - MSFT
      - NVDA
    for_trade_benchmark:
      - SPY
      - MSFT
    for_signal_benchmark:
      - ^VIX
      - SPY
""".strip(),
        encoding="utf-8",
    )

    settings = Settings.from_yaml(yaml_path)

    assert settings.common.watchlist.all == ["SPY", "AAPL", "MSFT", "NVDA", "^VIX"]


def test_trade_benchmark_must_be_subset_of_data_signal(tmp_path: Path) -> None:
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text(
        """
common:
  watchlist:
    for_data_signal:
      - AAPL
      - MSFT
    for_trade_benchmark:
      - AAPL
      - NVDA
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValidationError, match="common.watchlist.for_trade_benchmark must be a subset"):
        Settings.from_yaml(yaml_path)


def test_signal_option_trading_filter_defaults_relax_weekly_spread_and_dte() -> None:
  defaults = SignalOptionTradingFilterSettings()

  assert defaults.max_relative_spread == 0.10
  assert defaults.min_dte == 5


def test_signal_option_leg_liquidity_floor_defaults_align_with_stage3_contract() -> None:
  defaults = SignalOptionLegLiquidityFloorSettings()

  assert defaults.profile_name == "stage3_aligned"
  assert defaults.min_leg_volume == 25
  assert defaults.min_exit_strike_open_interest == 100
  assert defaults.max_worst_leg_bid_ask_spread_ratio == 0.20


def test_signal_option_leg_liquidity_floor_loads_from_yaml(tmp_path: Path) -> None:
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text(
        """
signal_service:
  filters:
    options:
      leg_liquidity_floor:
        profile_name: custom_floor
        min_leg_volume: 30
        min_exit_strike_open_interest: 150
        max_worst_leg_bid_ask_spread_ratio: 0.18
""".strip(),
        encoding="utf-8",
    )

    settings = Settings.from_yaml(yaml_path)

    profile = settings.signal_service.filters.options.leg_liquidity_floor
    assert profile.profile_name == "custom_floor"
    assert profile.min_leg_volume == 30
    assert profile.min_exit_strike_open_interest == 150
    assert profile.max_worst_leg_bid_ask_spread_ratio == 0.18