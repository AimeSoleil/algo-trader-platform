from __future__ import annotations

from pathlib import Path

from shared.config.settings import Settings


def test_env_overrides_yaml_for_infra_urls(tmp_path: Path, monkeypatch) -> None:
    yaml_path = tmp_path / "config.yaml"
    yaml_path.write_text(
        """
infra:
  database:
    timescale_url: postgresql+asyncpg://trader:trader_dev@localhost:5432/algo_trader
    postgres_url: postgresql+asyncpg://trader:trader_dev@localhost:5433/algo_trader_biz
  redis:
    url: redis://localhost:6379/0
  rabbitmq:
    url: amqp://trader:trader_dev@localhost:5672//
""".strip(),
        encoding="utf-8",
    )

    monkeypatch.setenv(
        "INFRA__DATABASE__TIMESCALE_URL",
        "postgresql+asyncpg://trader:trader_dev@algo_timescaledb:5432/algo_trader",
    )
    monkeypatch.setenv(
        "INFRA__DATABASE__POSTGRES_URL",
        "postgresql+asyncpg://trader:trader_dev@algo_postgres:5432/algo_trader_biz",
    )
    monkeypatch.setenv("INFRA__REDIS__URL", "redis://algo_redis:6379/0")
    monkeypatch.setenv("INFRA__RABBITMQ__URL", "amqp://trader:trader_dev@algo_rabbitmq:5672//")

    settings = Settings.from_yaml(yaml_path)

    assert settings.infra.database.timescale_url == "postgresql+asyncpg://trader:trader_dev@algo_timescaledb:5432/algo_trader"
    assert settings.infra.database.postgres_url == "postgresql+asyncpg://trader:trader_dev@algo_postgres:5432/algo_trader_biz"
    assert settings.infra.redis.url == "redis://algo_redis:6379/0"
    assert settings.infra.rabbitmq.url == "amqp://trader:trader_dev@algo_rabbitmq:5672//"