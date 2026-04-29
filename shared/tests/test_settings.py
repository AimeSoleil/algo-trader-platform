from __future__ import annotations

from pathlib import Path

from shared.config.settings import Settings


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