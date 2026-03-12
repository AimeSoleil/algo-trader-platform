"""系统配置 — 从 .env 和 config.yaml 加载"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings


class DatabaseSettings(BaseSettings):
    timescale_url: str = "postgresql+asyncpg://trader:trader_dev@localhost:5432/algo_trader"
    postgres_url: str = "postgresql+asyncpg://trader:trader_dev@localhost:5433/algo_trader_biz"

class RedisSettings(BaseSettings):
    url: str = "redis://localhost:6379/0"

class RabbitMQSettings(BaseSettings):
    url: str = "amqp://trader:trader_dev@localhost:5672//"

class MinIOSettings(BaseSettings):
    endpoint: str = "localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    secure: bool = False

class LLMSettings(BaseSettings):
    provider: str = "openai"  # "openai" / "copilot"

    # ── OpenAI ──
    openai_api_key: str = ""
    openai_model: str = "gpt-4o"
    openai_temperature: float = 0.1
    openai_max_tokens: int = 8192

    # ── Copilot SDK ──
    copilot_cli_path: str = "copilot"
    copilot_github_token: str = ""
    copilot_model: str = "gpt-4o"  # model used inside Copilot session
    copilot_temperature: float = 0.1
    copilot_max_tokens: int = 4096

    # ── Common ──
    cache_enabled: bool = True
    cache_ttl: int = 3600  # seconds
    skill_dir: str = ""  # path to skills directory (provider resolves default)

class TradingSettings(BaseSettings):
    timezone: str = "America/New_York"
    execution_interval: int = 300  # seconds (5 min)


class MarketHoursSettings(BaseSettings):
    start: str = "09:30"
    end: str = "16:00"


class IntradayHotStorageRetention(BaseSettings):
    stock_1min: int = 90
    option_5min: int = 60


class IntradayArchiveRetention(BaseSettings):
    stock_1min: int = 365
    option_5min: int = 180


class DataServiceIntradaySettings(BaseSettings):
    stock_capture_interval_seconds: int = 60
    option_capture_interval_seconds: int = 300
    capture_every_minutes: int = 5
    max_option_expiries: int = 3
    hot_storage_retention_days: IntradayHotStorageRetention = Field(default_factory=IntradayHotStorageRetention)
    archive_retention_days: IntradayArchiveRetention = Field(default_factory=IntradayArchiveRetention)


class DataProviderSettings(BaseSettings):
    """Data fetcher provider selection."""
    stock: str = "yfinance"
    options: str = "yfinance"
    options_historical: str = "none"


class DataServiceSettings(BaseSettings):
    intraday_enabled: bool = False
    providers: DataProviderSettings = Field(default_factory=DataProviderSettings)
    market_hours: MarketHoursSettings = Field(default_factory=MarketHoursSettings)
    intraday: DataServiceIntradaySettings = Field(default_factory=DataServiceIntradaySettings)

class OptionStrategySettings(BaseSettings):
    lookback_days: int = 252          # iv_percentile 滚动窗口（交易日）
    high_quantile: float = 0.7        # iv_percentile >= 70 → "high" 波动率区间
    low_quantile: float = 0.3         # iv_percentile <= 30 → "low" 波动率区间

class ScheduleSettings(BaseSettings):
    """盘后批处理流水线调度时间"""
    blueprint_load_time: str = "09:20"
    market_open: str = "09:30"
    market_close: str = "16:00"
    batch_flush_time: str = "16:30"
    backfill_time: str = "16:35"
    signal_compute_time: str = "17:00"
    blueprint_generate_time: str = "17:10"

class LoggingSettings(BaseSettings):
    level: str = "INFO"
    format: str = "json"  # "json" / "console"

class Settings(BaseSettings):
    """Root settings — assembles all sub-settings"""
    model_config = {"env_prefix": "", "env_nested_delimiter": "__"}

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    rabbitmq: RabbitMQSettings = Field(default_factory=RabbitMQSettings)
    minio: MinIOSettings = Field(default_factory=MinIOSettings)
    llm: LLMSettings = Field(default_factory=LLMSettings)
    trading: TradingSettings = Field(default_factory=TradingSettings)
    data_service: DataServiceSettings = Field(default_factory=DataServiceSettings)
    option_strategy: OptionStrategySettings = Field(default_factory=OptionStrategySettings)
    schedule: ScheduleSettings = Field(default_factory=ScheduleSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)

    # Watchlist — symbols to track
    watchlist: list[str] = Field(default_factory=lambda: ["AAPL", "MSFT", "NVDA", "TSLA", "SPY", "QQQ"])

    @classmethod
    def from_yaml(cls, yaml_path: str | Path | None = None) -> Settings:
        """从 config.yaml 加载，环境变量覆盖"""
        if yaml_path is None:
            # Search upward for config/config.yaml
            yaml_path = Path(__file__).resolve().parents[2] / "config" / "config.yaml"

        yaml_path = Path(yaml_path)
        yaml_data = {}
        if yaml_path.exists():
            with open(yaml_path) as f:
                yaml_data = yaml.safe_load(f) or {}

        return cls(**yaml_data)


@lru_cache
def get_settings() -> Settings:
    """获取全局配置（带缓存）"""
    return Settings.from_yaml()
