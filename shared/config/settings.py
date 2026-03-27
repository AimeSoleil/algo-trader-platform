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
    cluster_enabled: bool = False        # True → use RedisCluster instead of standalone
    cluster_nodes: list[dict] = Field(   # seed nodes for cluster mode
        default_factory=list,
        description='e.g. [{"host": "redis-node-1", "port": 6380}]',
    )
    lock_ttl_default: int = 300          # default distributed lock TTL (seconds)
    lock_retry_interval: float = 0.5     # retry interval when lock is contended

class RabbitMQSettings(BaseSettings):
    url: str = "amqp://trader:trader_dev@localhost:5672//"

class MinIOSettings(BaseSettings):
    endpoint: str = "localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    secure: bool = False


class OpenAILLMSettings(BaseSettings):
    api_key: str = ""
    model: str = "gpt-4o"
    temperature: float = 0.1
    max_tokens: int = 8192
    request_timeout_seconds: int = 600


class CopilotLLMSettings(BaseSettings):
    cli_path: str = "copilot"
    github_token: str = ""
    model: str = "gpt-4o"  # model used inside Copilot session
    reasoning_effort: str = "medium"  # low / medium / high / xhigh
    request_timeout_seconds: int = 600

class LLMSettings(BaseSettings):
    provider: str = "openai"  # "openai" / "copilot"

    openai: OpenAILLMSettings = Field(default_factory=OpenAILLMSettings)
    copilot: CopilotLLMSettings = Field(default_factory=CopilotLLMSettings)

    # ── Common ──
    cache_enabled: bool = True
    cache_ttl: int = 3600  # seconds
    skill_dir: str = ""  # path to skills directory (provider resolves default)

    # ── Chunking ──
    chunk_size: int = 5                    # max symbols per LLM chunk (excl. benchmarks)
    max_concurrent_chunks: int = 3         # max parallel LLM calls
    benchmark_symbols: list[str] = Field(
        default_factory=lambda: ["SPY", "QQQ"],
        description="Symbols injected into every chunk for market context",
    )

    # ── Agentic pipeline ──
    agentic_mode: bool = False                  # True → multi-agent pipeline, False → chunk-based one-shot
    max_critic_revisions: int = 2               # max Synthesizer↔Critic revision rounds

    # ── Retry / Resilience ──
    max_retries: int = 3
    backoff_base_seconds: float = 2.0          # exponential backoff base
    backoff_max_seconds: float = 60.0          # cap on backoff delay
    circuit_breaker_threshold: int = 5         # consecutive failures before circuit opens
    circuit_breaker_cooldown_seconds: int = 60 # seconds before circuit half-opens

class TradingSettings(BaseSettings):
    timezone: str = "America/New_York"
    execution_interval: int = 300  # seconds (5 min)


class StopLossSettings(BaseSettings):
    enabled: bool = True
    check_interval_seconds: int = 60
    portfolio_loss_limit: float = 2000.0
    position_loss_limit: float = 500.0
    cooldown_seconds: int = 60


class PaperBrokerSettings(BaseSettings):
    initial_cash: float = 100_000.0


class FutuBrokerSettings(BaseSettings):
    host: str = "127.0.0.1"
    port: int = 11111
    trader_id: str = ""
    trd_env: str = "SIMULATE"    # SIMULATE | REAL
    market: str = "US"


class BrokerSettings(BaseSettings):
    type: str = "paper"          # paper | futu
    paper: PaperBrokerSettings = Field(default_factory=PaperBrokerSettings)
    futu: FutuBrokerSettings = Field(default_factory=FutuBrokerSettings)


class RiskSettings(BaseSettings):
    stop_loss: StopLossSettings = Field(default_factory=StopLossSettings)


class DataQualitySettings(BaseSettings):
    """数据质量评分权重 & 执行门控阈值 — 对应 config.yaml 中 data_quality 段"""
    # 评分权重（三项之和应 = 1.0）
    weight_stock: float = 0.5          # 股票覆盖率权重
    weight_option: float = 0.3         # 期权覆盖率权重
    weight_degradation: float = 0.2    # 无降级指标奖励权重
    # 评分参考值（满分对应行数）
    stock_full_bars: int = 260         # ≈ 1 年交易日
    option_full_rows: int = 200
    # 执行门控
    skip_threshold: float = 0.3        # < 此值 → 跳过执行
    reduce_threshold: float = 0.7      # < 此值 → 缩减仓位
    reduce_factor: float = 0.5         # 缩减比例（0.5 = 减半）


class MarketHoursSettings(BaseSettings):
    start: str = "09:30"
    end: str = "16:00"


class IntradayRetentionSettings(BaseSettings):
    stock_1min: int = 90
    option_5min: int = 60


class DataServiceIntradaySettings(BaseSettings):
    capture_every_minutes: int = 5
    retention_days: IntradayRetentionSettings = Field(default_factory=IntradayRetentionSettings)


class DataProviderSettings(BaseSettings):
    """Data fetcher provider selection."""
    stock: str = "yfinance"
    options: str = "yfinance"


class OptionFetchSettings(BaseSettings):
    """Option-chain-specific fetch parameters."""
    max_days_to_expiry: int = 730


class ResilienceSettings(BaseSettings):
    """Provider-agnostic retry / rate-limit / concurrency settings."""
    max_retries: int = 3
    backoff_base_seconds: float = 1.0
    rate_limit_per_call_seconds: float = 0.5
    rate_limit_per_symbol_seconds: float = 1.5
    concurrent_symbols: int = 3


class DataServiceSettings(BaseSettings):
    providers: DataProviderSettings = Field(default_factory=DataProviderSettings)
    market_hours: MarketHoursSettings = Field(default_factory=MarketHoursSettings)
    intraday: DataServiceIntradaySettings = Field(default_factory=DataServiceIntradaySettings)
    options: OptionFetchSettings = Field(default_factory=OptionFetchSettings)
    resilience: ResilienceSettings = Field(default_factory=ResilienceSettings)

class OptionStrategySettings(BaseSettings):
    lookback_days: int = 252          # iv_percentile 滚动窗口（交易日）
    high_quantile: float = 0.7        # iv_percentile >= 70 → "high" 波动率区间
    low_quantile: float = 0.3         # iv_percentile <= 30 → "low" 波动率区间

class ScheduleSettings(BaseSettings):
    """盘后批处理流水线调度时间"""
    blueprint_load_time: str = "09:20"
    market_open: str = "09:30"
    market_close: str = "16:00"
    batch_flush_time: str = "18:30"
    backfill_time: str = "19:00"
    signal_compute_time: str = "19:30"
    blueprint_generate_time: str = "20:00"

class LoggingSettings(BaseSettings):
    level: str = "INFO"
    format: str = "json"  # "json" / "console"
    to_console: bool = True
    to_file: bool = False
    file_path: str = "logs/algo-trader.log"
    file_rotate_mode: str = "time"
    file_max_bytes: int = 104857600
    file_rotate: bool = True
    file_rotate_when: str = "midnight"
    file_rotate_interval: int = 1
    file_backup_count: int = 14
    file_rotate_utc: bool = False

class Settings(BaseSettings):
    """Root settings — assembles all sub-settings"""
    model_config = {"env_prefix": "", "env_nested_delimiter": "__"}

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    rabbitmq: RabbitMQSettings = Field(default_factory=RabbitMQSettings)
    minio: MinIOSettings = Field(default_factory=MinIOSettings)
    broker: BrokerSettings = Field(default_factory=BrokerSettings)
    llm: LLMSettings = Field(default_factory=LLMSettings)
    trading: TradingSettings = Field(default_factory=TradingSettings)
    risk: RiskSettings = Field(default_factory=RiskSettings)
    data_quality: DataQualitySettings = Field(default_factory=DataQualitySettings)
    data_service: DataServiceSettings = Field(default_factory=DataServiceSettings)
    option_strategy: OptionStrategySettings = Field(default_factory=OptionStrategySettings)
    schedule: ScheduleSettings = Field(default_factory=ScheduleSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)

    # Watchlist — symbols to track
    watchlist: list[str] = Field(default_factory=lambda: ["AAPL", "MSFT", "NVDA", "TSLA", "SPY", "QQQ"])

    # Cross-asset benchmarks for multi-factor signal model
    cross_asset_benchmarks: list[str] = Field(
        default_factory=lambda: ["SPY", "QQQ", "IWM", "TLT"],
        description="ETF benchmarks used for beta / correlation computation",
    )
    environment_symbols: list[str] = Field(
        default_factory=lambda: ["^VIX"],
        description="Market regime indicators (e.g. VIX) — not traded, used as environment context",
    )

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
