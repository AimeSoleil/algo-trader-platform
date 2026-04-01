"""系统配置 — 从 .env 和 config.yaml 加载"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, ClassVar

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource


def _default_yaml_path() -> Path:
    return Path(__file__).resolve().parents[2] / "config" / "config.yaml"


def _load_yaml_data(yaml_path: Path) -> dict[str, Any]:
    if not yaml_path.exists():
        return {}

    with yaml_path.open(encoding="utf-8") as file:
        return yaml.safe_load(file) or {}


class YamlSettingsSource(PydanticBaseSettingsSource):
    """Load settings from config.yaml as a low-priority fallback source."""

    def __init__(self, settings_cls: type[BaseSettings], yaml_path: Path) -> None:
        super().__init__(settings_cls)
        self.yaml_path = yaml_path

    def get_field_value(self, field: Any, field_name: str) -> tuple[Any, str, bool]:
        data = self()
        return data.get(field_name), field_name, False

    def __call__(self) -> dict[str, Any]:
        return _load_yaml_data(self.yaml_path)


# ── Infrastructure ───────────────────────────────────────────

class DatabaseSettings(BaseSettings):
    timescale_url: str = "postgresql+asyncpg://trader:trader_dev@localhost:5432/algo_trader"
    postgres_url: str = "postgresql+asyncpg://trader:trader_dev@localhost:5433/algo_trader_biz"

class RedisSettings(BaseSettings):
    url: str = "redis://localhost:6379/0"
    lock_ttl_default: int = 300
    lock_retry_interval: float = 0.5

class RabbitMQSettings(BaseSettings):
    url: str = "amqp://trader:trader_dev@localhost:5672//"

class InfraSettings(BaseSettings):
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    rabbitmq: RabbitMQSettings = Field(default_factory=RabbitMQSettings)


# ── Common — 跨服务共用 ──────────────────────────────────────

class MarketHoursSettings(BaseSettings):
    start: str = "09:30"
    end: str = "16:00"

class DataQualitySettings(BaseSettings):
    """数据质量评分权重 & 执行门控阈值"""
    weight_stock: float = 0.5
    weight_option: float = 0.3
    weight_degradation: float = 0.2
    stock_full_bars: int = 260
    option_full_rows: int = 200
    skip_threshold: float = 0.3
    reduce_threshold: float = 0.7
    reduce_factor: float = 0.5

class ScheduleSettings(BaseSettings):
    """盘后批处理流水线调度时间"""
    blueprint_load_time: str = "09:20"
    batch_flush_time: str = "18:30"
    backfill_time: str = "19:00"
    signal_compute_time: str = "19:30"
    blueprint_generate_time: str = "20:00"

class LoggingSettings(BaseSettings):
    level: str = "INFO"
    format: str = "json"
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

class CommonSettings(BaseSettings):
    """跨服务共用配置"""
    timezone: str = "America/New_York"
    market_hours: MarketHoursSettings = Field(default_factory=MarketHoursSettings)
    watchlist: list[str] = Field(default_factory=lambda: ["AAPL", "MSFT", "NVDA", "TSLA", "SPY", "QQQ"])
    data_quality: DataQualitySettings = Field(default_factory=DataQualitySettings)
    schedule: ScheduleSettings = Field(default_factory=ScheduleSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)


# ── Data Service ─────────────────────────────────────────────

class DataProviderSettings(BaseSettings):
    """Data fetcher provider selection."""
    stock: str = "yfinance"
    options: str = "yfinance"

class IntradayRetentionSettings(BaseSettings):
    stock_1min: int = 90
    option_5min: int = 60

class DataServiceIntradaySettings(BaseSettings):
    capture_every_minutes: int = 5
    retention_days: IntradayRetentionSettings = Field(default_factory=IntradayRetentionSettings)

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

class OptionCleaningFilterSettings(BaseSettings):
    """Stage 1: 数据清洁过滤 — 仅剔除坏数据，不影响 IV smile / skew."""
    max_iv: float = 5.0

class OptionTradeableMarkingSettings(BaseSettings):
    """Stage 2: 可交易标记 — 不剔除合约，只设置 is_tradeable 标志."""
    min_volume: int = 10
    min_open_interest: int = 50
    max_relative_spread: float = 0.10
    min_strike_ratio: float = 0.70
    max_strike_ratio: float = 1.30
    min_delta_threshold: float = 0.01
    max_stale_trade_days: int = 7

class OptionDataFilterSettings(BaseSettings):
    """data_service.filters.options — 组合清洁 + 可交易标记."""
    cleaning: OptionCleaningFilterSettings = Field(default_factory=OptionCleaningFilterSettings)
    tradeable_marking: OptionTradeableMarkingSettings = Field(default_factory=OptionTradeableMarkingSettings)

class DataServiceFilterSettings(BaseSettings):
    """data_service.filters — 按资产类型组织的过滤器配置."""
    options: OptionDataFilterSettings = Field(default_factory=OptionDataFilterSettings)

class DataServiceSettings(BaseSettings):
    providers: DataProviderSettings = Field(default_factory=DataProviderSettings)
    intraday: DataServiceIntradaySettings = Field(default_factory=DataServiceIntradaySettings)
    options: OptionFetchSettings = Field(default_factory=OptionFetchSettings)
    resilience: ResilienceSettings = Field(default_factory=ResilienceSettings)
    filters: DataServiceFilterSettings = Field(default_factory=DataServiceFilterSettings)


# ── Signal Service ───────────────────────────────────────────

class SignalOptionTradingFilterSettings(BaseSettings):
    """Stage 3: 交易级过滤 — 仅用于策略类指标，不影响分析类指标."""
    min_volume: int = 10
    min_open_interest: int = 100
    max_relative_spread: float = 0.08
    min_delta: float = 0.05
    max_delta: float = 0.95
    min_dte: int = 7
    max_dte: int = 180

class SignalOptionFilterSettings(BaseSettings):
    """signal_service.filters.options — 期权交易级过滤."""
    trading: SignalOptionTradingFilterSettings = Field(default_factory=SignalOptionTradingFilterSettings)

class SignalServiceFilterSettings(BaseSettings):
    """signal_service.filters — 按资产类型组织."""
    options: SignalOptionFilterSettings = Field(default_factory=SignalOptionFilterSettings)

class OptionStrategySettings(BaseSettings):
    lookback_days: int = 252
    high_quantile: float = 0.7
    low_quantile: float = 0.3

class SignalServiceSettings(BaseSettings):
    """signal_service 顶级配置."""
    iv_lookback_days: int = 252
    option_strategy: OptionStrategySettings = Field(default_factory=OptionStrategySettings)
    cross_asset_benchmarks: list[str] = Field(
        default_factory=lambda: ["SPY", "QQQ", "IWM", "TLT"],
        description="ETF benchmarks used for beta / correlation computation",
    )
    filters: SignalServiceFilterSettings = Field(default_factory=SignalServiceFilterSettings)


# ── Analysis Service ─────────────────────────────────────────

class OpenAILLMSettings(BaseSettings):
    api_key: str = ""
    model: str = "gpt-4o"
    temperature: float = 0.1
    max_tokens: int = 8192
    request_timeout_seconds: int = 600

class CopilotLLMSettings(BaseSettings):
    cli_path: str = "copilot"
    github_token: str = ""
    model: str = "gpt-4o"
    reasoning_effort: str = "medium"
    request_timeout_seconds: int = 600

class LLMSettings(BaseSettings):
    provider: str = "openai"

    openai: OpenAILLMSettings = Field(default_factory=OpenAILLMSettings)
    copilot: CopilotLLMSettings = Field(default_factory=CopilotLLMSettings)

    # ── Common ──
    cache_enabled: bool = True
    cache_ttl: int = 3600
    skill_dir: str = ""

    # ── Chunking ──
    chunk_size: int = 5
    max_concurrent_chunks: int = 3
    benchmark_symbols: list[str] = Field(
        default_factory=lambda: ["SPY", "QQQ"],
        description="Symbols injected into every chunk for market context",
    )

    # ── Agentic pipeline ──
    agentic_mode: bool = False
    max_critic_revisions: int = 2

    # ── Retry / Resilience ──
    max_retries: int = 3
    backoff_base_seconds: float = 2.0
    backoff_max_seconds: float = 60.0
    circuit_breaker_threshold: int = 5
    circuit_breaker_cooldown_seconds: int = 60

class AnalysisServiceSettings(BaseSettings):
    """analysis_service 顶级配置."""
    llm: LLMSettings = Field(default_factory=LLMSettings)


# ── Trade Service ────────────────────────────────────────────

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
    trd_env: str = "SIMULATE"
    market: str = "US"

class BrokerSettings(BaseSettings):
    type: str = "paper"
    paper: PaperBrokerSettings = Field(default_factory=PaperBrokerSettings)
    futu: FutuBrokerSettings = Field(default_factory=FutuBrokerSettings)

class RiskSettings(BaseSettings):
    stop_loss: StopLossSettings = Field(default_factory=StopLossSettings)

class TradeServiceSettings(BaseSettings):
    """trade_service 顶级配置."""
    execution_interval: int = 300
    broker: BrokerSettings = Field(default_factory=BrokerSettings)
    risk: RiskSettings = Field(default_factory=RiskSettings)


# ── Root Settings ────────────────────────────────────────────

class Settings(BaseSettings):
    """Root settings — assembles all sub-settings"""
    model_config = {"env_prefix": "", "env_nested_delimiter": "__"}
    _yaml_path: ClassVar[Path] = _default_yaml_path()

    common: CommonSettings = Field(default_factory=CommonSettings)
    infra: InfraSettings = Field(default_factory=InfraSettings)
    data_service: DataServiceSettings = Field(default_factory=DataServiceSettings)
    signal_service: SignalServiceSettings = Field(default_factory=SignalServiceSettings)
    analysis_service: AnalysisServiceSettings = Field(default_factory=AnalysisServiceSettings)
    trade_service: TradeServiceSettings = Field(default_factory=TradeServiceSettings)

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        yaml_settings = YamlSettingsSource(settings_cls, cls._yaml_path)
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            yaml_settings,
            file_secret_settings,
        )

    @classmethod
    def from_yaml(cls, yaml_path: str | Path | None = None) -> Settings:
        """从 config.yaml 加载，环境变量覆盖"""
        cls._yaml_path = Path(yaml_path) if yaml_path is not None else _default_yaml_path()
        return cls()


@lru_cache
def get_settings() -> Settings:
    """获取全局配置（带缓存）"""
    return Settings.from_yaml()
