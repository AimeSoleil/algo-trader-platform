"""系统配置 — 从 .env 和 config.yaml 加载"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, ClassVar

import yaml
from pydantic import Field, field_validator
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

class MinioSettings(BaseSettings):
    endpoint: str = "localhost:9000"
    access_key: str = "minioadmin"
    secret_key: str = "minioadmin"
    secure: bool = False

class PrometheusSettings(BaseSettings):
    url: str = "http://localhost:9090"
    scrape_interval: int = 15              # seconds

class GrafanaSettings(BaseSettings):
    url: str = "http://localhost:3300"

class InfraSettings(BaseSettings):
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    redis: RedisSettings = Field(default_factory=RedisSettings)
    rabbitmq: RabbitMQSettings = Field(default_factory=RabbitMQSettings)
    minio: MinioSettings = Field(default_factory=MinioSettings)
    prometheus: PrometheusSettings = Field(default_factory=PrometheusSettings)
    grafana: GrafanaSettings = Field(default_factory=GrafanaSettings)


# ── Common — Celery / Beat / Flower (shared across all workers) ──────────────

class CelerySettings(BaseSettings):
    """Celery runtime settings — applied to all workers."""
    prefetch_multiplier: int = 4           # 预取任务数（analysis worker 单独覆盖为 1）
    max_memory_per_child: int = 500_000    # KB, 500 MB — exceeded → auto-restart
    task_acks_late: bool = True            # ack after completion, not on delivery
    task_track_started: bool = True        # track STARTED state
    task_soft_time_limit: int = 600        # seconds — SoftTimeLimitExceeded (graceful)
    task_time_limit: int = 900             # seconds — SIGKILL (hard ceiling)
    task_reject_on_worker_lost: bool = True  # re-queue if worker dies mid-task
    concurrency: int = 0                   # 0 = Celery default (CPU cores)

class BeatSettings(BaseSettings):
    redbeat_lock_timeout: int = 900        # seconds before a dead beat loses the lock

class FlowerSettings(BaseSettings):
    port: int = 5555
    basic_auth_user: str = "admin"
    basic_auth_password: str = "changeme"
    persistent: bool = True


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

class LoggingSettings(BaseSettings):
    level: str = "INFO"
    lib_level: str = "WARNING"
    format: str = "json"
    to_console: bool = True
    to_file: bool = True
    file_path: str = "logs/algo-trader.log"
    file_rotate_mode: str = "time"
    file_max_bytes: int = 104857600
    file_rotate: bool = True
    file_rotate_when: str = "midnight"
    file_rotate_interval: int = 1
    file_backup_count: int = 14
    file_rotate_utc: bool = False

class WatchlistSettings(BaseSettings):
    """Structured watchlist with three symbol categories.

    ``for_trade``  — symbols we collect data for AND generate trading
                     blueprints for.  If a symbol also appears in
                     *for_trade_benchmark* it is still treated as a
                     trade target (deduplication during analysis).
    ``for_trade_benchmark`` — injected into every LLM analysis chunk
                     for market context.  Plans are NOT generated for
                     these unless they also appear in *for_trade*.
    ``for_signal_benchmark`` — used by the signal service for beta /
                     correlation computation.  Data is collected but
                     no blueprints are generated.

    Use ``.all`` to get the deduplicated union across all three lists
    (for data collection, backfill, and maintenance).
    """
    for_trade: list[str] = Field(
        default_factory=lambda: [
            # ETFs
            "SPY", "QQQ", "IWM",
            # Tech / Growth
            "AAPL", "MSFT", "NVDA", "TSLA", "AMZN",
            "META", "GOOGL", "AMD", "NFLX", "AVGO",
            # AI / Defense
            "PLTR",
            # Financials
            "JPM", "GS",
            # Payments · Healthcare · Pharma · Energy
            "V", "UNH", "LLY", "XOM",
            # Consumer · Industrials · Crypto
            "COST", "CAT", "UBER", "COIN", "IBIT",
        ],
    )
    for_trade_benchmark: list[str] = Field(
        default_factory=lambda: ["SPY", "QQQ", "IWM", "DIA"],
    )
    for_signal_benchmark: list[str] = Field(
        default_factory=lambda: [
            "SPY", "QQQ", "IWM", "TLT", "^VIX",
            "GLD", "HYG", "XLE", "IBIT",
        ],
    )

    @property
    def all(self) -> list[str]:
        """Deduplicated union of all three lists (order-preserving)."""
        return list(dict.fromkeys(
            self.for_trade + self.for_trade_benchmark + self.for_signal_benchmark
        ))


class NotifierBackendConfig(BaseSettings):
    """Single notification backend entry."""
    type: str = "discord"
    enabled: bool = True
    webhook_url: str = ""
    timeout: float = 10.0

class NotifierSettings(BaseSettings):
    """Notification system — async, fire-and-forget."""
    enabled: bool = False
    backends: list[NotifierBackendConfig] = Field(default_factory=list)
    daily_report_time: str = "16:30"   # ET — independent beat task

    @field_validator("backends", mode="before")
    @classmethod
    def _normalize_backends(cls, value: Any) -> Any:
        """Accept both list input and env-nested dict input.

        Pydantic env parsing with nested keys like
        ``COMMON__NOTIFIER__BACKENDS__0__TYPE=discord`` may produce:
        ``{"0": {"type": "discord", ...}}``.
        Convert that shape into a list ordered by numeric index.
        """
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            if all(str(k).isdigit() for k in value.keys()):
                return [value[k] for k in sorted(value.keys(), key=lambda k: int(str(k)))]
            return list(value.values())
        return value


class CommonSettings(BaseSettings):
    """跨服务共用配置"""
    timezone: str = "America/New_York"
    market_hours: MarketHoursSettings = Field(default_factory=MarketHoursSettings)
    watchlist: WatchlistSettings = Field(default_factory=WatchlistSettings)
    data_quality: DataQualitySettings = Field(default_factory=DataQualitySettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    celery: CelerySettings = Field(default_factory=CelerySettings)
    beat: BeatSettings = Field(default_factory=BeatSettings)
    flower: FlowerSettings = Field(default_factory=FlowerSettings)
    notifier: NotifierSettings = Field(default_factory=NotifierSettings)


# ── Data Service ─────────────────────────────────────────────

class DataProviderSettings(BaseSettings):
    """Data fetcher provider selection."""
    stock: str = "yfinance"
    options: str = "yfinance"

class IntradayRetentionSettings(BaseSettings):
    stock_1min: int = 90
    option_5min: int = 60

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
    max_days_to_expiry: int = 730      # 跳过超过此天数的远期到期日（~2年）

class OptionTradeableMarkingSettings(BaseSettings):
    """Stage 2: 可交易标记 — 不剔除合约，只设置 is_tradeable 标志."""
    min_volume: int = 10
    min_open_interest: int = 50
    max_relative_spread: float = 0.10
    min_strike_ratio: float = 0.75
    max_strike_ratio: float = 1.25
    min_delta_threshold: float = 0.01
    max_stale_trade_days: int = 5

class OptionDataFilterSettings(BaseSettings):
    """data_service.filters.options — 组合清洁 + 可交易标记."""
    cleaning: OptionCleaningFilterSettings = Field(default_factory=OptionCleaningFilterSettings)
    tradeable_marking: OptionTradeableMarkingSettings = Field(default_factory=OptionTradeableMarkingSettings)

class DataServiceFilterSettings(BaseSettings):
    """data_service.filters — 按资产类型组织的过滤器配置."""
    options: OptionDataFilterSettings = Field(default_factory=OptionDataFilterSettings)


# ── Data Service Worker ───────────────────────────────────────

class DataWorkerScheduleSettings(BaseSettings):
    """data_service.worker.schedule — 盘中/盘后调度时间."""
    options_capture_every_minutes: int = 5      # 盘中期权链采集间隔（分钟）
    post_market_time: str = "17:00"             # 统一盘后流水线（options 聚合 + stock 采集 + 下游）
    refresh_earnings_time: str = "16:50"        # 刷新 earnings cache（pipeline 前）

class DataPipelineSettings(BaseSettings):
    """data_service.worker.pipeline — 流水线 stop-after 门控.

    Valid values (ordered):
      compute_daily_signals → generate_daily_blueprint

    Backfill runs as fire-and-forget and is not gated by stop_after.
    """
    chunk_size: int = 5
    stop_after: str = "generate_daily_blueprint"
    coordination_timeout_minutes: int = 60     # 两条流水线协调超时（分钟）

class DataWorkerSettings(BaseSettings):
    """data_service.worker — 数据服务 worker 配置."""
    schedule: DataWorkerScheduleSettings = Field(default_factory=DataWorkerScheduleSettings)
    pipeline: DataPipelineSettings = Field(default_factory=DataPipelineSettings)


class DataServiceSettings(BaseSettings):
    providers: DataProviderSettings = Field(default_factory=DataProviderSettings)
    retention_days: IntradayRetentionSettings = Field(default_factory=IntradayRetentionSettings)
    resilience: ResilienceSettings = Field(default_factory=ResilienceSettings)
    filters: DataServiceFilterSettings = Field(default_factory=DataServiceFilterSettings)
    worker: DataWorkerSettings = Field(default_factory=DataWorkerSettings)

# ── Signal Service ───────────────────────────────────────────

class SignalOptionTradingFilterSettings(BaseSettings):
    """Stage 3: 交易级过滤 — 仅用于策略类指标，不影响分析类指标."""
    min_volume: int = 25
    min_open_interest: int = 100
    max_relative_spread: float = 0.08
    min_delta: float = 0.08
    max_delta: float = 0.90
    min_dte: int = 7
    max_dte: int = 120

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
    filters: SignalServiceFilterSettings = Field(default_factory=SignalServiceFilterSettings)


# ── Analysis Service ─────────────────────────────────────────

class OpenAILLMSettings(BaseSettings):
    api_key: str = ""
    model: str = "claude-opus-4.6"
    temperature: float = 0.1
    max_tokens: int = 16384
    request_timeout_seconds: int = 600

class CopilotLLMSettings(BaseSettings):
    cli_path: str = "copilot"
    github_token: str = ""
    model: str = "claude-opus-4.6"
    reasoning_effort: str = "high"
    request_timeout_seconds: int = 600

class AgentModelsSettings(BaseSettings):
    """Per-agent model overrides. ``null`` / empty → use provider default."""
    trend: str = ""
    volatility: str = ""
    flow: str = ""
    chain: str = ""
    spread: str = ""
    cross_asset: str = ""
    synthesizer: str = ""
    critic: str = ""

class LLMSettings(BaseSettings):
    provider: str = "copilot"

    openai: OpenAILLMSettings = Field(default_factory=OpenAILLMSettings)
    copilot: CopilotLLMSettings = Field(default_factory=CopilotLLMSettings)

    # ── Per-agent model overrides ──
    agent_models_override: AgentModelsSettings = Field(default_factory=AgentModelsSettings)

    # ── Common ──
    cache_enabled: bool = True
    cache_ttl: int = 3600
    skill_dir: str = ""

    # ── Orchestrator — symbol chunking for context window management ──
    orchestrator_chunk_size: int = 9
    orchestrator_max_parallel: int = 3

    # ── Critic revision ──
    max_critic_revisions: int = 1

    # ── Retry / Resilience ──
    max_retries: int = 3
    backoff_base_seconds: float = 2.0
    backoff_max_seconds: float = 60.0

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
    trade_start_time: str = "09:20"        # 盘前加载蓝图 + 启动执行 tick
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
