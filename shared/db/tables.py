"""Central SQLAlchemy ORM definitions for TimescaleDB + PostgreSQL.

Design goals:
1) Keep schema stable for existing services and SQL statements.
2) Make time-series tables Timescale-hypertable friendly.
3) Keep conflict keys explicit for ON CONFLICT upserts.
"""
from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase


# ── Declarative bases ──────────────────────────────────────


class TimescaleBase(DeclarativeBase):
    """Base for time-series tables stored in TimescaleDB."""


class BusinessBase(DeclarativeBase):
    """Base for business tables stored in PostgreSQL."""


# ── TimescaleDB tables ─────────────────────────────────────


class StockBar(TimescaleBase):
    """1-minute stock bars.

    Composite PK includes partitioning column ``timestamp`` to satisfy
    Timescale unique-index constraints on hypertables.
    """

    __tablename__ = "stock_1min_bars"

    symbol = Column(String(20), primary_key=True, nullable=False)
    timestamp = Column(DateTime(timezone=True), primary_key=True, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(BigInteger, nullable=False, default=0)
    vwap = Column(Float, nullable=True)

    __table_args__ = (
        Index("idx_stock_bars_symbol_time", "symbol", "timestamp"),
    )


class OptionSnapshot(TimescaleBase):
    """5-minute option snapshots.

    Composite PK includes partitioning column ``timestamp``.
    """

    __tablename__ = "option_5min_snapshots"

    underlying = Column(String(20), nullable=False)
    symbol = Column(String(50), primary_key=True, nullable=False)
    timestamp = Column(DateTime(timezone=True), primary_key=True, nullable=False)
    expiry = Column(Date, nullable=False)
    strike = Column(Float, nullable=False)
    option_type = Column(String(4), nullable=False)
    last_price = Column(Float, nullable=False, default=0.0)
    bid = Column(Float, nullable=False, default=0.0)
    ask = Column(Float, nullable=False, default=0.0)
    volume = Column(BigInteger, nullable=False, default=0)
    open_interest = Column(BigInteger, nullable=False, default=0)
    iv = Column(Float, nullable=False, default=0.0)
    delta = Column(Float, nullable=False, default=0.0)
    gamma = Column(Float, nullable=False, default=0.0)
    theta = Column(Float, nullable=False, default=0.0)
    vega = Column(Float, nullable=False, default=0.0)
    vanna = Column(Float, nullable=False, default=0.0)
    charm = Column(Float, nullable=False, default=0.0)
    underlying_price = Column(Float, nullable=True)
    is_tradeable = Column(Boolean, nullable=False, default=False)

    __table_args__ = (
        Index("idx_option_snap_underlying_expiry", "underlying", "expiry", "timestamp"),
        Index("idx_option_snap_strike", "underlying", "strike", "option_type"),
        Index("idx_option_snap_tradeable", "underlying", "timestamp", "is_tradeable"),
    )


class StockDailyBar(TimescaleBase):
    """Daily stock bars (swing mode)."""

    __tablename__ = "stock_daily"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    trading_date = Column(Date, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(BigInteger, nullable=False, default=0)

    __table_args__ = (
        UniqueConstraint("symbol", "trading_date", name="uq_stock_daily"),
        Index("idx_stock_daily_symbol_date", "symbol", "trading_date"),
    )


class OptionDailySnapshot(TimescaleBase):
    """Daily option-chain snapshots (swing mode)."""

    __tablename__ = "option_daily"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    underlying = Column(String(20), nullable=False)
    symbol = Column(String(50), nullable=False)
    snapshot_date = Column(Date, nullable=False)
    expiry = Column(Date, nullable=False)
    strike = Column(Float, nullable=False)
    option_type = Column(String(4), nullable=False)
    last_price = Column(Float, nullable=False, default=0.0)
    bid = Column(Float, nullable=False, default=0.0)
    ask = Column(Float, nullable=False, default=0.0)
    volume = Column(BigInteger, nullable=False, default=0)
    open_interest = Column(BigInteger, nullable=False, default=0)
    iv = Column(Float, nullable=False, default=0.0)
    delta = Column(Float, nullable=False, default=0.0)
    gamma = Column(Float, nullable=False, default=0.0)
    theta = Column(Float, nullable=False, default=0.0)
    vega = Column(Float, nullable=False, default=0.0)
    vanna = Column(Float, nullable=False, default=0.0)
    charm = Column(Float, nullable=False, default=0.0)
    underlying_price = Column(Float, nullable=True)
    is_tradeable = Column(Boolean, nullable=False, default=False)

    __table_args__ = (
        UniqueConstraint("symbol", "snapshot_date", name="uq_option_daily"),
        Index("idx_option_daily_underlying_date", "underlying", "snapshot_date"),
        Index("idx_option_daily_expiry", "underlying", "expiry", "snapshot_date"),
        Index("idx_option_daily_tradeable", "underlying", "snapshot_date", "is_tradeable"),
    )


class OptionIVDaily(TimescaleBase):
    """Daily aggregated IV summary per underlying (derived from intraday 5-min snapshots).

    NOT a hypertable — small table (~symbols × trading_days).
    Populated by ``aggregate_option_daily`` Celery task after ``batch_flush_to_db``.
    """

    __tablename__ = "option_iv_daily"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    underlying = Column(String(20), nullable=False)
    trading_date = Column(Date, nullable=False)
    avg_iv = Column(Float, nullable=True)
    atm_iv = Column(Float, nullable=True)
    call_iv = Column(Float, nullable=True)
    put_iv = Column(Float, nullable=True)
    sample_size = Column(Integer, nullable=False, default=0)
    underlying_price = Column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint("underlying", "trading_date", name="uq_option_iv_daily"),
        Index("idx_option_iv_daily_date", "underlying", "trading_date"),
    )


# ── PostgreSQL business tables ─────────────────────────────


class BlueprintRecord(BusinessBase):
    """Generated LLM trading blueprint."""

    __tablename__ = "llm_trading_blueprint"

    id = Column(String(36), primary_key=True)
    trading_date = Column(Date, nullable=False, unique=True)
    generated_at = Column(DateTime(timezone=True), nullable=False)
    model_provider = Column(String(20), nullable=False, default="openai")
    model_version = Column(String(50), nullable=False, default="gpt-4o")
    blueprint_json = Column(JSONB, nullable=False)
    reasoning_json = Column(JSONB, nullable=True)
    status = Column(String(20), nullable=False, default="pending")
    execution_summary = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index("idx_blueprint_date", "trading_date"),
        Index("idx_blueprint_status", "status"),
    )


class OrderRecord(BusinessBase):
    """Order records (JSON payload for multi-leg flexibility)."""

    __tablename__ = "orders"

    id = Column(String(36), primary_key=True)
    blueprint_id = Column(String(36), nullable=True)
    underlying = Column(String(20), nullable=False)
    order_json = Column(JSONB, nullable=False)
    status = Column(String(20), nullable=False, default="pending")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    filled_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("idx_order_blueprint", "blueprint_id"),
        Index("idx_order_status", "status"),
    )


class PositionRecord(BusinessBase):
    """Open/closed positions."""

    __tablename__ = "positions"

    id = Column(String(36), primary_key=True)
    symbol = Column(String(50), nullable=False)
    underlying = Column(String(20), nullable=False)
    asset_type = Column(String(10), nullable=False)
    side = Column(String(10), nullable=False)
    quantity = Column(Integer, nullable=False)
    avg_entry_price = Column(Float, nullable=False)
    current_price = Column(Float, nullable=False, default=0.0)
    unrealized_pnl = Column(Float, nullable=False, default=0.0)
    realized_pnl = Column(Float, nullable=False, default=0.0)
    position_json = Column(JSONB, nullable=True)
    opened_at = Column(DateTime(timezone=True), nullable=True)
    closed_at = Column(DateTime(timezone=True), nullable=True)
    is_open = Column(Boolean, nullable=False, default=True)

    __table_args__ = (
        Index("idx_position_symbol", "symbol"),
        Index("idx_position_open", "is_open"),
    )


class SignalFeatureRecord(BusinessBase):
    """Computed signal features per symbol/date."""

    __tablename__ = "signal_features"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    date = Column(Date, nullable=False)
    computed_at = Column(DateTime(timezone=True), nullable=False)
    features_json = Column(JSONB, nullable=False)

    __table_args__ = (
        UniqueConstraint("symbol", "date", name="uq_signal_feature"),
        Index("idx_signal_symbol_date", "symbol", "date"),
    )


class BackfillLog(BusinessBase):
    """Backfill run logs."""

    __tablename__ = "backfill_logs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    data_type = Column(String(20), nullable=False)
    date = Column(Date, nullable=False)
    gap_start = Column(DateTime(timezone=True), nullable=True)
    gap_end = Column(DateTime(timezone=True), nullable=True)
    records_filled = Column(Integer, nullable=False, default=0)
    status = Column(String(20), nullable=False, default="pending")
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class ExecutionEventRecord(BusinessBase):
    """Audit trail for execution events (stop-loss triggers, order lifecycle, etc.)."""

    __tablename__ = "execution_events"

    id = Column(String(36), primary_key=True)
    event_type = Column(String(40), nullable=False)
    symbol = Column(String(50), nullable=True)
    blueprint_id = Column(String(36), nullable=True)
    order_id = Column(String(36), nullable=True)
    payload = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (
        Index("idx_exec_event_type", "event_type"),
        Index("idx_exec_event_symbol", "symbol"),
        Index("idx_exec_event_created", "created_at"),
    )
