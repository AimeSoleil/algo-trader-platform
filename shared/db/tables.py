"""SQLAlchemy ORM 表定义 — TimescaleDB + PostgreSQL"""
from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import (
    JSON,
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


# ── TimescaleDB Tables (时序数据) ──────────────────────────


class TimescaleBase(DeclarativeBase):
    pass


class StockBar(TimescaleBase):
    """股票1分钟K线"""
    __tablename__ = "stock_1min_bars"

    symbol = Column(String(20), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(BigInteger, default=0)
    vwap = Column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint("symbol", "timestamp", name="uq_stock_bar"),
        Index("idx_stock_bars_symbol_time", "symbol", "timestamp"),
    )


class OptionSnapshot(TimescaleBase):
    """期权5分钟快照"""
    __tablename__ = "option_5min_snapshots"

    underlying = Column(String(20), nullable=False)
    symbol = Column(String(50), nullable=False)  # OCC option symbol
    timestamp = Column(DateTime(timezone=True), nullable=False)
    expiry = Column(Date, nullable=False)
    strike = Column(Float, nullable=False)
    option_type = Column(String(4), nullable=False)  # "call" / "put"
    last_price = Column(Float, default=0.0)
    bid = Column(Float, default=0.0)
    ask = Column(Float, default=0.0)
    volume = Column(BigInteger, default=0)
    open_interest = Column(BigInteger, default=0)
    iv = Column(Float, default=0.0)
    delta = Column(Float, default=0.0)
    gamma = Column(Float, default=0.0)
    theta = Column(Float, default=0.0)
    vega = Column(Float, default=0.0)

    __table_args__ = (
        UniqueConstraint("symbol", "timestamp", name="uq_option_snapshot"),
        Index("idx_option_snap_underlying_expiry", "underlying", "expiry", "timestamp"),
        Index("idx_option_snap_strike", "underlying", "strike", "option_type"),
    )


class StockDailyBar(TimescaleBase):
    """股票日线快照（Swing 模式）"""
    __tablename__ = "stock_daily"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    trading_date = Column(Date, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(BigInteger, default=0)

    __table_args__ = (
        UniqueConstraint("symbol", "trading_date", name="uq_stock_daily"),
        Index("idx_stock_daily_symbol_date", "symbol", "trading_date"),
    )


class OptionDailySnapshot(TimescaleBase):
    """期权日频链快照（Swing 模式）"""
    __tablename__ = "option_daily"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    underlying = Column(String(20), nullable=False)
    symbol = Column(String(50), nullable=False)
    snapshot_date = Column(Date, nullable=False)
    expiry = Column(Date, nullable=False)
    strike = Column(Float, nullable=False)
    option_type = Column(String(4), nullable=False)
    last_price = Column(Float, default=0.0)
    bid = Column(Float, default=0.0)
    ask = Column(Float, default=0.0)
    volume = Column(BigInteger, default=0)
    open_interest = Column(BigInteger, default=0)
    iv = Column(Float, default=0.0)
    delta = Column(Float, default=0.0)
    gamma = Column(Float, default=0.0)
    theta = Column(Float, default=0.0)
    vega = Column(Float, default=0.0)

    __table_args__ = (
        UniqueConstraint("symbol", "snapshot_date", name="uq_option_daily"),
        Index("idx_option_daily_underlying_date", "underlying", "snapshot_date"),
        Index("idx_option_daily_expiry", "underlying", "expiry", "snapshot_date"),
    )


# ── PostgreSQL Tables (业务数据) ──────────────────────────


class BusinessBase(DeclarativeBase):
    pass


class BlueprintRecord(BusinessBase):
    """LLM 交易蓝图存储"""
    __tablename__ = "llm_trading_blueprint"

    id = Column(String(36), primary_key=True)
    trading_date = Column(Date, nullable=False, unique=True)
    generated_at = Column(DateTime(timezone=True), nullable=False)
    model_provider = Column(String(20), default="openai")
    model_version = Column(String(50), default="gpt-4o")
    blueprint_json = Column(JSONB, nullable=False)
    status = Column(String(20), default="pending")
    execution_summary = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index("idx_blueprint_date", "trading_date"),
        Index("idx_blueprint_status", "status"),
    )


class OrderRecord(BusinessBase):
    """订单记录"""
    __tablename__ = "orders"

    id = Column(String(36), primary_key=True)
    blueprint_id = Column(String(36), nullable=True)
    underlying = Column(String(20), nullable=False)
    order_json = Column(JSONB, nullable=False)  # Full Order model as JSON
    status = Column(String(20), default="pending")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    filled_at = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("idx_order_blueprint", "blueprint_id"),
        Index("idx_order_status", "status"),
    )


class PositionRecord(BusinessBase):
    """持仓记录"""
    __tablename__ = "positions"

    id = Column(String(36), primary_key=True)
    symbol = Column(String(50), nullable=False)
    underlying = Column(String(20), nullable=False)
    asset_type = Column(String(10), nullable=False)  # "stock" / "option"
    side = Column(String(10), nullable=False)  # "long" / "short"
    quantity = Column(Integer, nullable=False)
    avg_entry_price = Column(Float, nullable=False)
    current_price = Column(Float, default=0.0)
    unrealized_pnl = Column(Float, default=0.0)
    realized_pnl = Column(Float, default=0.0)
    position_json = Column(JSONB, nullable=True)  # Extra details
    opened_at = Column(DateTime(timezone=True), nullable=True)
    closed_at = Column(DateTime(timezone=True), nullable=True)
    is_open = Column(Boolean, default=True)

    __table_args__ = (
        Index("idx_position_symbol", "symbol"),
        Index("idx_position_open", "is_open"),
    )


class SignalFeatureRecord(BusinessBase):
    """信号特征记录"""
    __tablename__ = "signal_features"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    date = Column(Date, nullable=False)
    computed_at = Column(DateTime(timezone=True), nullable=False)
    features_json = Column(JSONB, nullable=False)  # Full SignalFeatures as JSON

    __table_args__ = (
        UniqueConstraint("symbol", "date", name="uq_signal_feature"),
        Index("idx_signal_symbol_date", "symbol", "date"),
    )


class BackfillLog(BusinessBase):
    """数据回填日志"""
    __tablename__ = "backfill_logs"

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    symbol = Column(String(20), nullable=False)
    data_type = Column(String(20), nullable=False)  # "stock_bars" / "option_snapshots"
    date = Column(Date, nullable=False)
    gap_start = Column(DateTime(timezone=True), nullable=True)
    gap_end = Column(DateTime(timezone=True), nullable=True)
    records_filled = Column(Integer, default=0)
    status = Column(String(20), default="pending")  # "pending" / "completed" / "failed"
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
