"""组合与持仓数据模型"""
from __future__ import annotations
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field


class PositionSide(str, Enum):
    LONG = "long"
    SHORT = "short"


class OptionPosition(BaseModel):
    """期权持仓"""
    symbol: str  # 合约代码
    underlying: str
    expiry: str  # ISO date string
    strike: float
    option_type: str  # "call" / "put"
    side: PositionSide
    quantity: int
    avg_entry_price: float
    current_price: float = 0.0
    
    # 实时Greeks
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0
    vega: float = 0.0
    
    # P&L
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    
    market_value: float = 0.0  # quantity * current_price * 100 * direction_sign
    
    opened_at: datetime | None = None


class StockPosition(BaseModel):
    """股票持仓"""
    symbol: str
    side: PositionSide
    quantity: int
    avg_entry_price: float
    current_price: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    market_value: float = 0.0
    opened_at: datetime | None = None


class PortfolioGreeks(BaseModel):
    """组合级希腊字母"""
    total_delta: float = 0.0
    total_gamma: float = 0.0
    total_theta: float = 0.0
    total_vega: float = 0.0


class PortfolioSnapshot(BaseModel):
    """组合快照"""
    timestamp: datetime
    
    # 持仓
    option_positions: list[OptionPosition] = Field(default_factory=list)
    stock_positions: list[StockPosition] = Field(default_factory=list)
    
    # 组合Greeks
    greeks: PortfolioGreeks = Field(default_factory=PortfolioGreeks)
    
    # 资金
    cash: float = 0.0
    total_market_value: float = 0.0
    net_liquidation_value: float = 0.0
    
    # 保证金
    margin_used: float = 0.0
    margin_available: float = 0.0
    margin_usage_ratio: float = 0.0
    
    # 日P&L
    daily_pnl: float = 0.0
    daily_pnl_percent: float = 0.0
    total_unrealized_pnl: float = 0.0
    total_realized_pnl: float = 0.0
