"""期权合约与希腊字母数据模型"""
from __future__ import annotations
from datetime import date, datetime
from enum import Enum
from pydantic import BaseModel, Field

from shared.utils.time import today_trading


class OptionType(str, Enum):
    CALL = "call"
    PUT = "put"


class OptionGreeks(BaseModel):
    """期权希腊字母"""
    delta: float = 0.0
    gamma: float = 0.0
    theta: float = 0.0
    vega: float = 0.0
    rho: float = 0.0  # 保留字段（向后兼容），当前不计算/不持久化
    iv: float = 0.0  # implied volatility
    vanna: float = 0.0  # ∂Δ/∂σ — IV 变化对 delta 的二阶影响
    charm: float = 0.0  # ∂Δ/∂t — 时间对 delta 的衰减影响


class OptionContract(BaseModel):
    """单个期权合约"""
    symbol: str  # e.g. "AAPL250321C00185000"
    underlying: str  # e.g. "AAPL"
    expiry: date
    strike: float
    option_type: OptionType
    last_price: float = 0.0
    bid: float = 0.0
    ask: float = 0.0
    volume: int = 0
    open_interest: int = 0
    greeks: OptionGreeks = Field(default_factory=OptionGreeks)
    timestamp: datetime | None = None
    is_tradeable: bool = False  # 由 filter 模块标记，非 fetcher 设置
    last_trade_date: date | None = None  # yfinance lastTradeDate，用于 stale trade check

    @property
    def mid_price(self) -> float:
        if self.bid > 0 and self.ask > 0:
            return (self.bid + self.ask) / 2
        return self.last_price

    @property
    def spread(self) -> float:
        if self.bid > 0 and self.ask > 0:
            return self.ask - self.bid
        return 0.0

    @property
    def days_to_expiry(self) -> int:
        return (self.expiry - today_trading()).days


class OptionChainSnapshot(BaseModel):
    """期权链快照（某标的某时刻所有合约）"""
    underlying: str
    underlying_price: float
    timestamp: datetime
    contracts: list[OptionContract] = Field(default_factory=list)

    @property
    def calls(self) -> list[OptionContract]:
        return [c for c in self.contracts if c.option_type == OptionType.CALL]

    @property
    def puts(self) -> list[OptionContract]:
        return [c for c in self.contracts if c.option_type == OptionType.PUT]

    @property
    def expiries(self) -> list[date]:
        return sorted(set(c.expiry for c in self.contracts))


class StockBar(BaseModel):
    """股票K线数据"""
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    vwap: float | None = None
