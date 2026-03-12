"""订单与交易数据模型"""
from __future__ import annotations
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field
import uuid

from shared.utils.time import now_utc


class OrderSide(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"


class OrderStatus(str, Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class AssetType(str, Enum):
    STOCK = "stock"
    OPTION = "option"


class OrderLeg(BaseModel):
    """订单腿（多腿订单中的单腿）"""
    symbol: str  # 合约或股票代码
    asset_type: AssetType
    side: OrderSide
    quantity: int
    order_type: OrderType = OrderType.LIMIT
    limit_price: float | None = None
    stop_price: float | None = None
    
    # 成交信息
    filled_quantity: int = 0
    avg_fill_price: float = 0.0
    status: OrderStatus = OrderStatus.PENDING


class Order(BaseModel):
    """交易订单（支持多腿）"""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    blueprint_id: str | None = None  # 关联的蓝图ID
    symbol_plan_underlying: str | None = None  # 关联的标的
    
    legs: list[OrderLeg] = Field(default_factory=list, min_length=1)
    
    # 元信息
    created_at: datetime = Field(default_factory=now_utc)
    updated_at: datetime = Field(default_factory=now_utc)
    status: OrderStatus = OrderStatus.PENDING
    
    # 风控
    max_cost: float | None = None  # 最大花费限额
    
    # 执行信息
    submitted_at: datetime | None = None
    filled_at: datetime | None = None
    cancelled_at: datetime | None = None
    reject_reason: str | None = None
    
    @property
    def is_multi_leg(self) -> bool:
        return len(self.legs) > 1
    
    @property
    def is_terminal(self) -> bool:
        return self.status in (
            OrderStatus.FILLED, OrderStatus.CANCELLED, 
            OrderStatus.REJECTED, OrderStatus.EXPIRED
        )

    @property
    def total_filled_cost(self) -> float:
        return sum(
            leg.avg_fill_price * leg.filled_quantity * (1 if leg.side == OrderSide.BUY else -1)
            for leg in self.legs
        )
