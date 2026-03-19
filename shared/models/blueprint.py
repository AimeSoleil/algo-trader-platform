"""LLM 交易蓝图数据模型 — 盘后生成、盘中机械执行"""
from __future__ import annotations
from datetime import date, datetime
from enum import Enum
from typing import Any
from pydantic import BaseModel, Field
import uuid

from shared.models.signal import DataQuality


class BlueprintStatus(str, Enum):
    PENDING = "pending"      # 已生成，待次日加载
    ACTIVE = "active"        # 已加载，盘中执行中
    COMPLETED = "completed"  # 当日收盘后归档
    CANCELLED = "cancelled"  # 手动取消


class StrategyType(str, Enum):
    """支持的期权策略类型"""
    SINGLE_LEG = "single_leg"
    VERTICAL_SPREAD = "vertical_spread"
    IRON_CONDOR = "iron_condor"
    IRON_BUTTERFLY = "iron_butterfly"
    BUTTERFLY = "butterfly"
    CALENDAR_SPREAD = "calendar_spread"
    DIAGONAL_SPREAD = "diagonal_spread"
    STRADDLE = "straddle"
    STRANGLE = "strangle"
    COVERED_CALL = "covered_call"
    PROTECTIVE_PUT = "protective_put"
    COLLAR = "collar"


class Direction(str, Enum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"


class ConditionOperator(str, Enum):
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="
    EQ = "=="
    BETWEEN = "between"
    CROSSES_ABOVE = "crosses_above"
    CROSSES_BELOW = "crosses_below"


class ConditionField(str, Enum):
    """规则引擎可读取的字段"""
    UNDERLYING_PRICE = "underlying_price"
    IV = "iv"
    IV_RANK = "iv_rank"
    DELTA = "delta"
    GAMMA = "gamma"
    THETA = "theta"
    PORTFOLIO_DELTA = "portfolio_delta"
    SPREAD_WIDTH = "spread_width"
    TIME = "time"  # HH:MM format
    PNL_PERCENT = "pnl_percent"
    VOLUME = "volume"


class TriggerCondition(BaseModel):
    """规则引擎可机械判定的触发条件"""
    field: ConditionField
    operator: ConditionOperator
    value: float | list[float] = Field(
        description="阈值；BETWEEN 时为 [low, high] 列表"
    )
    timeframe: str = "realtime"  # "realtime" / "5min_avg" / "since_open"
    description: str = ""  # 人类可读描述

    def evaluate(self, current_value: float, previous_value: float | None = None) -> bool:
        """评估条件是否满足"""
        match self.operator:
            case ConditionOperator.GT:
                return current_value > self.value
            case ConditionOperator.GTE:
                return current_value >= self.value
            case ConditionOperator.LT:
                return current_value < self.value
            case ConditionOperator.LTE:
                return current_value <= self.value
            case ConditionOperator.EQ:
                return abs(current_value - self.value) < 1e-6
            case ConditionOperator.BETWEEN:
                if isinstance(self.value, list) and len(self.value) == 2:
                    return self.value[0] <= current_value <= self.value[1]
                return False
            case ConditionOperator.CROSSES_ABOVE:
                if previous_value is None:
                    return False
                return previous_value <= self.value and current_value > self.value
            case ConditionOperator.CROSSES_BELOW:
                if previous_value is None:
                    return False
                return previous_value >= self.value and current_value < self.value
        return False


class AdjustmentAction(str, Enum):
    HEDGE_DELTA = "hedge_delta"
    ROLL_STRIKE = "roll_strike"
    CLOSE_LEG = "close_leg"
    ADD_LEG = "add_leg"
    CLOSE_ALL = "close_all"


class AdjustmentRule(BaseModel):
    """盘中动态调整规则"""
    trigger: TriggerCondition
    action: AdjustmentAction
    params: dict[str, Any] = Field(default_factory=dict)
    description: str = ""


class OptionLeg(BaseModel):
    """期权策略单腿"""
    expiry: date
    strike: float
    option_type: str  # "call" / "put"
    side: str  # "buy" / "sell"
    quantity: int = 1
    target_entry_price: float | None = None  # 目标入场价（限价）
    price_tolerance: float = 0.05  # 价格容忍度（滑点范围）

    @property
    def is_long(self) -> bool:
        return self.side == "buy"
    
    @property 
    def is_short(self) -> bool:
        return self.side == "sell"


class SymbolPlan(BaseModel):
    """单个标的的交易计划"""
    underlying: str
    strategy_type: StrategyType
    direction: Direction
    
    # 期权腿
    legs: list[OptionLeg] = Field(default_factory=list, min_length=1)
    
    # 触发条件
    entry_conditions: list[TriggerCondition] = Field(default_factory=list)
    exit_conditions: list[TriggerCondition] = Field(default_factory=list)
    adjustment_rules: list[AdjustmentRule] = Field(default_factory=list)
    
    # 风控参数
    max_position_size: int = 1  # 最大合约组数
    stop_loss_amount: float | None = None  # 止损金额
    take_profit_amount: float | None = None  # 止盈金额
    max_loss_per_trade: float = 500.0  # 单笔最大亏损
    
    # LLM推理
    reasoning: str = ""  # LLM 推理过程
    confidence: float = 0.5  # 置信度 0-1

    # 信号数据质量标注（由 analysis-service 后处理注入）
    data_quality_score: float = Field(1.0, ge=0.0, le=1.0, description="信号数据综合质量 0-1")
    data_quality_warnings: list[str] = Field(default_factory=list, description="数据质量问题描述")
    signal_data_quality: DataQuality | None = Field(None, description="完整信号数据质量对象")

    # 执行状态（盘中更新）
    is_entered: bool = False
    entry_time: datetime | None = None
    entry_fill_prices: list[float] = Field(default_factory=list)
    is_exited: bool = False
    exit_time: datetime | None = None
    exit_fill_prices: list[float] = Field(default_factory=list)
    realized_pnl: float = 0.0


class LLMTradingBlueprint(BaseModel):
    """LLM 生成的日内交易蓝图"""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    trading_date: date  # 适用交易日
    generated_at: datetime  # 生成时间
    model_provider: str = "openai"  # "openai" / "copilot"
    model_version: str = "gpt-4o"
    
    # 市场判断
    market_regime: str = "neutral"  # "high_vol" / "low_vol" / "trending_up" / "trending_down" / "ranging" / "neutral"
    market_analysis: str = ""  # 市场分析摘要
    
    # 标的级策略
    symbol_plans: list[SymbolPlan] = Field(default_factory=list)
    
    # 全局风控约束
    max_total_positions: int = 10
    max_daily_loss: float = 2000.0  # 日最大亏损限额
    max_margin_usage: float = 0.5  # 最大保证金占用比例
    portfolio_delta_limit: float = 0.5  # 组合 Delta 上限
    portfolio_gamma_limit: float = 0.1  # 组合 Gamma 上限
    
    # 状态
    status: BlueprintStatus = BlueprintStatus.PENDING

    # 数据质量全局摘要（所有 symbol_plan 中的最低 data_quality_score）
    min_data_quality_score: float = Field(1.0, ge=0.0, le=1.0, description="所有标的中最低数据质量分")
    data_quality_summary: list[str] = Field(default_factory=list, description="全局数据质量警告")

    # 盘后执行摘要（收盘后回填）
    execution_summary: dict[str, Any] | None = None
