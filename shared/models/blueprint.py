"""LLM 交易蓝图数据模型 — 盘后生成、盘中机械执行"""
from __future__ import annotations
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Any, Literal
from pydantic import BaseModel, Field, field_validator, model_validator
import re
import uuid

from shared.models.signal import DataQuality

_NUMBER_RE = re.compile(r"[+-]?\d+(?:\.\d+)?")
_DTE_RE = re.compile(r"(?P<start>\d+)(?:\s*-\s*(?P<end>\d+))?\s*dte", re.I)


def _extract_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    match = _NUMBER_RE.search(value)
    if not match:
        return None
    return float(match.group())


def _normalize_numeric_value(value: Any) -> float | list[float] | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, list):
        normalized: list[float] = []
        for item in value:
            parsed = _extract_float(item)
            if parsed is None:
                return None
            normalized.append(parsed)
        return normalized
    if isinstance(value, str):
        return _extract_float(value)
    return None


def _normalize_expiry_value(value: Any) -> Any:
    if not isinstance(value, str):
        return value

    raw = value.strip()
    try:
        return date.fromisoformat(raw)
    except ValueError:
        pass

    match = _DTE_RE.search(raw)
    if not match:
        return value

    start_days = int(match.group("start"))
    end_days = int(match.group("end") or start_days)
    target_days = round((start_days + end_days) / 2)
    return date.today() + timedelta(days=target_days)


def _is_valid_enum_value(enum_cls: type[Enum], value: Any) -> bool:
    try:
        enum_cls(value)
    except (ValueError, TypeError):
        return False
    return True


def _sanitize_trigger_condition_items(items: Any) -> Any:
    if not isinstance(items, list):
        return items

    sanitized: list[Any] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        normalized = dict(item)
        if not _is_valid_enum_value(ConditionField, normalized.get("field")):
            continue
        if not _is_valid_enum_value(ConditionOperator, normalized.get("operator")):
            continue
        normalized_value = _normalize_numeric_value(normalized.get("value"))
        if normalized_value is None:
            continue
        normalized["value"] = normalized_value
        sanitized.append(normalized)
    return sanitized


def _sanitize_adjustment_rules(items: Any) -> Any:
    if not isinstance(items, list):
        return items

    sanitized: list[Any] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        normalized = dict(item)
        trigger = normalized.get("trigger")
        if isinstance(trigger, dict):
            normalized_trigger = dict(trigger)
            if not _is_valid_enum_value(ConditionField, normalized_trigger.get("field")):
                continue
            if not _is_valid_enum_value(ConditionOperator, normalized_trigger.get("operator")):
                continue
            normalized_value = _normalize_numeric_value(normalized_trigger.get("value"))
            if normalized_value is None:
                continue
            normalized_trigger["value"] = normalized_value
            normalized["trigger"] = normalized_trigger
        elif not isinstance(trigger, str):
            continue
        sanitized.append(normalized)
    return sanitized


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
    VWAP = "vwap"
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


# Map common LLM action aliases → valid AdjustmentAction values
_ACTION_ALIASES: dict[str, AdjustmentAction] = {
    "close_position": AdjustmentAction.CLOSE_ALL,
    "close": AdjustmentAction.CLOSE_ALL,
    "reduce_position": AdjustmentAction.CLOSE_LEG,
    "reduce": AdjustmentAction.CLOSE_LEG,
    "reduce_size": AdjustmentAction.CLOSE_LEG,
    "hedge": AdjustmentAction.HEDGE_DELTA,
    "roll": AdjustmentAction.ROLL_STRIKE,
    "roll_untested_side": AdjustmentAction.ROLL_STRIKE,
    "roll_tested_side": AdjustmentAction.ROLL_STRIKE,
    "add": AdjustmentAction.ADD_LEG,
}

# Map operator strings used in LLM free-text triggers
_OP_MAP: list[tuple[str, ConditionOperator]] = [
    (">=", ConditionOperator.GTE),
    ("<=", ConditionOperator.LTE),
    ("==", ConditionOperator.EQ),
    (">", ConditionOperator.GT),
    ("<", ConditionOperator.LT),
]

# Natural-language operator patterns → (ConditionOperator, negate?)
# Order matters — longer phrases first to avoid partial matches.
import re as _re

_NL_OP_PATTERNS: list[tuple[_re.Pattern, ConditionOperator]] = [
    (_re.compile(r"\bdrops\s+below\b", _re.I), ConditionOperator.LT),
    (_re.compile(r"\bfalls\s+below\b", _re.I), ConditionOperator.LT),
    (_re.compile(r"\bbreaks?\s+below\b", _re.I), ConditionOperator.LT),
    (_re.compile(r"\bbelow\b", _re.I), ConditionOperator.LT),
    (_re.compile(r"\bbreaks?\s+above\b", _re.I), ConditionOperator.GT),
    (_re.compile(r"\brises?\s+above\b", _re.I), ConditionOperator.GT),
    (_re.compile(r"\babove\b", _re.I), ConditionOperator.GT),
    (_re.compile(r"\bexceeds?\b", _re.I), ConditionOperator.GT),
    (_re.compile(r"\bbreache?s?\b", _re.I), ConditionOperator.GT),
    (_re.compile(r"\bmoves?\s+to\b", _re.I), ConditionOperator.GTE),
    (_re.compile(r"\breache?s?\b", _re.I), ConditionOperator.GTE),
]

# Map natural-language field names → ConditionField
_FIELD_ALIASES: dict[str, ConditionField] = {
    "underlying_price": ConditionField.UNDERLYING_PRICE,
    "underlying": ConditionField.UNDERLYING_PRICE,
    "price": ConditionField.UNDERLYING_PRICE,
    "vwap": ConditionField.VWAP,
    "iv_rank": ConditionField.IV_RANK,
    "iv rank": ConditionField.IV_RANK,
    "iv": ConditionField.IV,
    "delta": ConditionField.DELTA,
    "gamma": ConditionField.GAMMA,
    "theta": ConditionField.THETA,
    "portfolio_delta": ConditionField.PORTFOLIO_DELTA,
    "portfolio delta": ConditionField.PORTFOLIO_DELTA,
    "pnl_percent": ConditionField.PNL_PERCENT,
    "pnl percent": ConditionField.PNL_PERCENT,
    "pnl": ConditionField.PNL_PERCENT,
    "volume": ConditionField.VOLUME,
    "spread_width": ConditionField.SPREAD_WIDTH,
    "spread width": ConditionField.SPREAD_WIDTH,
}

# Regex to extract the first numeric value (possibly negative, with decimals)
_NUMBER_RE = _re.compile(r"[+-]?\d+(?:\.\d+)?")


def _guess_field(text: str) -> ConditionField:
    """Guess the ConditionField from a free-text fragment."""
    lower = text.lower()
    for alias, field in _FIELD_ALIASES.items():
        if alias in lower:
            return field
    return ConditionField.UNDERLYING_PRICE


def _parse_trigger_string(raw: str) -> TriggerCondition:
    """Best-effort parse of a free-text trigger like 'underlying_price > 352'.

    Handles both symbolic operators (>, <, >=, <=, ==) and natural language
    ('exceeds', 'drops below', 'breaches', etc.).  Compound conditions with
    'or'/'and' take the first clause only; the full text is preserved in
    ``description``.
    """
    # Take first clause before 'or' / 'and'
    clause = raw.split(" or ")[0].split(" and ")[0].strip()

    # ── Stage 1: try symbolic operators ──
    for op_str, op_enum in _OP_MAP:
        if op_str in clause:
            parts = clause.split(op_str, 1)
            field_raw = parts[0].strip()
            value_raw = parts[1].strip()
            try:
                field_enum = ConditionField(field_raw)
            except ValueError:
                field_enum = _guess_field(field_raw)
            num = _NUMBER_RE.search(value_raw)
            value = float(num.group()) if num else 0.0
            return TriggerCondition(
                field=field_enum,
                operator=op_enum,
                value=value,
                description=raw,
            )

    # ── Stage 2: try natural-language operators ──
    for pattern, op_enum in _NL_OP_PATTERNS:
        m = pattern.search(clause)
        if m:
            field_enum = _guess_field(clause[:m.start()])
            num = _NUMBER_RE.search(clause[m.end():])
            value = float(num.group()) if num else 0.0
            return TriggerCondition(
                field=field_enum,
                operator=op_enum,
                value=value,
                description=raw,
            )

    # ── Stage 3: fallback — extract any number, guess field ──
    num = _NUMBER_RE.search(raw)
    if num:
        return TriggerCondition(
            field=_guess_field(raw),
            operator=ConditionOperator.GT,
            value=float(num.group()),
            description=f"[fuzzy] {raw}",
        )

    # Fully unparseable
    return TriggerCondition(
        field=ConditionField.UNDERLYING_PRICE,
        operator=ConditionOperator.GT,
        value=0.0,
        description=f"[unparsed] {raw}",
    )


class AdjustmentRule(BaseModel):
    """盘中动态调整规则"""
    trigger: TriggerCondition
    action: AdjustmentAction
    params: dict[str, Any] = Field(default_factory=dict)
    description: str = ""

    @field_validator("trigger", mode="before")
    @classmethod
    def _coerce_trigger(cls, v: Any) -> Any:
        if isinstance(v, str):
            return _parse_trigger_string(v)
        return v

    @field_validator("action", mode="before")
    @classmethod
    def _coerce_action(cls, v: Any) -> Any:
        if isinstance(v, str):
            v_lower = v.strip().lower()
            # Try direct enum match first
            try:
                return AdjustmentAction(v_lower)
            except ValueError:
                pass
            # Try alias map
            if v_lower in _ACTION_ALIASES:
                return _ACTION_ALIASES[v_lower]
            # Fallback
            return AdjustmentAction.CLOSE_ALL
        return v


class OptionLeg(BaseModel):
    """期权策略单腿"""
    expiry: date
    strike: float
    option_type: Literal["call", "put"]
    side: Literal["buy", "sell"]
    quantity: int = 1
    target_entry_price: float | None = None  # 目标入场价（限价）
    price_tolerance: float = 0.05  # 价格容忍度（滑点范围）

    @field_validator("expiry", mode="before")
    @classmethod
    def _coerce_expiry(cls, v: Any) -> Any:
        return _normalize_expiry_value(v)

    @field_validator("strike", mode="before")
    @classmethod
    def _coerce_strike(cls, v: Any) -> Any:
        normalized = _normalize_numeric_value(v)
        return normalized if normalized is not None else v

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
    max_position_size: float = Field(1.0, ge=0.0, le=1.5, description="仓位比例 (1.0=全仓, 0.5=半仓)")
    max_contracts: int = Field(1, ge=1, description="最大合约组数")
    stop_loss_amount: float | None = None  # 止损金额
    take_profit_amount: float | None = None  # 止盈金额
    max_loss_per_trade: float = Field(500.0, gt=0, description="单笔最大亏损")

    @field_validator("max_contracts", mode="before")
    @classmethod
    def _coerce_contracts(cls, v: Any) -> int:
        """LLMs sometimes return a float for contract count; coerce to int ≥ 1."""
        if isinstance(v, float):
            v = max(1, round(v)) if v >= 1 else 1
        return int(v)

    @field_validator("entry_conditions", mode="before")
    @classmethod
    def _coerce_entry_conditions(cls, v: Any) -> Any:
        return _sanitize_trigger_condition_items(v)

    @field_validator("exit_conditions", mode="before")
    @classmethod
    def _coerce_exit_conditions(cls, v: Any) -> Any:
        return _sanitize_trigger_condition_items(v)

    @field_validator("adjustment_rules", mode="before")
    @classmethod
    def _coerce_adjustment_rules(cls, v: Any) -> Any:
        return _sanitize_adjustment_rules(v)
    
    # LLM推理
    reasoning: str = ""  # LLM 推理过程
    confidence: float = Field(0.5, ge=0.0, le=1.0, description="置信度 0-1")

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

    @model_validator(mode="after")
    def _validate_strategy_legs(self):
        """Validate strategy_type ↔ legs count consistency."""
        n = len(self.legs)
        expected = {
            StrategyType.SINGLE_LEG: (1, 1),
            StrategyType.VERTICAL_SPREAD: (2, 2),
            StrategyType.IRON_CONDOR: (4, 4),
            StrategyType.IRON_BUTTERFLY: (4, 4),
            StrategyType.BUTTERFLY: (3, 4),
            StrategyType.CALENDAR_SPREAD: (2, 2),
            StrategyType.DIAGONAL_SPREAD: (2, 2),
            StrategyType.STRADDLE: (2, 2),
            StrategyType.STRANGLE: (2, 2),
            StrategyType.COVERED_CALL: (1, 2),
            StrategyType.PROTECTIVE_PUT: (1, 2),
            StrategyType.COLLAR: (2, 3),
        }
        rng = expected.get(self.strategy_type)
        if rng and not (rng[0] <= n <= rng[1]):
            raise ValueError(
                f"{self.strategy_type.value} expects {rng[0]}-{rng[1]} legs, got {n}"
            )
        return self


class LLMTradingBlueprint(BaseModel):
    """LLM 生成的日内交易蓝图"""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    trading_date: date  # 适用交易日
    generated_at: datetime  # 生成时间
    model_provider: str = "openai"  # "openai" / "copilot"
    model_version: str = "gpt-4o"
    
    # 市场判断
    market_regime: str = "neutral"  # LLM may return varied regime strings — keep loose for forward compat
    market_analysis: str = ""  # 市场分析摘要
    
    # 标的级策略
    symbol_plans: list[SymbolPlan] = Field(default_factory=list)
    
    # 全局风控约束
    max_total_positions: int = 5
    max_daily_loss: float = 2000.0  # 日最大亏损限额
    max_margin_usage: float = 0.5  # 最大保证金占用比例
    portfolio_delta_limit: float = 0.5  # 组合 Delta 上限
    portfolio_gamma_limit: float = 0.1  # 组合 Gamma 上限
    
    # 状态
    status: BlueprintStatus = BlueprintStatus.PENDING

    # 数据质量全局摘要（所有 symbol_plan 中的最低 data_quality_score）
    min_data_quality_score: float = Field(1.0, ge=0.0, le=1.0, description="所有标的中最低数据质量分")
    data_quality_summary: list[str] = Field(default_factory=list, description="全局数据质量警告")

    # Symbols from failed chunks that couldn't be analyzed
    missing_symbols: list[str] = Field(default_factory=list, description="Symbols lost due to chunk failures")

    # LLM reasoning context (agent outputs, critic feedback, raw response)
    # Stored separately — not part of the blueprint_json column.
    reasoning_context: dict[str, Any] | None = Field(
        None,
        exclude=True,
        description="Full LLM reasoning chain for auditability",
    )

    # 盘后执行摘要（收盘后回填）
    execution_summary: dict[str, Any] | None = None
