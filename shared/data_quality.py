"""数据质量评估模块 — Data Quality Scoring & Gating

本模块集中管理数据质量相关的所有常量、评分逻辑和执行门控，
确保 Signal Service（评分）和 Trade Service（门控）使用一致的配置。

评分公式（加权三部分）：

    score = stock_coverage × W_stock
          + option_coverage × W_option
          + degradation_bonus × W_degradation

其中：
    stock_coverage  = min(1, stock_bar_count / STOCK_FULL_BARS)   # 线性 0→1
    option_coverage = min(1, option_row_count / OPTION_FULL_ROWS) # 线性 0→1
    degradation_bonus = 0 if 有降级指标 else 1                     # 二值

门控阈值（执行层）：
    score < skip_threshold  → 完全跳过该标的
    score < reduce_threshold → 仓位减半（保守执行）
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from shared.models.signal import DataQuality


# ═══════════════════════════════════════════════════════════
# 默认常量 — 可通过 DataQualityConfig 覆盖
# ═══════════════════════════════════════════════════════════

# ── 评分参考值（分母）──
# 260 ≈ 美股一年交易日，代表"完整"的日线数据覆盖量
STOCK_FULL_BARS: int = 260
# 200 行期权链记录代表一次较为完整的快照
OPTION_FULL_ROWS: int = 200

# ── 评分权重（三部分之和应 = 1.0）──
WEIGHT_STOCK: float = 0.5       # 股票数据覆盖占比
WEIGHT_OPTION: float = 0.3      # 期权数据覆盖占比
WEIGHT_DEGRADATION: float = 0.2  # 无降级指标奖励

# ── 警告触发阈值 ──
STOCK_MIN_BARS: int = 30         # 股票最低可用行数
STOCK_WARN_BARS: int = 200       # 低于此值提示"偏少"
OPTION_MIN_ROWS: int = 20        # 期权最低可用行数

# ── 执行门控阈值 ──
DEFAULT_SKIP_THRESHOLD: float = 0.3    # score < 此值 → 跳过执行
DEFAULT_REDUCE_THRESHOLD: float = 0.7  # score < 此值 → 仓位减半
DEFAULT_REDUCE_FACTOR: float = 0.5     # 仓位缩减比例（0.5 = 减半）


# ═══════════════════════════════════════════════════════════
# 可配置参数容器
# ═══════════════════════════════════════════════════════════

@dataclass
class DataQualityConfig:
    """数据质量评分 & 门控的完整配置。

    所有字段均有默认值，可通过 ``from_settings()`` 从 config.yaml 加载覆盖。
    """

    # ── 评分权重 ──
    weight_stock: float = WEIGHT_STOCK
    weight_option: float = WEIGHT_OPTION
    weight_degradation: float = WEIGHT_DEGRADATION

    # ── 评分参考值 ──
    stock_full_bars: int = STOCK_FULL_BARS
    option_full_rows: int = OPTION_FULL_ROWS

    # ── 执行门控 ──
    skip_threshold: float = DEFAULT_SKIP_THRESHOLD
    reduce_threshold: float = DEFAULT_REDUCE_THRESHOLD
    reduce_factor: float = DEFAULT_REDUCE_FACTOR

    @classmethod
    def from_settings(cls, settings: object | None = None) -> DataQualityConfig:
        """从全局 Settings 构造配置，缺失字段使用默认值。

        Parameters
        ----------
        settings : Settings | None
            ``shared.config.get_settings()`` 返回的配置对象。
            传 None 时使用纯默认值（方便测试）。
        """
        if settings is None:
            return cls()

        dq = getattr(settings, "data_quality", None)
        if dq is None:
            return cls()

        return cls(
            weight_stock=getattr(dq, "weight_stock", WEIGHT_STOCK),
            weight_option=getattr(dq, "weight_option", WEIGHT_OPTION),
            weight_degradation=getattr(dq, "weight_degradation", WEIGHT_DEGRADATION),
            stock_full_bars=getattr(dq, "stock_full_bars", STOCK_FULL_BARS),
            option_full_rows=getattr(dq, "option_full_rows", OPTION_FULL_ROWS),
            skip_threshold=getattr(dq, "skip_threshold", DEFAULT_SKIP_THRESHOLD),
            reduce_threshold=getattr(dq, "reduce_threshold", DEFAULT_REDUCE_THRESHOLD),
            reduce_factor=getattr(dq, "reduce_factor", DEFAULT_REDUCE_FACTOR),
        )


# ═══════════════════════════════════════════════════════════
# 评分函数 — Signal Service 调用
# ═══════════════════════════════════════════════════════════

def compute_quality_score(
    stock_bar_count: int,
    option_row_count: int,
    degraded_indicators: list[str],
    *,
    cfg: DataQualityConfig | None = None,
) -> float:
    """根据数据覆盖量和指标降级情况计算综合质量评分。

    Parameters
    ----------
    stock_bar_count : int
        用于计算股票技术指标的 OHLCV bar 行数。
    option_row_count : int
        用于计算期权指标的期权链记录行数。
    degraded_indicators : list[str]
        因数据不足而降级的指标名列表（空列表 = 无降级）。
    cfg : DataQualityConfig | None
        评分配置。None 时使用默认权重。

    Returns
    -------
    float
        0.0 ~ 1.0 之间的综合质量分数，保留 4 位小数。

    Examples
    --------
    >>> compute_quality_score(260, 200, [])
    1.0
    >>> compute_quality_score(130, 100, ["MACD"])
    0.4
    """
    if cfg is None:
        cfg = DataQualityConfig()

    # 股票覆盖率：线性映射 [0, stock_full_bars] → [0, 1]
    stock_coverage = min(1.0, stock_bar_count / cfg.stock_full_bars) if cfg.stock_full_bars > 0 else 0.0

    # 期权覆盖率：线性映射 [0, option_full_rows] → [0, 1]
    option_coverage = min(1.0, option_row_count / cfg.option_full_rows) if cfg.option_full_rows > 0 else 0.0

    # 降级惩罚：有任何降级指标 → 失去全部降级奖励
    degradation_bonus = 0.0 if degraded_indicators else 1.0

    score = (
        stock_coverage * cfg.weight_stock
        + option_coverage * cfg.weight_option
        + degradation_bonus * cfg.weight_degradation
    )
    return round(score, 4)


def build_quality_warnings(
    stock_bar_count: int,
    option_row_count: int,
) -> list[str]:
    """根据数据量生成人类可读的质量警告列表。

    Parameters
    ----------
    stock_bar_count : int
        股票 OHLCV bar 行数。
    option_row_count : int
        期权链行数。

    Returns
    -------
    list[str]
        警告消息列表；如果数据充足则返回空列表。
    """
    warnings: list[str] = []

    if stock_bar_count == 0:
        warnings.append("无股票数据")
    elif stock_bar_count < STOCK_MIN_BARS:
        warnings.append(f"股票数据不足: {stock_bar_count} 行 (<{STOCK_MIN_BARS} 最低要求)")
    elif stock_bar_count < STOCK_WARN_BARS:
        warnings.append(f"股票数据偏少: {stock_bar_count} 行 (<{STOCK_WARN_BARS} 完整要求)")

    if option_row_count == 0:
        warnings.append("无期权数据，期权指标均为默认值")
    elif option_row_count < OPTION_MIN_ROWS:
        warnings.append(f"期权链数据偏少: {option_row_count} 行")

    return warnings


# ═══════════════════════════════════════════════════════════
# 门控函数 — Trade Service 调用
# ═══════════════════════════════════════════════════════════

def apply_quality_gate(
    quality_score: float,
    max_position_size: int,
    *,
    cfg: DataQualityConfig | None = None,
) -> tuple[bool, int]:
    """根据质量分数决定是否跳过执行或缩减仓位。

    Parameters
    ----------
    quality_score : float
        数据质量评分（0.0 ~ 1.0）。
    max_position_size : int
        原始最大仓位数量。
    cfg : DataQualityConfig | None
        门控配置。None 时使用默认阈值。

    Returns
    -------
    tuple[bool, int]
        (should_skip, adjusted_position_size)
        - should_skip=True  → 应完全跳过该标的
        - should_skip=False → 使用 adjusted_position_size 执行
    """
    if cfg is None:
        cfg = DataQualityConfig()

    # 极低质量 → 跳过
    if quality_score < cfg.skip_threshold:
        return True, 0

    # 中等质量 → 缩减仓位
    if quality_score < cfg.reduce_threshold:
        reduced = max(1, int(max_position_size * cfg.reduce_factor))
        return False, reduced

    # 质量合格 → 原样执行
    return False, max_position_size
