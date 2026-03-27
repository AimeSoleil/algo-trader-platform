"""Signal Service 过滤器包 — 按资产类型组织

导出期权交易级过滤器的公共接口。股票过滤器为预留扩展点。
"""
from shared.models.filter import FilterResult
from services.signal_service.app.filters.option_filters import (
    apply_trading_filter,
)

__all__ = [
    "FilterResult",
    "apply_trading_filter",
]
