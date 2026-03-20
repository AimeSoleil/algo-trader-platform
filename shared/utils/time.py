"""统一时间工具 — UTC 存储 + 配置时区 trading date

Design:
  - DB 列全部为 TIMESTAMPTZ（PostgreSQL 底层 UTC）
  - 代码中 datetime 统一用 UTC 生成 → ``now_utc()``
  - trading date 按配置时区（默认 America/New_York）计算 → ``today_trading()``
  - 需要交易时区当前时间时 → ``now_market()``
"""
from __future__ import annotations

from datetime import date, datetime, time, timezone
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo

# ── Constants ──────────────────────────────────────────────

_UTC = timezone.utc


# ── Core helpers ───────────────────────────────────────────


def now_utc() -> datetime:
    """返回当前 UTC 时间（timezone-aware）。

    替代所有 ``datetime.now()`` / ``datetime.utcnow()``。
    """
    return datetime.now(_UTC)


@lru_cache(maxsize=1)
def _trading_tz() -> ZoneInfo:
    """从配置读取交易时区（懒加载 + 缓存）。"""
    from shared.config import get_settings

    return ZoneInfo(get_settings().trading.timezone)


def market_tz() -> ZoneInfo:
    """返回配置中的交易时区 ZoneInfo 实例。"""
    return _trading_tz()


def today_trading() -> date:
    """按配置交易时区计算"今天"的日期。

    在 UTC 服务器上 16:00–00:00 ET 期间，``date.today()``
    返回的日期比美东日期多一天。本函数始终返回正确的美东日期。
    """
    return datetime.now(_trading_tz()).date()


def ensure_utc(dt: datetime) -> datetime:
    """确保 datetime 为 UTC tz-aware。

    - 已有 tzinfo → ``astimezone(UTC)``
    - naive → 假定为 UTC 并附加 tzinfo
    """
    if dt.tzinfo is None:
        return dt.replace(tzinfo=_UTC)
    return dt.astimezone(_UTC)


def next_trading_day(from_date: date | None = None) -> date:
    """返回下一个交易日（跳过周末，不含节假日）。"""
    from datetime import timedelta

    d = (from_date or today_trading()) + timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d


def previous_trading_day(from_date: date | None = None) -> date:
    """返回上一个交易日（跳过周末，不含节假日）。"""
    from datetime import timedelta

    d = (from_date or today_trading()) - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


# ── Market-hours helpers (always use trading.timezone) ─────


def now_market() -> datetime:
    """当前时间，始终为 trading.timezone（tz-aware）。

    所有 market_hours / schedule 时间比较应使用此函数，
    而非 ``datetime.now()`` 或 ``datetime.now(some_tz)``。
    """
    return datetime.now(_UTC).astimezone(_trading_tz())


def parse_hhmm(hhmm: str) -> time:
    """解析 ``"HH:MM"`` 字符串为 ``datetime.time``。"""
    h, m = map(int, hhmm.split(":"))
    return time(h, m)


def is_market_open() -> bool:
    """判断当前是否处于 [market_hours.start, market_hours.end]（交易日 + 交易时段）。

    始终按 ``trading.timezone`` 判断，不受服务器本地时区影响。
    """
    from shared.config import get_settings

    now = now_market()
    if now.weekday() >= 5:          # 周末
        return False

    settings = get_settings()
    start = parse_hhmm(settings.data_service.market_hours.start)
    end = parse_hhmm(settings.data_service.market_hours.end)
    now_t = now.time().replace(second=0, microsecond=0)
    return start <= now_t <= end


def before_market_open() -> bool:
    """判断当前时刻是否早于今日开盘时间（按 trading.timezone）。"""
    from shared.config import get_settings

    now = now_market()
    open_t = parse_hhmm(get_settings().data_service.market_hours.start)
    return now.time() < open_t


def after_market_close() -> bool:
    """判断当前时刻是否晚于今日收盘时间（按 trading.timezone）。

    周末视为"盘后"（返回 True）。
    """
    from shared.config import get_settings

    now = now_market()
    if now.weekday() >= 5:
        return True
    close_t = parse_hhmm(get_settings().data_service.market_hours.end)
    return now.time() > close_t


def resolve_trading_date_arg(trading_date: Any, prev_result: Any = None) -> str | None:
    """解析任务入口 trading_date 参数，兼容 Celery chain 上游结果注入。

    支持以下输入形态：
    - 直接传入 ``str`` / ``date``
    - 上游结果 ``dict`` 中的 ``trading_date`` 或 ``date`` 字段
    """

    def _extract(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, dict):
            candidate = value.get("trading_date") or value.get("date")
            if isinstance(candidate, str):
                return candidate
            if isinstance(candidate, date):
                return candidate.isoformat()
        return None

    return _extract(trading_date) or _extract(prev_result)
