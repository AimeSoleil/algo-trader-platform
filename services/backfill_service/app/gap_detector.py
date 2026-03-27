"""数据缺口检测器 — 覆盖 4 张核心表

Tables checked:
  - stock_1min_bars        : 盘后采集的当天 1 分钟 K 线
  - stock_daily            : 日线数据
  - option_daily           : 盘后期权链快照
  - option_5min_snapshots  : 盘中 5 分钟期权链快照（intraday 模式）
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import text

from shared.db.session import get_timescale_session
from shared.utils import get_logger, market_tz, parse_hhmm
from shared.config import get_settings

logger = get_logger("gap_detector")


def _market_open() -> time:
    return parse_hhmm(get_settings().common.market_hours.start)


def _market_close() -> time:
    return parse_hhmm(get_settings().common.market_hours.end)


# ── 辅助函数 ───────────────────────────────────────────────


def _expected_intraday_timestamps(
    trading_date: date,
    interval_minutes: int,
) -> list[datetime]:
    """生成某交易日预期的盘中时间戳序列（UTC，与 DB TIMESTAMPTZ 对齐）"""
    tz = market_tz()
    timestamps: list[datetime] = []
    current = datetime.combine(trading_date, _market_open(), tzinfo=tz)
    end = datetime.combine(trading_date, _market_close(), tzinfo=tz)
    delta = timedelta(minutes=interval_minutes)
    while current <= end:
        timestamps.append(current.astimezone(ZoneInfo("UTC")))
        current += delta
    return timestamps


def _trading_dates_between(start: date, end: date) -> list[date]:
    """生成 [start, end] 范围内的交易日列表（排除周末，不含节假日）"""
    dates: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # Mon-Fri
            dates.append(current)
        current += timedelta(days=1)
    return dates


# ── stock_1min_bars 缺口检测 ───────────────────────────────


async def detect_stock_1min_gaps(symbol: str, trading_date: date) -> list[dict]:
    """检测当天 stock_1min_bars 的 1 分钟数据缺口"""
    async with get_timescale_session() as session:
        result = await session.execute(
            text(
                "SELECT timestamp FROM stock_1min_bars "
                "WHERE symbol = :symbol AND timestamp::date = :date "
                "ORDER BY timestamp"
            ),
            {"symbol": symbol, "date": trading_date},
        )
        actual = {row[0] for row in result.fetchall()}

    expected = _expected_intraday_timestamps(trading_date, interval_minutes=1)
    missing = [ts for ts in expected if ts not in actual]
    logger.debug(
        "gap_detector.stock_1min_summary",
        symbol=symbol,
        date=str(trading_date),
        expected_count=len(expected),
        actual_count=len(actual),
        missing_count=len(missing),
    )

    if missing:
        logger.warning(
            "gap_detector.stock_1min_gaps",
            symbol=symbol,
            date=str(trading_date),
            gaps=len(missing),
        )
    return [{"symbol": symbol, "timestamp": ts, "table": "stock_1min_bars"} for ts in missing]


# ── stock_daily 缺口检测 ───────────────────────────────────


async def detect_stock_daily_gaps(
    symbol: str,
    start_date: date,
    end_date: date,
) -> list[date]:
    """检测 stock_daily 表中缺失的交易日"""
    async with get_timescale_session() as session:
        result = await session.execute(
            text(
                "SELECT trading_date FROM stock_daily "
                "WHERE symbol = :symbol "
                "AND trading_date BETWEEN :start AND :end "
                "ORDER BY trading_date"
            ),
            {"symbol": symbol, "start": start_date, "end": end_date},
        )
        actual = {row[0] for row in result.fetchall()}

    expected = _trading_dates_between(start_date, end_date)
    missing = [d for d in expected if d not in actual]
    logger.debug(
        "gap_detector.stock_daily_summary",
        symbol=symbol,
        start=str(start_date),
        end=str(end_date),
        expected_count=len(expected),
        actual_count=len(actual),
        missing_count=len(missing),
    )

    if missing:
        logger.warning(
            "gap_detector.stock_daily_gaps",
            symbol=symbol,
            start=str(start_date),
            end=str(end_date),
            gaps=len(missing),
        )
    return missing


# ── option_daily 缺口检测 ──────────────────────────────────


async def detect_option_daily_gaps(
    symbol: str,
    start_date: date,
    end_date: date,
) -> list[date]:
    """检测 option_daily 表中缺失的快照日（不可回填，仅记录）"""
    async with get_timescale_session() as session:
        result = await session.execute(
            text(
                "SELECT DISTINCT snapshot_date FROM option_daily "
                "WHERE underlying = :symbol "
                "AND snapshot_date BETWEEN :start AND :end "
                "ORDER BY snapshot_date"
            ),
            {"symbol": symbol, "start": start_date, "end": end_date},
        )
        actual = {row[0] for row in result.fetchall()}

    expected = _trading_dates_between(start_date, end_date)
    missing = [d for d in expected if d not in actual]
    logger.debug(
        "gap_detector.option_daily_summary",
        symbol=symbol,
        start=str(start_date),
        end=str(end_date),
        expected_count=len(expected),
        actual_count=len(actual),
        missing_count=len(missing),
    )

    if missing:
        logger.warning(
            "gap_detector.option_daily_gaps",
            symbol=symbol,
            start=str(start_date),
            end=str(end_date),
            gaps=len(missing),
            note="not_fillable",
        )
    return missing


# ── option_5min_snapshots 缺口检测 ─────────────────────────


async def detect_option_5min_gaps(symbol: str, trading_date: date) -> list[dict]:
    """检测盘中期权 5 分钟快照缺口（不可回填，仅记录）"""
    async with get_timescale_session() as session:
        result = await session.execute(
            text(
                "SELECT DISTINCT timestamp FROM option_5min_snapshots "
                "WHERE underlying = :symbol AND timestamp::date = :date "
                "ORDER BY timestamp"
            ),
            {"symbol": symbol, "date": trading_date},
        )
        actual = {row[0] for row in result.fetchall()}

    expected = _expected_intraday_timestamps(trading_date, interval_minutes=5)
    missing = [ts for ts in expected if ts not in actual]
    logger.debug(
        "gap_detector.option_5min_summary",
        symbol=symbol,
        date=str(trading_date),
        expected_count=len(expected),
        actual_count=len(actual),
        missing_count=len(missing),
    )

    if missing:
        logger.warning(
            "gap_detector.option_5min_gaps",
            symbol=symbol,
            date=str(trading_date),
            gaps=len(missing),
            note="not_fillable",
        )
    return [{"symbol": symbol, "timestamp": ts, "table": "option_5min_snapshots"} for ts in missing]
