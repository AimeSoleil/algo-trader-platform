"""统一结构化日志配置

All timestamps — both structlog and stdlib (including Celery worker output) —
are rendered in the configured ``common.timezone`` (default America/New_York).
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from zoneinfo import ZoneInfo

import structlog

from shared.config.settings import get_settings


# ── Timezone-aware stdlib formatter ────────────────────────


class _TZFormatter(logging.Formatter):
    """stdlib Formatter that renders ``%(asctime)s`` in a fixed timezone."""

    def __init__(self, fmt: str | None = None, tz: ZoneInfo | None = None):
        super().__init__(fmt)
        self._tz = tz or ZoneInfo("UTC")

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:  # noqa: N802
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc).astimezone(self._tz)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat(timespec="milliseconds")


# ── Timezone-aware structlog timestamper ───────────────────


def _make_tz_timestamper(tz: ZoneInfo):
    """Return a structlog processor that stamps ISO timestamps in *tz*."""

    def _stamp(logger, method, event_dict):
        event_dict["timestamp"] = (
            datetime.now(tz=timezone.utc)
            .astimezone(tz)
            .isoformat(timespec="milliseconds")
        )
        return event_dict

    return _stamp


# ── File handler builder (reused by setup_logging + Celery hook) ──


def _build_file_handler(settings) -> logging.Handler | None:
    """Create a rotating file handler from config, or None if file logging is off."""
    if not settings.common.logging.to_file:
        return None

    log_file_path = Path(settings.common.logging.file_path)
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    mode = settings.common.logging.file_rotate_mode.lower()

    if not settings.common.logging.file_rotate and mode == "time":
        mode = "none"
    if mode not in {"time", "size", "none"}:
        mode = "time"

    if mode == "time":
        return TimedRotatingFileHandler(
            filename=log_file_path,
            when=settings.common.logging.file_rotate_when,
            interval=settings.common.logging.file_rotate_interval,
            backupCount=settings.common.logging.file_backup_count,
            encoding="utf-8",
            utc=settings.common.logging.file_rotate_utc,
        )
    if mode == "size":
        return RotatingFileHandler(
            filename=log_file_path,
            maxBytes=settings.common.logging.file_max_bytes,
            backupCount=settings.common.logging.file_backup_count,
            encoding="utf-8",
        )
    return logging.FileHandler(log_file_path, encoding="utf-8")


# ── Public API ─────────────────────────────────────────────


def setup_logging(service_name: str = "algo-trader") -> None:
    """初始化 structlog 结构化日志（FastAPI services 在 startup 中调用）"""
    settings = get_settings()
    log_level = getattr(logging, settings.common.logging.level.upper(), logging.INFO)
    lib_level = getattr(logging, settings.common.logging.lib_level.upper(), logging.WARNING)
    log_tz = ZoneInfo(settings.common.timezone)

    formatter = _TZFormatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        tz=log_tz,
    )

    handlers: list[logging.Handler] = []

    if settings.common.logging.to_console:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(formatter)
        handlers.append(h)

    file_handler = _build_file_handler(settings)
    if file_handler:
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    if not handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(formatter)
        handlers.append(h)

    # Root logger → lib_level (third-party libraries)
    logging.basicConfig(
        handlers=handlers,
        level=lib_level,
        force=True,
    )

    # Project loggers → level (more verbose than libs)
    for prefix in ("shared", "services"):
        logging.getLogger(prefix).setLevel(log_level)

    _setup_structlog(settings, log_level, log_tz)


def _setup_structlog(settings, log_level: int, log_tz: ZoneInfo) -> None:
    """Configure structlog processors and filtering level."""
    # Choose structlog renderer
    if settings.common.logging.format == "json":
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            _make_tz_timestamper(log_tz),
            structlog.processors.format_exc_info,
            structlog.processors.EventRenamer("msg"),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def setup_celery_logging(**kwargs) -> None:
    """Celery ``after_setup_logger`` signal handler.

    Celery workers have their own logging bootstrap that runs *before* any task
    module is imported.  This hook injects our timezone-aware formatter and
    the configured file handler into Celery's root logger so that:
      1. Console output uses the trading timezone.
      2. Worker logs are also written to the same rotating log file.

    The effective log level is the **lower** (more verbose) of Celery's
    ``--loglevel`` CLI flag and the ``logging.level`` in config.yaml, so
    that ``--loglevel=DEBUG`` always works even if the config says INFO.
    """
    settings = get_settings()
    log_tz = ZoneInfo(settings.common.timezone)
    formatter = _TZFormatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        tz=log_tz,
    )

    # Celery passes its --loglevel as the ``loglevel`` kwarg (int).
    # Honour whichever is more verbose (lower numeric value).
    celery_level: int | None = kwargs.get("loglevel")
    config_level = getattr(logging, settings.common.logging.level.upper(), logging.INFO)
    lib_level = getattr(logging, settings.common.logging.lib_level.upper(), logging.WARNING)
    effective_level = min(celery_level, config_level) if celery_level is not None else config_level
    effective_lib_level = min(celery_level, lib_level) if celery_level is not None else lib_level

    root = logging.getLogger()
    root.setLevel(effective_lib_level)

    # Re-format existing handlers (console) with TZ-aware formatter
    for h in root.handlers:
        h.setFormatter(formatter)
        h.setLevel(effective_lib_level)

    # Project loggers → project-level (more verbose than libs)
    for prefix in ("shared", "services"):
        logging.getLogger(prefix).setLevel(effective_level)

    # Add the file handler if not already present
    file_handler = _build_file_handler(settings)
    if file_handler:
        file_handler.setFormatter(formatter)
        file_handler.setLevel(effective_lib_level)
        root.addHandler(file_handler)

    # Also setup structlog for task code that uses get_logger()
    _setup_structlog(settings, effective_level, log_tz)


def get_logger(name: str = __name__) -> structlog.BoundLogger:
    """获取带服务名的 logger"""
    return structlog.get_logger(service=name)
