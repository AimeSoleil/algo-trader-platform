"""统一结构化日志配置"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler

import structlog

from shared.config.settings import get_settings


def setup_logging(service_name: str = "algo-trader") -> None:
    """初始化 structlog 结构化日志"""
    settings = get_settings()
    log_level = getattr(logging, settings.logging.level.upper(), logging.INFO)

    handlers: list[logging.Handler] = []

    if settings.logging.to_console:
        handlers.append(logging.StreamHandler(sys.stdout))

    if settings.logging.to_file:
        log_file_path = Path(settings.logging.file_path)
        log_file_path.parent.mkdir(parents=True, exist_ok=True)
        mode = settings.logging.file_rotate_mode.lower()

        if not settings.logging.file_rotate and mode == "time":
            mode = "none"

        if mode not in {"time", "size", "none"}:
            mode = "time"

        if mode == "time":
            handlers.append(
                TimedRotatingFileHandler(
                    filename=log_file_path,
                    when=settings.logging.file_rotate_when,
                    interval=settings.logging.file_rotate_interval,
                    backupCount=settings.logging.file_backup_count,
                    encoding="utf-8",
                    utc=settings.logging.file_rotate_utc,
                )
            )
        elif mode == "size":
            handlers.append(
                RotatingFileHandler(
                    filename=log_file_path,
                    maxBytes=settings.logging.file_max_bytes,
                    backupCount=settings.logging.file_backup_count,
                    encoding="utf-8",
                )
            )
        else:
            handlers.append(logging.FileHandler(log_file_path, encoding="utf-8"))

    if not handlers:
        handlers.append(logging.StreamHandler(sys.stdout))

    # Configure stdlib logging
    logging.basicConfig(
        format="%(message)s",
        handlers=handlers,
        level=log_level,
        force=True,
    )

    # Choose renderer based on config
    if settings.logging.format == "json":
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.format_exc_info,
            structlog.processors.EventRenamer("msg"),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str = __name__) -> structlog.BoundLogger:
    """获取带服务名的 logger"""
    return structlog.get_logger(service=name)
