from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass
class ExecutionRuntimeState:
    loaded_blueprint_id: str | None = None
    loaded_trading_date: date | None = None
    status: str = "idle"
    loaded_at: datetime | None = None
    last_tick_at: datetime | None = None
    paused: bool = False
    manual_override_reason: str | None = None


runtime_state = ExecutionRuntimeState()
