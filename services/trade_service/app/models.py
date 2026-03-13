from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime


@dataclass
class ExecutionRuntimeState:
    loaded_blueprint_id: str | None = None
    loaded_trading_date: date | None = None
    status: str = "idle"
    loaded_at: datetime | None = None
    last_tick_at: datetime | None = None
    last_risk_check_at: datetime | None = None
    paused: bool = False
    manual_override_reason: str | None = None
    stoploss_cooldowns: dict[str, datetime] = field(default_factory=dict)
    stoploss_last_events: list[dict] = field(default_factory=list)


runtime_state = ExecutionRuntimeState()
