from __future__ import annotations

from typing import Any


class BlueprintRuleEngine:
    def evaluate_symbol_plan(self, plan: dict[str, Any], market_ctx: dict[str, Any]) -> dict[str, Any]:
        entry_ok = self._evaluate_conditions(plan.get("entry_conditions", []), market_ctx)
        exit_ok = self._evaluate_conditions(plan.get("exit_conditions", []), market_ctx)
        action = "hold"
        if exit_ok:
            action = "exit"
        elif entry_ok:
            action = "enter"

        return {
            "symbol": plan.get("symbol"),
            "entry_ok": entry_ok,
            "exit_ok": exit_ok,
            "action": action,
        }

    def _evaluate_conditions(self, conditions: list[dict[str, Any]], market_ctx: dict[str, Any]) -> bool:
        if not conditions:
            return False
        return all(self._eval_single_condition(cond, market_ctx) for cond in conditions)

    def _eval_single_condition(self, condition: dict[str, Any], market_ctx: dict[str, Any]) -> bool:
        metric = condition.get("metric")
        operator = condition.get("operator")
        value = condition.get("value")
        current = market_ctx.get(metric)
        if current is None:
            return False

        if operator == ">":
            return current > value
        if operator == ">=":
            return current >= value
        if operator == "<":
            return current < value
        if operator == "<=":
            return current <= value
        if operator == "==":
            return current == value
        if operator == "between":
            if not isinstance(value, (list, tuple)) or len(value) != 2:
                return False
            return value[0] <= current <= value[1]
        return False
