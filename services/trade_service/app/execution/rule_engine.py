from __future__ import annotations

from typing import Any

from shared.utils import get_logger

logger = get_logger("execution_rule_engine")


class BlueprintRuleEngine:
    def evaluate_symbol_plan(self, plan: dict[str, Any], market_ctx: dict[str, Any]) -> dict[str, Any]:
        symbol = plan.get("symbol")
        logger.debug(
            "rule_engine.evaluate_started",
            log_event="evaluate_symbol_plan",
            stage="start",
            symbol=symbol,
            entry_conditions=len(plan.get("entry_conditions", [])),
            exit_conditions=len(plan.get("exit_conditions", [])),
        )
        entry_ok = self._evaluate_conditions(plan.get("entry_conditions", []), market_ctx)
        exit_ok = self._evaluate_conditions(plan.get("exit_conditions", []), market_ctx)
        action = "hold"
        if exit_ok:
            action = "exit"
        elif entry_ok:
            action = "enter"

        logger.debug(
            "rule_engine.evaluate_completed",
            log_event="evaluate_symbol_plan",
            stage="completed",
            symbol=symbol,
            entry_ok=entry_ok,
            exit_ok=exit_ok,
            action=action,
        )

        return {
            "symbol": plan.get("symbol"),
            "entry_ok": entry_ok,
            "exit_ok": exit_ok,
            "action": action,
        }

    def _evaluate_conditions(self, conditions: list[dict[str, Any]], market_ctx: dict[str, Any]) -> bool:
        if not conditions:
            logger.debug(
                "rule_engine.conditions_empty",
                log_event="evaluate_conditions",
                stage="short_circuit",
            )
            return False
        return all(self._eval_single_condition(cond, market_ctx) for cond in conditions)

    def _eval_single_condition(self, condition: dict[str, Any], market_ctx: dict[str, Any]) -> bool:
        field = condition.get("field")
        operator = condition.get("operator")
        value = condition.get("value")
        current = market_ctx.get(field)
        if current is None:
            logger.debug(
                "rule_engine.field_missing",
                log_event="eval_single_condition",
                stage="missing_field",
                field=field,
                operator=operator,
            )
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
                logger.debug(
                    "rule_engine.between_value_invalid",
                    log_event="eval_single_condition",
                    stage="invalid_value",
                    field=field,
                    operator=operator,
                )
                return False
            return value[0] <= current <= value[1]
        logger.debug(
            "rule_engine.operator_unsupported",
            log_event="eval_single_condition",
            stage="unsupported_operator",
            field=field,
            operator=operator,
        )
        return False
