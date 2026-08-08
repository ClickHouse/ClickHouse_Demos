"""Langfuse code evaluator for successful live SQL execution.

This deliberately measures only whether the agent produced and executed SQL. A
valid execution can still answer the user's question incorrectly.
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class Score:
    name: str
    value: Any
    data_type: str
    comment: str | None = None


@dataclass
class EvaluationResult:
    scores: list


def evaluate(ctx) -> EvaluationResult:
    output = (ctx.observation.output or {}) if ctx.observation else {}
    sql = output.get("sql")
    successful = (
        isinstance(sql, str)
        and bool(sql.strip())
        and output.get("error") is None
        and output.get("outcome_hint") in (None, "", "ok")
    )
    return EvaluationResult(scores=[Score(
        name="sql-execution-success",
        value=successful,
        data_type="BOOLEAN",
        comment="Checks successful SQL execution, not semantic correctness.",
    )])
