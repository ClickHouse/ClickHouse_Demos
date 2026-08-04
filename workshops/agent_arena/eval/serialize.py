"""JSON-safe serialization of ClickHouse result sets.

Both the agent's query result (put on the LangFuse trace output) and the golden
result (put on the dataset item's expected_output) go through the SAME serializer,
so the LangFuse Code Evaluator can compare them apples-to-apples (it re-implements
the normalize() below in stdlib — see eval/langfuse_evaluators/correctness_evaluator.py).
"""
from datetime import date, datetime
from decimal import Decimal


def _cell(v):
    """One cell → a JSON primitive. Keep numbers numeric (the evaluator rounds
    floats to float_dp); stringify temporals deterministically."""
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    return str(v)


def serialize_rows(rows):
    """rows: list[tuple|list] → list[list] of JSON primitives (or None)."""
    if rows is None:
        return None
    return [[_cell(c) for c in row] for row in rows]


def result_payload(*, sql, rows, cols, error=None, outcome_hint="", transcript=None,
                   cost_usd=0.0, latency_ms=0, retries=0, tier=0):
    """Agent result plus the exact measurements needed by the leaderboard."""
    return {
        "sql": sql or "",
        "columns": list(cols) if cols else [],
        "rows": serialize_rows(rows),
        "error": error or None,
        "outcome_hint": outcome_hint or "",
        "transcript": transcript or [],
        "cost_usd": float(cost_usd),
        "latency_ms": int(latency_ms),
        "retries": int(retries),
        "tier": int(tier),
    }


def golden_payload(*, golden_sql, rows, cols, ordered):
    """The golden result set as the dataset item's expected_output."""
    return {
        "golden_sql": golden_sql or "",
        "columns": list(cols) if cols else [],
        "rows": serialize_rows(rows),
        "ordered": bool(ordered),
    }
