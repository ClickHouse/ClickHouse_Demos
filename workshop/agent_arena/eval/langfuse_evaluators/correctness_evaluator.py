# AgentArena — LangFuse server-side CODE EVALUATOR: "correctness"
# ---------------------------------------------------------------------------
# Paste this into Langfuse → Evaluators → "+ Set up Evaluator" → Code, target
# = Experiments, filter to the `arena-golden` dataset.
#
# It grades NL→SQL by EXECUTION ACCURACY: it compares the agent's query RESULT
# SET (on the trace output) against the GOLDEN result set (the dataset item's
# expected_output). The queries themselves run in the agent (Langfuse code
# evaluators have NO network egress, 2s limit, stdlib only) — this evaluator
# makes the grading *decision* inside Langfuse.
#
# Mirrors eval/grading.py + eval/serialize.py. stdlib only.

from dataclasses import dataclass
from typing import Any


@dataclass
class Score:
    name: str
    value: Any
    data_type: str           # NUMERIC | CATEGORICAL | BOOLEAN | TEXT
    comment: str | None = None


@dataclass
class EvaluationResult:
    scores: list


def _normalize(rows, ordered, dp):
    out = []
    for r in rows:
        cells = []
        for v in r:
            if v is None:
                cells.append("∅")
            elif isinstance(v, bool):
                cells.append("1" if v else "0")
            elif isinstance(v, float):
                cells.append(f"{v:.{dp}f}")
            elif isinstance(v, int):
                cells.append(str(v))
            else:
                cells.append(str(v))
        out.append(tuple(cells))
    return out if ordered else sorted(out)


def evaluate(ctx) -> EvaluationResult:
    out = (ctx.observation.output or {}) if ctx.observation else {}
    exp = ctx.experiment
    expected = (exp.item_expected_output or {}) if exp else {}
    meta = (exp.item_metadata or {}) if exp else {}

    agent_rows = out.get("rows")
    agent_cols = out.get("columns") or []
    error = out.get("error")
    hint = out.get("outcome_hint") or ""

    golden_rows = expected.get("rows")
    golden_cols = expected.get("columns") or []
    ordered = bool(meta.get("ordered", expected.get("ordered", False)))
    dp = int(meta.get("float_dp", 4))

    # --- correctness (mirrors eval/grading.grade) ---
    if error or agent_rows is None:
        score = 0
    elif not agent_rows and not golden_rows:
        score = 1
    elif agent_rows and golden_rows and len(agent_cols) != len(golden_cols):
        score = 0
    else:
        score = int(_normalize(agent_rows, ordered, dp) == _normalize(golden_rows or [], ordered, dp))

    # --- outcome (mirrors eval/grading.classify_outcome) ---
    if score == 1:
        outcome = "correct"
    elif hint == "model_error":
        outcome = "model_error"
    elif hint == "sql_policy_rejected":
        outcome = "sql_policy_rejected"
    elif hint == "sql_exec_error" or error:
        outcome = "sql_exec_error"
    elif agent_rows is not None and len(agent_rows) == 0 and golden_rows:
        outcome = "empty_but_expected"
    else:
        outcome = "wrong_result"

    comment = (f"{'match' if score else 'mismatch'} · "
               f"agent {0 if agent_rows is None else len(agent_rows)} row(s) vs "
               f"golden {0 if golden_rows is None else len(golden_rows)} · "
               f"ordered={ordered} · {outcome}")

    return EvaluationResult(scores=[
        Score(name="correctness", value=float(score), data_type="NUMERIC", comment=comment),
        Score(name="outcome", value=outcome, data_type="CATEGORICAL", comment=comment),
    ])
