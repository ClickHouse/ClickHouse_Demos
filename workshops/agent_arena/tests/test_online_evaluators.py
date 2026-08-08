from types import SimpleNamespace

import pytest

from eval.langfuse_evaluators.sql_execution_success import evaluate


def _context(output):
    return SimpleNamespace(observation=SimpleNamespace(output=output))


def test_sql_execution_success_scores_valid_root_output():
    ctx = _context({
        "sql": "SELECT 1",
        "rows": [[1]],
        "error": None,
        "outcome_hint": "ok",
    })

    score = evaluate(ctx).scores[0]

    assert (score.name, score.value, score.data_type) == (
        "sql-execution-success",
        True,
        "BOOLEAN",
    )
    assert "not semantic correctness" in score.comment


@pytest.mark.parametrize(
    "output",
    [
        {"sql": "", "rows": [], "error": None, "outcome_hint": "ok"},
        {"sql": None, "rows": [], "error": None, "outcome_hint": "ok"},
        {"sql": "SELECT 1", "rows": [], "error": "query failed", "outcome_hint": "ok"},
        {"sql": "SELECT 1", "rows": [], "error": None, "outcome_hint": "wrong_result"},
    ],
)
def test_sql_execution_success_rejects_non_executed_outputs(output):
    score = evaluate(_context(output)).scores[0]

    assert (score.name, score.value, score.data_type) == (
        "sql-execution-success",
        False,
        "BOOLEAN",
    )


@pytest.mark.parametrize("outcome_hint", [None, "", "ok"])
def test_sql_execution_success_accepts_execution_hints(outcome_hint):
    output = {
        "sql": " SELECT 1 ",
        "rows": [],
        "error": None,
        "outcome_hint": outcome_hint,
    }

    assert evaluate(_context(output)).scores[0].value is True
