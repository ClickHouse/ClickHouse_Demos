from decimal import Decimal
from eval.grading import normalize, grade, classify_outcome
from agents.loop import AgentResult
from agents.llm import ZeroUsage


def _agent(rows, cols, error=None, hint="ok"):
    return AgentResult(sql="SELECT 1", rows=rows, cols=cols, error=error,
                       attempts=1, usage=ZeroUsage(), outcome_hint=hint)


def test_normalize_rounds_and_sentinels():
    rows = [(1, 2.0 / 3.0, None), (2, Decimal("1.50000"), "x")]
    out = normalize(rows, ordered=False, dp=4)
    assert ("1", "0.6667", "∅") in out
    assert ("2", "1.5000", "x") in out


def test_unordered_equal_despite_row_order():
    a = _agent([(1, "a"), (2, "b")], ["x", "y"])
    assert grade(a, golden_rows=[(2, "b"), (1, "a")], golden_cols=["x", "y"],
                 ordered=False) == 1


def test_ordered_penalizes_wrong_order():
    a = _agent([(2,), (1,)], ["n"])
    assert grade(a, golden_rows=[(1,), (2,)], golden_cols=["n"], ordered=True) == 0


def test_column_count_mismatch_scores_zero():
    a = _agent([(1, 2)], ["x", "y"])
    assert grade(a, golden_rows=[(1,)], golden_cols=["x"], ordered=False) == 0


def test_compare_by_position_not_name():
    a = _agent([(5,)], ["revenue"])
    assert grade(a, golden_rows=[(5,)], golden_cols=["total"], ordered=False) == 1


def test_float_equivalence_within_dp():
    a = _agent([(1.23456,)], ["v"])
    assert grade(a, golden_rows=[(1.23457,)], golden_cols=["v"], ordered=False) == 1


def test_both_empty_scores_one():
    a = _agent([], ["x"])
    assert grade(a, golden_rows=[], golden_cols=["x"], ordered=False) == 1


def test_error_scores_zero():
    a = _agent(None, None, error="boom", hint="sql_exec_error")
    assert grade(a, golden_rows=[(1,)], golden_cols=["x"], ordered=False) == 0


def test_keeps_duplicate_rows():
    a = _agent([(1,), (1,)], ["x"])
    assert grade(a, golden_rows=[(1,)], golden_cols=["x"], ordered=False) == 0


def test_outcome_taxonomy():
    assert classify_outcome(_agent(None, None, "e", "sql_policy_rejected"),
                            golden_rows=[(1,)], score=0) == "sql_policy_rejected"
    assert classify_outcome(_agent([(1,)], ["x"]), golden_rows=[(1,)], score=1) == "correct"
    assert classify_outcome(_agent([(9,)], ["x"]), golden_rows=[(1,)], score=0) == "wrong_result"
    assert classify_outcome(_agent([], ["x"]), golden_rows=[(1,)], score=0) == "empty_but_expected"
