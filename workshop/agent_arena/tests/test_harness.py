from types import SimpleNamespace

import pytest

import eval.harness as harness


def test_question_for_promoted_dataset_item_not_in_yaml():
    item = SimpleNamespace(
        id="q101", input={"question": "Promoted question?"},
        metadata={"tier": 3, "ordered": True,
                  "golden_sql": "SELECT count() FROM v_orders"},
        expected_output={"golden_sql": "SELECT count() FROM v_orders"},
    )
    question = harness._question_for_item(item, {})
    assert question.id == "q101"
    assert question.question == "Promoted question?"
    assert question.tier == 3
    assert question.ordered is True


def test_question_prefers_local_definition():
    local = SimpleNamespace(id="q1", question="Local")
    assert harness._question_for_item(SimpleNamespace(id="q1"), {"q1": local}) is local


def test_wait_for_scores_requires_correctness_and_judge(monkeypatch):
    tracer = SimpleNamespace(fetch_trace_scores=lambda tid: [
        {"name": "correctness", "value": 1, "string": None}
    ])
    clock = iter([0, 0, 2, 2])
    monkeypatch.setattr(harness.time, "time", lambda: next(clock, 2))
    monkeypatch.setattr(harness.time, "sleep", lambda _: None)
    with pytest.raises(RuntimeError, match="llm_judge"):
        harness._wait_for_scores(tracer, ["t1"], timeout=1, poll=0)


def test_wait_for_scores_accepts_both_required_scores():
    tracer = SimpleNamespace(fetch_trace_scores=lambda tid: [
        {"name": "correctness", "value": 1, "string": None},
        {"name": "llm_judge", "value": 0.8, "string": None},
    ])
    harness._wait_for_scores(tracer, ["t1"], timeout=1, poll=0)
