from types import SimpleNamespace

import pytest

from agents.llm import Usage
from agents.loop import AgentResult
from eval.golden import GoldenQuestion
import eval.harness as harness


def test_harness_defaults_to_current_policy():
    args = harness.parse_args([])
    assert args.policy_version == "policy-v2"


def test_effective_run_id_includes_policy_without_changing_config_id():
    assert harness.effective_run_id("online-loop-baseline", "policy-v1") == \
        "online-loop-baseline--policy-v1"


def test_main_orchestrates_selected_policy_and_release_provenance(monkeypatch):
    model = SimpleNamespace(
        id="provider/model", name="model-a",
        price_per_1m_in=1.0, price_per_1m_out=2.0,
    )
    prompt = SimpleNamespace(name="prompt-a", k=0)
    cfg = SimpleNamespace(
        langfuse=object(), clickhouse=object(),
        openrouter=SimpleNamespace(base_url="https://example.invalid", api_key="unused",
                                    inference={}),
        eval=SimpleNamespace(float_dp=2, default_max_retries=1),
        models=[model],
        resolved_grid=lambda: ([model.name], [prompt.name]),
        model_by_name=lambda name: model,
        prompt_by_name=lambda name: prompt,
    )
    question = GoldenQuestion(
        id="q1", tier=1, question="How many?", ordered=False,
        golden_sql="SELECT count() FROM v_orders",
    )
    agent_calls = []
    trace_calls = []
    experiment_calls = []

    class FakeRO:
        def query(self, sql):
            return SimpleNamespace(rows=[[1]], cols=["count()"])

    class FakeTracer:
        def ensure_dataset(self, items):
            pass

        def run_experiment(self, **kwargs):
            experiment_calls.append(kwargs)
            kwargs["task"](item={"id": question.id})

        def flush(self):
            pass

    def fake_run_agent(*args, **kwargs):
        agent_calls.append((args, kwargs))
        return AgentResult(
            sql="SELECT 1", rows=[[1]], cols=["n"], error=None,
            attempts=1, usage=Usage(10, 3), outcome_hint="ok", transcript=[],
        )

    monkeypatch.setattr(harness, "load_config", lambda: cfg)
    monkeypatch.setattr(harness, "LangfuseTracer", lambda unused: FakeTracer())
    monkeypatch.setattr(harness, "ROClickHouseClient", lambda unused: FakeRO())
    monkeypatch.setattr(harness, "OpenRouterClient", lambda *args: object())
    monkeypatch.setattr(harness, "load_golden", lambda: [question])
    monkeypatch.setattr(harness, "run_agent", fake_run_agent)
    monkeypatch.setattr(harness, "emit_agent_trace",
                        lambda **kwargs: trace_calls.append(kwargs) or "trace-1")
    monkeypatch.setattr(harness, "_wait_for_scores", lambda *args: None)
    monkeypatch.setattr("agents.llm.fetch_openrouter_prices", lambda unused: {})
    monkeypatch.setattr("agents.llm.apply_live_prices", lambda models, prices: models)

    harness.main(["--run-id", "online-loop-baseline",
                  "--policy-version", "policy-v1"])

    effective = "online-loop-baseline--policy-v1"
    config_id = "model-a__prompt-a"
    assert "Business metric policy (policy-v1)" in agent_calls[0][0][3]
    assert experiment_calls[0]["config_id"] == f"{effective}__{config_id}"
    assert trace_calls[0]["session_id"] == f"{effective}__{config_id}"
    assert trace_calls[0]["tags"] == [
        config_id, f"run:{effective}", "policy-v1", "model-a", "prompt-a",
    ]
    assert trace_calls[0]["metadata"] == {
        "config_id": config_id, "question_id": "q1",
        "model": "model-a", "prompt": "prompt-a", "run_id": effective,
        "policy_version": "policy-v1", "release": "online-loop-baseline",
    }


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
    with pytest.raises(RuntimeError, match="agent-arena-llm-judge"):
        harness._wait_for_scores(tracer, ["t1"], timeout=1, poll=0)


def test_wait_for_scores_accepts_both_required_scores():
    tracer = SimpleNamespace(fetch_trace_scores=lambda tid: [
        {"name": "correctness", "value": 1, "string": None},
        {"name": "agent-arena-llm-judge", "value": 0.8, "string": None},
    ])
    harness._wait_for_scores(tracer, ["t1"], timeout=1, poll=0)


def test_default_score_names_match_live_rule_scores_exactly():
    assert harness.DEFAULT_REQUIRED_SCORE_NAMES == (
        "correctness",
        "agent-arena-llm-judge",
    )


def test_stale_evaluator_name_does_not_satisfy_live_rule_score(monkeypatch):
    tracer = SimpleNamespace(fetch_trace_scores=lambda tid: [
        {"name": "correctness", "value": 1, "string": None},
        {"name": "llm_judge", "value": 0.8, "string": None},
    ])
    clock = iter([0, 0, 2, 2])
    monkeypatch.setattr(harness.time, "time", lambda: next(clock, 2))
    monkeypatch.setattr(harness.time, "sleep", lambda _: None)

    with pytest.raises(RuntimeError, match="agent-arena-llm-judge"):
        harness._wait_for_scores(tracer, ["t1"], timeout=1, poll=0)


def test_wait_for_scores_requires_each_repeatable_exact_score_name(monkeypatch):
    tracer = SimpleNamespace(fetch_trace_scores=lambda tid: [
        {"name": "correctness", "value": 1, "string": None},
        {"name": "agent-arena-llm-judge", "value": 0.8, "string": None},
        {"name": "business-policy-adherence-extra", "value": None,
         "string": "PASS"},
    ])
    clock = iter([0, 0, 2, 2])
    monkeypatch.setattr(harness.time, "time", lambda: next(clock, 2))
    monkeypatch.setattr(harness.time, "sleep", lambda _: None)

    with pytest.raises(RuntimeError, match="business-policy-adherence"):
        harness._wait_for_required_scores(
            tracer,
            ["t1"],
            timeout=1,
            poll=0,
            required_names=["business-policy-adherence"],
        )


def test_wait_for_scores_accepts_categorical_string_value_by_exact_name():
    tracer = SimpleNamespace(fetch_trace_scores=lambda tid: [
        {"name": "correctness", "value": 1, "string": None},
        {"name": "agent-arena-llm-judge", "value": 0.8, "string": None},
        {"name": "business-policy-adherence", "value": None,
         "string": "PASS"},
    ])

    harness._wait_for_required_scores(
        tracer,
        ["t1"],
        timeout=1,
        poll=0,
        required_names=["business-policy-adherence"],
    )


def test_parse_args_collects_repeatable_wait_for_score_names():
    args = harness.parse_args([
        "--wait-for-score", "business-policy-adherence",
        "--wait-for-score", "safety-policy",
    ])

    assert args.wait_for_score == ["business-policy-adherence", "safety-policy"]
