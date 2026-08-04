"""Behaviour tests for the Langfuse-backed leaderboard read model."""

import dashboard.app as dashboard
from dashboard.langfuse_results import fetch_experiment_rows


def test_normalizes_experiment_item_into_leaderboard_question():
    """Dropping any Langfuse field mapping must corrupt this public row contract."""
    item = {
        "id": "item-1",
        "traceId": "trace-1",
        "startTime": "2026-08-03T01:00:00Z",
        "endTime": "2026-08-03T01:00:02Z",
        "level": "DEFAULT",
        "environment": "default",
        "experimentId": "experiment-1",
        "experimentName": "sonnet__P1",
        "experimentItemId": "run-item-1",
        "experimentDatasetId": "dataset-1",
        "experimentItemVersion": "2026-08-03T00:00:00Z",
        "input": {"question": "How many orders?"},
        "output": {
            "sql": "SELECT count() FROM v_orders",
            "columns": ["count()"],
            "rows": [[42]],
            "error": None,
            "outcome_hint": "ok",
            "transcript": [{"role": "user", "content": "How many orders?"}],
            "cost_usd": 0.0123,
            "latency_ms": 1875,
            "retries": 1,
            "tier": 2,
        },
        "expectedOutput": {"rows": [[42]]},
        "metadata": {
            "runid": "run-aug03",
            "configid": "sonnet__P1",
            "questionid": "q001",
            "model": "sonnet",
            "prompt": "P1",
        },
        "experimentItemMetadata": {"tier": 2, "ordered": False},
        "experimentMetadata": None,
        "experimentDescription": "run run-aug03",
        "scores": [
            {"id": "s1", "projectId": "p1", "name": "correctness", "value": 1,
             "dataType": "NUMERIC", "source": "EVAL", "timestamp": "2026-08-03T01:01:00Z",
             "environment": "default", "createdAt": "2026-08-03T01:01:00Z",
             "updatedAt": "2026-08-03T01:01:00Z"},
            {"id": "s2", "projectId": "p1", "name": "llm_judge", "value": 0.8,
             "dataType": "NUMERIC", "source": "EVAL", "timestamp": "2026-08-03T01:01:00Z",
             "environment": "default", "createdAt": "2026-08-03T01:01:00Z",
             "updatedAt": "2026-08-03T01:01:00Z"},
            {"id": "s3", "projectId": "p1", "name": "outcome", "value": "correct",
             "dataType": "CATEGORICAL", "source": "EVAL", "timestamp": "2026-08-03T01:01:00Z",
             "environment": "default", "createdAt": "2026-08-03T01:01:00Z",
             "updatedAt": "2026-08-03T01:01:00Z"},
        ],
    }

    normalize = getattr(dashboard, "_normalize_experiment_item", None)
    assert normalize is not None, "dashboard must normalize Langfuse experiment items"
    assert normalize(item, "https://lf.example/project/p1") == {
        "run_id": "run-aug03",
        "config_id": "sonnet__P1",
        "model_name": "sonnet",
        "prompt_name": "P1",
        "question_id": "q001",
        "tier": 2,
        "correctness": 1,
        "judge_score": 0.8,
        "cost_usd": 0.0123,
        "latency_ms": 1875,
        "retries": 1,
        "outcome": "correct",
        "sql": "SELECT count() FROM v_orders",
        "trace_id": "trace-1",
        "trace_url": "https://lf.example/project/p1/traces/trace-1",
        "session_id": "run-aug03__sonnet__P1",
        "session_url": "https://lf.example/project/p1/sessions/run-aug03__sonnet__P1",
        "question": "How many orders?",
        "transcript": [{"role": "user", "content": "How many orders?"}],
        "started_at": "2026-08-03T01:00:00Z",
    }


def test_builds_leaderboard_aggregates_from_question_rows():
    """Wrong grouping or denominator must change the visible contest ranking."""
    rows = [
        {"run_id": "r1", "config_id": "m1__p1", "model_name": "m1",
         "prompt_name": "p1", "question_id": "q1", "tier": 1,
         "correctness": 1, "judge_score": 0.8, "cost_usd": 0.03,
         "latency_ms": 1000, "outcome": "correct", "started_at": "2026-08-03T01:00:00Z"},
        {"run_id": "r1", "config_id": "m1__p1", "model_name": "m1",
         "prompt_name": "p1", "question_id": "q2", "tier": 2,
         "correctness": 0, "judge_score": 0.4, "cost_usd": 0.01,
         "latency_ms": 3000, "outcome": "wrong_result", "started_at": "2026-08-03T01:00:01Z"},
        {"run_id": "r2", "config_id": "m2__p2", "model_name": "m2",
         "prompt_name": "p2", "question_id": "q1", "tier": 1,
         "correctness": 1, "judge_score": 1.0, "cost_usd": 0.02,
         "latency_ms": 500, "outcome": "correct", "started_at": "2026-08-03T02:00:00Z"},
    ]

    build = getattr(dashboard, "_build_read_model", None)
    assert build is not None, "dashboard must aggregate normalized Langfuse rows"
    model = build(rows)
    assert model["runs"] == ["r2", "r1"]
    assert model["leaderboard"]["r1"] == [{
        "config_id": "m1__p1",
        "model_name": "m1",
        "prompt_name": "p1",
        "n_questions": 2,
        "accuracy": 0.5,
        "n_correct": 1,
        "avg_judge_score": 0.6,
        "total_cost_usd": 0.04,
        "avg_latency_ms": 2000.0,
        "cost_per_correct_answer": 0.04,
    }]


def test_builds_tier_outcome_and_question_drilldowns():
    """Losing a grouping dimension must break the dashboard analysis views."""
    rows = [
        {"run_id": "r1", "config_id": "m1__p1", "model_name": "m1",
         "prompt_name": "p1", "question_id": "q2", "tier": 2,
         "correctness": 0, "judge_score": 0.4, "cost_usd": 0.01,
         "latency_ms": 3000, "outcome": "wrong_result", "sql": "SELECT 2",
         "trace_id": "t2", "trace_url": "https://lf/traces/t2",
         "session_url": "https://lf/sessions/s", "started_at": "2026-08-03T01:00:01Z"},
        {"run_id": "r1", "config_id": "m1__p1", "model_name": "m1",
         "prompt_name": "p1", "question_id": "q1", "tier": 2,
         "correctness": 1, "judge_score": 0.8, "cost_usd": 0.03,
         "latency_ms": 1000, "outcome": "correct", "sql": "SELECT 1",
         "trace_id": "t1", "trace_url": "https://lf/traces/t1",
         "session_url": "https://lf/sessions/s", "started_at": "2026-08-03T01:00:00Z"},
    ]

    model = dashboard._build_read_model(rows)
    assert model["tiers"]["r1"] == [{"config_id": "m1__p1", "tier": 2, "accuracy": 0.5}]
    assert model["outcomes"]["r1"] == [
        {"config_id": "m1__p1", "outcome": "correct", "n": 1},
        {"config_id": "m1__p1", "outcome": "wrong_result", "n": 1},
    ]
    assert [r["question_id"] for r in model["questions"][("r1", "m1__p1")]] == ["q1", "q2"]


def test_fetches_every_experiment_item_page_and_ignores_other_traces():
    """Stopping at page one or accepting unrelated traces must change visible runs."""
    def item(run_id, trace_id):
        return {
            "id": f"item-{trace_id}", "traceId": trace_id,
            "startTime": "2026-08-03T01:00:00Z", "endTime": "2026-08-03T01:00:01Z",
            "level": "DEFAULT", "environment": "default", "experimentId": "e1",
            "experimentName": "m__p", "experimentItemId": f"ri-{trace_id}",
            "input": {"question": "q"},
            "output": {"sql": "SELECT 1", "cost_usd": 0.01, "latency_ms": 1000,
                       "retries": 0, "tier": 1},
            "metadata": {"runid": run_id, "configid": "m__p", "questionid": trace_id,
                         "model": "m", "prompt": "p"},
            "experimentItemMetadata": {"tier": 1}, "scores": [],
        }

    calls = []

    def get_page(params):
        calls.append(dict(params))
        if "cursor" not in params:
            return {"data": [item("r1", "t1"), {"id": "unrelated"}],
                    "meta": {"cursor": "next-page"}}
        return {"data": [item("r2", "t2")], "meta": {"cursor": None}}

    fetch = getattr(dashboard, "_fetch_experiment_rows", None)
    assert fetch is not None, "dashboard must page through the Experiment Items API"
    rows = fetch(get_page, "https://lf.example/project/p1")
    assert [r["trace_id"] for r in rows] == ["t1", "t2"]
    assert len(calls) == 2
    assert calls[1]["cursor"] == "next-page"


def test_dashboard_endpoints_read_the_langfuse_model(monkeypatch):
    """Reintroducing a database query must break the dashboard endpoint contract."""
    question = {
        "question_id": "q1", "tier": 1, "correctness": 1, "judge_score": 0.9,
        "cost_usd": 0.01, "latency_ms": 500, "outcome": "correct", "sql": "SELECT 1",
        "trace_id": "t1", "trace_url": "https://lf/traces/t1",
        "session_url": "https://lf/sessions/s", "question": "One?",
        "transcript": [{"role": "user", "content": "One?"}],
    }
    read_model = {
        "runs": ["r1"],
        "leaderboard": {"r1": [{"config_id": "m__p"}]},
        "tiers": {"r1": [{"config_id": "m__p", "tier": 1, "accuracy": 1.0}]},
        "outcomes": {"r1": [{"config_id": "m__p", "outcome": "correct", "n": 1}]},
        "questions": {("r1", "m__p"): [question]},
    }
    monkeypatch.setattr(dashboard, "_read_model", lambda: read_model, raising=False)

    assert dashboard.runs() == ["r1"]
    assert dashboard.leaderboard("r1") == [{"config_id": "m__p"}]
    assert dashboard.tiers("r1") == [{"config_id": "m__p", "tier": 1, "accuracy": 1.0}]
    assert dashboard.outcomes("r1") == [{"config_id": "m__p", "outcome": "correct", "n": 1}]
    assert dashboard.questions("r1", "m__p")[0]["trace_id"] == "t1"


def test_health_endpoint_does_not_load_langfuse(monkeypatch):
    monkeypatch.setattr(dashboard, "_read_model",
                        lambda: (_ for _ in ()).throw(AssertionError("must not fetch")))
    assert dashboard.healthz() == {"status": "ok"}


def test_reused_run_id_keeps_only_the_newest_question_result():
    """A repeated run/config/question must not inflate counts or preserve stale scores."""
    base = {"run_id": "r1", "config_id": "m__p", "model_name": "m",
            "prompt_name": "p", "question_id": "q1", "tier": 1,
            "judge_score": 0.5, "cost_usd": 0.01, "latency_ms": 1000,
            "outcome": "wrong_result"}
    stale = {**base, "correctness": 0, "started_at": "2026-08-03T01:00:00Z"}
    newest = {**base, "correctness": 1, "outcome": "correct",
              "started_at": "2026-08-03T02:00:00Z"}

    model = dashboard._build_read_model([newest, stale])
    assert model["leaderboard"]["r1"][0]["n_questions"] == 1
    assert model["leaderboard"]["r1"][0]["accuracy"] == 1.0


def test_normalizer_recovers_ids_from_experiment_fields():
    item = {
        "traceId": "t1", "startTime": "2026-08-03T01:00:00Z",
        "experimentName": "review-demo__qwen3.7-flash__P1_zeroshot",
        "experimentItemId": "q008",
        "input": '{"question":"Recovered?"}',
        "output": '{"tier":2,"cost_usd":0.01}',
        "metadata": {"dataset_item_id": "q008"},
        "scores": [],
    }
    row = dashboard._normalize_experiment_item(item, "https://lf.example/project/p1")
    assert row["run_id"] == "review-demo"
    assert row["config_id"] == "qwen3.7-flash__P1_zeroshot"
    assert row["question_id"] == "q008"
    assert row["question"] == "Recovered?"


def test_leaderboard_sorts_by_cost_per_correct_answer():
    common = {"run_id": "r", "prompt_name": "p", "question_id": "q",
              "tier": 1, "judge_score": 1, "latency_ms": 1,
              "outcome": "correct", "started_at": "2026-08-03T00:00:00Z"}
    expensive = {**common, "config_id": "accurate__p", "model_name": "accurate",
                 "correctness": 1, "cost_usd": 1.0}
    cheap = {**common, "config_id": "cheap__p", "model_name": "cheap",
             "correctness": 1, "cost_usd": 0.01}
    board = dashboard._build_read_model([expensive, cheap])["leaderboard"]["r"]
    assert [row["config_id"] for row in board] == ["cheap__p", "accurate__p"]


def test_default_experiment_window_is_recent():
    calls = []

    def get_page(params):
        calls.append(params)
        return {"data": [], "meta": {"cursor": None}}

    fetch_experiment_rows(get_page, "https://lf.example/project/p1")
    assert calls[0]["fromStartTime"] > "2026-01-01T00:00:00Z"
