import sys
from unittest import mock
import pytest
from agents.llm import Usage
from agents.loop import AgentResult


@pytest.fixture
def api(monkeypatch):
    # Stub network clients so importing serving.api touches nothing live.
    import builtins
    import agents.chclient, eval.langfuse_adapter
    monkeypatch.setattr(agents.chclient, "ROClickHouseClient", lambda cfg: object())
    monkeypatch.setattr(eval.langfuse_adapter, "LangfuseTracer", lambda cfg: object())
    monkeypatch.setenv("AGENT_ARENA_POLICY_VERSION", "policy-v1")
    # NOTE: deviates from the brief's blanket `mock.mock_open` on builtins.open —
    # that also intercepts load_config()'s real config.yaml/.env reads (called at
    # serving.api import time) and breaks yaml.safe_load with a TypeError. Instead,
    # only fake the one path serving.api reads for schema context; everything else
    # (config.yaml, .env) goes through the real `open`.
    real_open = builtins.open

    def _fake_open(path, *a, **k):
        if str(path) == "schema/schema_context.md":
            return mock.mock_open(read_data="SCHEMA")(*a, **k)
        return real_open(path, *a, **k)

    monkeypatch.setattr("builtins.open", _fake_open)
    sys.modules.pop("serving.api", None)
    import serving.api as api
    # restore open for other tests
    monkeypatch.undo()
    return api


def _fake_ar():
    return AgentResult(sql="SELECT 1", rows=[[1]], cols=["n"], error=None,
                       attempts=1, usage=Usage(10, 3), outcome_hint="ok",
                       transcript=[{"role": "user", "content": "q"}])


def test_ask_returns_trace_and_session_and_calls_emit(api, monkeypatch):
    calls = {}
    agent_call = {}

    def fake_run_agent(*args, **kwargs):
        agent_call["context"] = args[3]
        return _fake_ar()

    monkeypatch.setattr(api, "run_agent", fake_run_agent)
    def fake_emit(**kw):
        calls.update(kw)
        return "trace-xyz"
    monkeypatch.setattr(api, "emit_agent_trace", fake_emit)
    monkeypatch.setattr(api, "get_client", lambda: mock.MagicMock())
    resp = api.ask(api.AskRequest(question="How many orders?",
                                  config_id="claude-sonnet-5__P1_zeroshot"))
    assert resp.trace_id == "trace-xyz"
    assert resp.session_id.startswith("ask-")
    assert resp.sql == "SELECT 1" and resp.rows == [[1]]
    assert resp.policy_version == "policy-v1"
    assert calls["question"] == "How many orders?"
    assert "serving" in calls["tags"]
    assert "policy-v1" in calls["tags"]
    assert calls["metadata"]["policy_version"] == "policy-v1"
    assert calls["metadata"]["release"] == "serving"
    assert "Business metric policy (policy-v1)" in agent_call["context"]
    assert calls["usage"].output_tokens == 3


def test_ask_honors_supplied_session_id(api, monkeypatch):
    monkeypatch.setattr(api, "run_agent", lambda *a, **k: _fake_ar())
    monkeypatch.setattr(api, "emit_agent_trace", lambda **kw: "t")
    monkeypatch.setattr(api, "get_client", lambda: mock.MagicMock())
    resp = api.ask(api.AskRequest(question="q", config_id="claude-sonnet-5__P1_zeroshot",
                                  session_id="conv-1"))
    assert resp.session_id == "conv-1"


def test_feedback_records_idempotent_boolean_user_thumbs(api, monkeypatch):
    class ScoreClient:
        def __init__(self):
            self.calls = []

        def create_score(self, *, score_id, name, value, trace_id, data_type,
                         comment):
            self.calls.append({
                "score_id": score_id,
                "name": name,
                "value": value,
                "trace_id": trace_id,
                "data_type": data_type,
                "comment": comment,
            })

        def flush(self):
            pass

    client = ScoreClient()
    monkeypatch.setattr(api, "get_client", lambda: client)
    assert api.feedback(api.FeedbackRequest(
        trace_id="trace-1", value=False, comment="metric mismatch"
    )) == {"ok": True}
    assert client.calls == [{
        "score_id": "user-thumbs-trace-1",
        "name": "user-thumbs",
        "value": False,
        "trace_id": "trace-1",
        "data_type": "BOOLEAN",
        "comment": "metric mismatch",
    }]


def test_feedback_rejects_missing_trace_id(api):
    import fastapi
    with pytest.raises(fastapi.HTTPException):
        api.feedback(api.FeedbackRequest(trace_id="", value=False))


def test_feedback_failure_redacts_exception_from_response_and_logs(
    api, monkeypatch, caplog
):
    import fastapi

    secret_marker = "SECRET-MARKER-provider-token"

    class FailingScoreClient:
        def create_score(self, **_kwargs):
            raise RuntimeError(secret_marker)

    monkeypatch.setattr(api, "get_client", lambda: FailingScoreClient())

    with caplog.at_level("ERROR"), pytest.raises(fastapi.HTTPException) as raised:
        api.feedback(api.FeedbackRequest(trace_id="trace-1", value=False))

    assert raised.value.status_code == 502
    assert raised.value.detail == "feedback service unavailable"
    diagnostics = "\n".join(record.getMessage() for record in caplog.records)
    assert "feedback phase=create-score exception_type=RuntimeError" in diagnostics
    assert secret_marker not in diagnostics
    assert secret_marker not in str(raised.value.detail)


def test_feedback_flush_failure_uses_fixed_sanitized_phase(api, monkeypatch, caplog):
    import fastapi

    secret_marker = "SECRET-MARKER-flush-token"

    class FailingFlushClient:
        def create_score(self, **_kwargs):
            pass

        def flush(self):
            raise OSError(secret_marker)

    monkeypatch.setattr(api, "get_client", lambda: FailingFlushClient())

    with caplog.at_level("ERROR"), pytest.raises(fastapi.HTTPException) as raised:
        api.feedback(api.FeedbackRequest(trace_id="trace-1", value=True))

    diagnostics = "\n".join(record.getMessage() for record in caplog.records)
    assert raised.value.detail == "feedback service unavailable"
    assert "feedback phase=flush exception_type=OSError" in diagnostics
    assert secret_marker not in diagnostics
