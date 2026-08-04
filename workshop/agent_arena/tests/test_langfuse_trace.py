import sys
import types
import json
from unittest import mock
from agents.llm import Usage
import eval.langfuse_adapter as adapter


def test_str_meta_coerces_keys_and_truncates_values():
    out = adapter._str_meta({"config_id": "a", "n": 123, "long": "x" * 300})
    assert out == {"configid": "a", "n": "123", "long": "x" * 200}
    assert all(isinstance(k, str) and k.isalnum() for k in out)
    assert all(isinstance(v, str) and len(v) <= 200 for v in out.values())


def test_emit_agent_trace_uses_v4_api_and_returns_trace_id():
    client = mock.MagicMock()
    client.get_current_trace_id.return_value = "trace-123"
    # context managers used with `with`
    client.start_as_current_observation.return_value.__enter__ = mock.Mock()
    client.start_as_current_observation.return_value.__exit__ = mock.Mock(return_value=False)
    pa_cm = mock.MagicMock()
    with mock.patch.object(adapter, "get_client", return_value=client), \
         mock.patch.object(adapter, "propagate_attributes", return_value=pa_cm) as pa:
        tid = adapter.emit_agent_trace(
            trace_name="agent_run", session_id="run__cfg",
            tags=["cfg", "run:r1", "m", "p"],
            metadata={"config_id": "cfg", "question_id": "q1"},
            question="How many orders?", model="m",
            transcript=[{"role": "user", "content": "q"}],
            sql="SELECT 1", output_payload={"sql": "SELECT 1", "rows": []},
            usage=Usage(10, 3))
    assert tid == "trace-123"
    # propagate_attributes got trace-level attrs with stringified metadata
    _, kw = pa.call_args
    assert kw["trace_name"] == "agent_run"
    assert kw["session_id"] == "run__cfg"
    assert kw["tags"] == ["cfg", "run:r1", "m", "p"]
    assert kw["metadata"] == {"configid": "cfg", "questionid": "q1"}
    # a generation child with usage_details was created
    _, gkw = client.start_as_current_observation.call_args
    assert gkw["as_type"] == "generation"
    assert gkw["model"] == "m"
    assert gkw["usage_details"] == {"input": 10, "output": 3}
    # root observation input/output set so the trace derives them
    _, ukw = client.update_current_span.call_args
    assert ukw["input"] == {"question": "How many orders?"}
    assert ukw["output"] == {"sql": "SELECT 1", "rows": []}


def test_emit_agent_trace_never_raises_returns_empty_on_failure():
    with mock.patch.object(adapter, "get_client", side_effect=RuntimeError("boom")):
        tid = adapter.emit_agent_trace(
            trace_name="t", session_id="s", tags=[], metadata={},
            question="q", model="m", transcript=[], sql=None,
            output_payload={}, usage=Usage(0, 0))
    assert tid == ""


def test_fetch_trace_scores_reads_typed_values_from_scores_v3():
    """Using the deprecated trace endpoint must lose current v3 score records."""
    tracer = object.__new__(adapter.LangfuseTracer)
    tracer._host = "https://lf.example"
    tracer._auth = "basic-token"
    response = mock.MagicMock()
    response.read.return_value = json.dumps({"data": [
        {"name": "correctness", "value": 1.0, "dataType": "NUMERIC"},
        {"name": "outcome", "value": "correct", "dataType": "CATEGORICAL"},
    ], "meta": {"cursor": None}}).encode()
    response.__enter__.return_value = response
    response.__exit__.return_value = False

    with mock.patch.object(adapter.urllib.request, "urlopen", return_value=response):
        assert tracer.fetch_trace_scores("trace-1") == [
            {"name": "correctness", "value": 1.0, "string": None, "dataType": "NUMERIC"},
            {"name": "outcome", "value": None, "string": "correct", "dataType": "CATEGORICAL"},
        ]
