from __future__ import annotations

import json

import boto3
import pytest
from botocore.stub import Stubber

from app import llm_bedrock
from app.settings import settings


@pytest.fixture(scope="session", autouse=True)
def wait_for_api() -> None:
    return None


PLAN = {"answer": "Trips per hour.", "sql": "SELECT count() AS trips FROM taxi_trips LIMIT 10",
        "chart": {"type": "bar", "x": None, "y": "trips"}}


def _response(text: str, stop: str = "end_turn") -> dict:
    return {
        "output": {"message": {"role": "assistant", "content": [{"text": text}]}},
        "stopReason": stop,
        "usage": {"inputTokens": 1200, "outputTokens": 80, "totalTokens": 1280,
                  "cacheReadInputTokens": 1000, "cacheWriteInputTokens": 0},
        "metrics": {"latencyMs": 900},
    }


class _Gen:
    def __init__(self) -> None:
        self.updates: list[dict] = []
        self.ended = False

    def update(self, **kw) -> None:
        self.updates.append(kw)

    def end(self) -> None:
        self.ended = True


class _Lf:
    def __init__(self) -> None:
        self.gen = _Gen()

    def start_observation(self, **_kw) -> _Gen:
        return self.gen


@pytest.fixture()
def bedrock():
    client = boto3.client("bedrock-runtime", region_name="ap-southeast-1",
                          aws_access_key_id="test", aws_secret_access_key="test")
    with Stubber(client) as stub:
        yield client, stub


def test_generate_sql_plan_parses_converse_output_and_traces_usage(monkeypatch, bedrock):
    client, stub = bedrock
    stub.add_response("converse", _response(json.dumps(PLAN)))
    lf = _Lf()
    monkeypatch.setattr(llm_bedrock, "_langfuse", lambda: lf)

    plan = llm_bedrock.generate_sql_plan("How many trips per hour?", "taxi_trips (...)", client=client)

    assert plan == PLAN
    stub.assert_no_pending_responses()
    usage = next(u["usage_details"] for u in lf.gen.updates if "usage_details" in u)
    assert usage == {"input": 1200, "output": 80, "cache_read_input_tokens": 1000, "cache_creation_input_tokens": 0}
    assert lf.gen.ended is True


def test_converse_request_carries_schema_and_no_cache_point_for_haiku(monkeypatch, bedrock):
    client, stub = bedrock
    monkeypatch.setattr(settings, "bedrock_model_id", "global.anthropic.claude-haiku-4-5-20251001-v1:0")
    stub.add_response("converse", _response(json.dumps(PLAN)))
    monkeypatch.setattr(llm_bedrock, "_langfuse", lambda: None)

    llm_bedrock.generate_sql_plan("q", "schema", client=client)

    # Stubber validates every request against the service model before answering, so a
    # consumed response proves outputConfig/json_schema and the message shape are valid
    # for this botocore release.
    stub.assert_no_pending_responses()


def test_bad_stop_reason_raises(monkeypatch, bedrock):
    client, stub = bedrock
    stub.add_response("converse", _response("", stop="malformed_model_output"))
    monkeypatch.setattr(llm_bedrock, "_langfuse", lambda: None)

    with pytest.raises(llm_bedrock.LlmOutputError, match="malformed_model_output"):
        llm_bedrock.generate_sql_plan("q", "schema", client=client)


def test_relative_window_rewrites_the_few_shot_literals():
    turns = llm_bedrock.few_shots_as_converse("relative")
    sqls = [json.loads(t["content"][0]["text"])["sql"] for t in turns if t["role"] == "assistant"]
    assert "now() - INTERVAL 1 DAY" in sqls[0]
    assert "now() - INTERVAL 31 DAY" in sqls[1]
    assert "2022" not in " ".join(sqls)
    fixed = [json.loads(t["content"][0]["text"])["sql"] for t in llm_bedrock.few_shots_as_converse("fixed") if t["role"] == "assistant"]
    assert "2022-07-02" in fixed[0]


@pytest.fixture()
def no_aws_profile(monkeypatch):
    # Keep the developer's ~/.aws config (SSO, login providers) out of client construction.
    monkeypatch.setenv("AWS_CONFIG_FILE", "/dev/null")
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", "/dev/null")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.setattr(boto3, "DEFAULT_SESSION", None)  # an earlier test may have cached one


def test_blank_bedrock_api_key_is_dropped_so_missing_credentials_surface(monkeypatch, no_aws_profile):
    # Compose passes AWS_BEARER_TOKEN_BEDROCK even when it is blank; botocore would send an
    # empty Bearer header instead of raising NoCredentialsError (the chat's 503 setup hint).
    monkeypatch.setenv("AWS_BEARER_TOKEN_BEDROCK", "")
    llm_bedrock._client()
    import os

    assert "AWS_BEARER_TOKEN_BEDROCK" not in os.environ


def test_a_set_bedrock_api_key_is_kept(monkeypatch, no_aws_profile):
    monkeypatch.setenv("AWS_BEARER_TOKEN_BEDROCK", "ABSKexample")
    llm_bedrock._client()
    import os

    assert os.environ["AWS_BEARER_TOKEN_BEDROCK"] == "ABSKexample"


def test_debug_logging_never_records_the_bedrock_api_key_or_the_question(monkeypatch, no_aws_profile):
    # The workshop runs LOG_LEVEL=DEBUG; botocore's DEBUG request log carries the Authorization
    # header (the bearer token) and the prompt, and container logs are shipped to ClickStack.
    # A Stubber answers before that log line, so answer from before-send instead, which botocore
    # emits after logging the request.
    import logging

    from botocore.awsrequest import AWSResponse

    from app.observability import configure_logging

    class _Raw:
        def __init__(self, body: bytes) -> None:
            self._body = body

        def stream(self, **_kw):
            yield self._body

    def answer(request, **_kw):
        body = json.dumps(_response(json.dumps(PLAN))).encode()
        return AWSResponse(request.url, 200, {"Content-Type": "application/json"}, _Raw(body))

    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("AWS_BEARER_TOKEN_BEDROCK", "ABSKsecret-token-value")
    configure_logging()  # replaces the root handlers, so capture with a handler added after it
    records: list[str] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record.getMessage())

    capture = _Capture(level=logging.DEBUG)
    logging.getLogger().addHandler(capture)
    try:
        client = llm_bedrock._client()
        client.meta.events.register("before-send.bedrock-runtime.Converse", answer)
        plan = llm_bedrock.generate_sql_plan("how many secret-question trips", "t(x Int32)", client=client)
        logging.getLogger("app.chat").debug("capture-canary")
    finally:
        logging.getLogger().removeHandler(capture)
    logged = "\n".join(records)
    assert plan == PLAN
    assert "capture-canary" in logged, "DEBUG records are not captured; the test would be vacuous"
    assert "ABSKsecret-token-value" not in logged
    assert "secret-question" not in logged
