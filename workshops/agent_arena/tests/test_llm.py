import io
import json
from urllib.error import HTTPError

import agents.llm
import pytest
from agents.llm import (
    Usage, ZeroUsage, ConverseResult, OpenRouterClient, cost_usd,
    _to_openai_messages, _parse_response,
)


def test_translates_converse_shape_to_openai():
    msgs = [{"role": "user", "content": [{"text": "hello"}]},
            {"role": "assistant", "content": [{"text": "hi"}]}]
    out = _to_openai_messages("SYS", msgs)
    assert out == [
        {"role": "system", "content": "SYS"},
        {"role": "user", "content": "hello"},
        {"role": "assistant", "content": "hi"},
    ]


def test_omits_system_when_empty():
    out = _to_openai_messages("", [{"role": "user", "content": [{"text": "q"}]}])
    assert out == [{"role": "user", "content": "q"}]


def test_parse_response_extracts_text_and_usage():
    resp = {"choices": [{"message": {"content": "```sql\nSELECT 1\n```"}}],
            "usage": {"prompt_tokens": 12, "completion_tokens": 3}}
    r = _parse_response(resp)
    assert isinstance(r, ConverseResult)
    assert r.text == "```sql\nSELECT 1\n```"
    assert r.usage == Usage(12, 3)


def test_parse_response_handles_missing_usage_and_null_content():
    r = _parse_response({"choices": [{"message": {"content": None}}]})
    assert r.text == ""
    assert r.usage == ZeroUsage()


def test_cost_usd_matches_formula():
    assert cost_usd(Usage(1_000_000, 500_000), 2.0, 6.0) == 2.0 + 3.0


def test_openrouter_forwards_allowlisted_inference_in_exact_request_body(monkeypatch):
    requests = []

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def read(self):
            return b'{"choices":[{"message":{"content":"SELECT 1"}}]}'

    def urlopen(request, timeout):
        requests.append((request, timeout))
        return Response()

    monkeypatch.setattr(agents.llm.urllib.request, "urlopen", urlopen)

    result = OpenRouterClient("https://openrouter.ai/api/v1", "test-key").converse(
        "qwen/qwen3.7-flash",
        "Return SQL.",
        [{"role": "user", "content": [{"text": "Count rows"}]}],
        {
            "temperature": 0.0,
            "max_tokens": 2048,
            "reasoning": {"max_tokens": 512},
            "secret_payload": "must-not-be-forwarded",
        },
    )

    assert result.text == "SELECT 1"
    assert len(requests) == 1
    request, timeout = requests[0]
    assert timeout == 60
    assert json.loads(request.data) == {
        "model": "qwen/qwen3.7-flash",
        "messages": [
            {"role": "system", "content": "Return SQL."},
            {"role": "user", "content": "Count rows"},
        ],
        "temperature": 0.0,
        "max_tokens": 2048,
        "reasoning": {"max_tokens": 512},
    }


@pytest.mark.parametrize("reasoning", [None, "512", [512]])
def test_openrouter_rejects_non_mapping_reasoning_before_network(monkeypatch, reasoning):
    def unexpected_urlopen(*_args, **_kwargs):
        pytest.fail("invalid reasoning configuration reached the network")

    monkeypatch.setattr(agents.llm.urllib.request, "urlopen", unexpected_urlopen)

    with pytest.raises(TypeError, match="reasoning"):
        OpenRouterClient("https://openrouter.ai/api/v1", "test-key").converse(
            "qwen/qwen3.7-flash", "", [], {"reasoning": reasoning},
        )


def test_openrouter_rejects_unknown_reasoning_key_before_network(monkeypatch):
    def unexpected_urlopen(*_args, **_kwargs):
        pytest.fail("invalid reasoning configuration reached the network")

    monkeypatch.setattr(agents.llm.urllib.request, "urlopen", unexpected_urlopen)

    with pytest.raises(ValueError, match="unknown.*api_key"):
        OpenRouterClient("https://openrouter.ai/api/v1", "test-key").converse(
            "qwen/qwen3.7-flash", "", [],
            {"reasoning": {"max_tokens": 512, "api_key": "secret"}},
        )


@pytest.mark.parametrize("max_tokens", [True, 0, -1, 1.5, "512"])
def test_openrouter_rejects_invalid_reasoning_max_tokens_before_network(
        monkeypatch, max_tokens):
    def unexpected_urlopen(*_args, **_kwargs):
        pytest.fail("invalid reasoning configuration reached the network")

    monkeypatch.setattr(agents.llm.urllib.request, "urlopen", unexpected_urlopen)

    with pytest.raises(ValueError, match="positive integer"):
        OpenRouterClient("https://openrouter.ai/api/v1", "test-key").converse(
            "qwen/qwen3.7-flash", "", [],
            {"reasoning": {"max_tokens": max_tokens}},
        )


def test_openrouter_retries_429_then_returns_completion(monkeypatch):
    calls = []
    sleeps = []
    rate_limited = HTTPError(
        "https://openrouter.ai/api/v1/chat/completions", 429,
        "Too Many Requests", {"Retry-After": "2"}, io.BytesIO(b"rate limited"),
    )

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def read(self):
            return b'{"choices":[{"message":{"content":"QWEN_OK"}}]}'

    responses = iter([rate_limited, Response()])

    def urlopen(request, timeout):
        calls.append((request.full_url, timeout))
        response = next(responses)
        if isinstance(response, Exception):
            raise response
        return response

    monkeypatch.setattr(agents.llm.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(agents.llm.time, "sleep", sleeps.append)

    result = OpenRouterClient("https://openrouter.ai/api/v1", "test-key").converse(
        "qwen/qwen3.7-flash", "system", [], {"max_tokens": 2048},
    )

    assert result.text == "QWEN_OK"
    assert len(calls) == 2
    assert sleeps == [2.0]
