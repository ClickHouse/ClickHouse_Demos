import io
from urllib.error import HTTPError

import agents.llm
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
