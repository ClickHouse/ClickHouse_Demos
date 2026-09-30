"""Amazon Bedrock (Converse API) provider for the NL-to-SQL chat.

Selected with LLM_PROVIDER=bedrock. Returns the same {answer, sql, chart} dict the OpenAI
path parses, so every guardrail downstream (SELECT-only, LIMIT injection, readonly
execution) is shared and untouched. Credentials come from the default boto3 chain; in the
workshop that is the Bedrock API key in AWS_BEARER_TOKEN_BEDROCK.

Langfuse tracing is manual here (no drop-in wrapper exists for boto3): one generation per
call with token usage, cache usage and a cost estimate from _PRICES.
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

from app.observability import start_span
from app.settings import settings

# USD per million tokens (input, output). Langfuse's built-in price table does not know
# Bedrock model ids, so the cost is computed here.
_PRICES: dict[str, tuple[float, float]] = {
    "global.anthropic.claude-haiku-4-5-20251001-v1:0": (1.00, 5.00),
    "global.anthropic.claude-sonnet-4-6": (3.00, 15.00),
}

# The response contract, as a JSON schema for Converse structured output. Bedrock rejects
# numeric bounds and string length keywords, so this carries only types and required keys.
SQL_PLAN_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "answer": {"type": "string"},
        "sql": {"type": ["string", "null"]},
        "chart": {
            "type": "object",
            "properties": {
                "type": {"type": "string", "enum": ["line", "bar", "none"]},
                "x": {"type": ["string", "null"]},
                "y": {"type": ["string", "array", "null"], "items": {"type": "string"}},
            },
            "required": ["type", "x", "y"],
            "additionalProperties": False,
        },
    },
    "required": ["answer", "sql", "chart"],
    "additionalProperties": False,
}

_TERMINAL_STOP_REASONS = ("malformed_model_output", "content_filtered", "guardrail_intervened")

_DATE_LITERAL = re.compile(r"toDateTime\('(\d{4})-(\d{2})-(\d{2}) 00:00:00'\)")


class LlmOutputError(RuntimeError):
    """The model stopped without a usable answer (schema violation, filter, guardrail)."""


def _client() -> Any:
    import boto3
    from botocore.config import Config

    # docker-compose.workshop.yml always passes AWS_BEARER_TOKEN_BEDROCK, blank until a learner
    # pastes a key. botocore treats a blank value as a token and sends an empty Bearer header,
    # so drop it: with no key the call then fails as NoCredentialsError (the 503 setup hint),
    # and an instance-role host falls through to its role as before.
    if not os.environ.get("AWS_BEARER_TOKEN_BEDROCK", "").strip():
        os.environ.pop("AWS_BEARER_TOKEN_BEDROCK", None)

    return boto3.client(
        "bedrock-runtime",
        region_name=settings.bedrock_region,
        config=Config(read_timeout=120, retries={"max_attempts": 2}),
    )


def _relative_window(sql: str) -> str:
    """Rewrite the few-shots' 2022 literals as windows relative to now().

    A trailing-window dataset makes an example that says July 2022 teach the model to
    write queries that return nothing. Only the exact literal shape
    the few-shots use is rewritten; everything else passes through.
    """
    bounds = _DATE_LITERAL.findall(sql)
    if len(bounds) != 2:
        return sql
    (y1, m1, d1), (y2, m2, d2) = bounds
    from datetime import date

    span_days = (date(int(y2), int(m2), int(d2)) - date(int(y1), int(m1), int(d1))).days
    sql = sql.replace(f"toDateTime('{y1}-{m1}-{d1} 00:00:00')", f"now() - INTERVAL {span_days} DAY", 1)
    return sql.replace(f"toDateTime('{y2}-{m2}-{d2} 00:00:00')", "now()", 1)


def few_shots_as_converse(window: str) -> list[dict[str, Any]]:
    """Render chat_service.FEW_SHOTS as Converse user and assistant turns."""
    from app.chat_service import FEW_SHOTS

    turns: list[dict[str, Any]] = []
    for shot in FEW_SHOTS:
        content = shot["content"]
        if window == "relative":
            if shot["role"] == "assistant":
                plan = json.loads(content)
                if plan.get("sql"):
                    plan["sql"] = _relative_window(plan["sql"])
                plan["answer"] = re.sub(r"(on|in|for) (July|\d{4}-\d{2}-\d{2}|July 2022)( 2022)?", "in the selected window", plan["answer"])
                content = json.dumps(plan)
            else:
                content = re.sub(r"( on| in| for)? (July 2022|2022-\d{2}-\d{2})", " in the last days", content)
        turns.append({"role": shot["role"], "content": [{"text": content}]})
    return turns


def _langfuse() -> Any:
    if not (settings.langfuse_public_key and settings.langfuse_secret_key):
        return None
    try:
        from langfuse import get_client

        return get_client()
    except Exception:  # noqa: BLE001 - tracing must never break the request
        return None


def generate_sql_plan(question: str, schema_text: str, client: Any = None) -> dict[str, Any]:
    """One Converse call. Returns the parsed plan dict; raises LlmOutputError on a bad stop."""
    from app.chat_service import SYSTEM_PROMPT

    client = client or _client()
    system: list[dict[str, Any]] = [
        {"text": SYSTEM_PROMPT.format(schema=schema_text, row_limit=settings.chat_row_limit)}
    ]
    # Prompt caching needs a 4,096-token minimum on Haiku, which this prompt is under;
    # Sonnet's minimum is 1,024, so the cache point only pays off there.
    if "sonnet" in settings.bedrock_model_id:
        system.append({"cachePoint": {"type": "default"}})
    messages = few_shots_as_converse(settings.chat_fewshot_window) + [
        {"role": "user", "content": [{"text": question}]}
    ]
    inference = {"maxTokens": 800, "temperature": 0}

    lf = _langfuse()
    gen = lf.start_observation(name="bedrock.converse", as_type="generation") if lf else None
    if gen is not None:
        gen.update(model=settings.bedrock_model_id, input={"system": system, "messages": messages},
                   model_parameters=inference)
    try:
        with start_span("bedrock.converse"):
            resp = client.converse(
                modelId=settings.bedrock_model_id,
                system=system,
                messages=messages,
                inferenceConfig=inference,
                outputConfig={"textFormat": {"type": "json_schema", "structure": {
                    "jsonSchema": {"name": "sql_plan", "schema": json.dumps(SQL_PLAN_SCHEMA)}}}},
            )
        stop = resp.get("stopReason")
        if stop in _TERMINAL_STOP_REASONS:
            raise LlmOutputError(stop)
        text = next(b["text"] for b in resp["output"]["message"]["content"] if "text" in b)
        u = resp.get("usage", {})
        if gen is not None:
            pin, pout = _PRICES.get(settings.bedrock_model_id, (0.0, 0.0))
            gen.update(
                output=text,
                usage_details={
                    "input": u.get("inputTokens", 0),
                    "output": u.get("outputTokens", 0),
                    "cache_read_input_tokens": u.get("cacheReadInputTokens", 0),
                    "cache_creation_input_tokens": u.get("cacheWriteInputTokens", 0),
                },
                cost_details={
                    "input": u.get("inputTokens", 0) * pin / 1e6,
                    "output": u.get("outputTokens", 0) * pout / 1e6,
                },
                metadata={"stopReason": stop, "latencyMs": str(resp.get("metrics", {}).get("latencyMs", ""))},
            )
        return json.loads(text)
    except LlmOutputError:
        if gen is not None:
            gen.update(level="ERROR", status_message="bad stop reason")
        raise
    finally:
        if gen is not None:
            gen.end()
