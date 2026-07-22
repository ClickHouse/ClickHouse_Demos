from __future__ import annotations

import json
import re
import time
from contextlib import nullcontext
from dataclasses import dataclass
from typing import Any

from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError
from fastapi import HTTPException

from app.db import is_not_seeded_error
from app.settings import settings

# Tables the model is allowed to reference. The ClickHouse client connects with
# database=nyc_tlc_data, so the prompt uses unqualified names like the rest of the app.
ALLOWED_TABLES = ("taxi_trips", "taxi_zones", "taxi_trips_expanded")

# Fallback schema used when ClickHouse introspection is unavailable (e.g. running the
# chat prompt/guardrails without a live database). Mirrors db/init/001_schema.sql.
FALLBACK_SCHEMA = """\
taxi_trips (
  car_type String,
  vendor_id Nullable(UInt16),
  pickup_datetime DateTime('UTC'),
  dropoff_datetime DateTime('UTC'),
  pickup_location_id Nullable(UInt16),
  dropoff_location_id Nullable(UInt16),
  pickup_borough Nullable(String),
  dropoff_borough Nullable(String),
  passenger_count Nullable(UInt16),
  trip_distance Nullable(Float64),
  payment_type Nullable(UInt16),
  fare_amount Nullable(Float64),
  tip_amount Nullable(Float64),
  tolls_amount Nullable(Float64),
  total_amount Nullable(Float64),
  congestion_surcharge Nullable(Float64),
  airport_fee Nullable(Float64)
)

taxi_zones (
  location_id UInt16,
  zone String,
  borough String,
  subregion String
)
"""

# A single guardrailed NL-to-SQL turn returns strict JSON with these keys.
SYSTEM_PROMPT = """\
You are a SQL analyst for a NYC taxi analytics dashboard backed by ClickHouse.
Translate the user's question into ONE read-only ClickHouse SQL query over the
schema below, then answer in plain English.

Schema (database nyc_tlc_data, reference tables WITHOUT the database prefix):
{schema}

Rules:
- Emit exactly ONE statement and it MUST be a SELECT (a leading WITH ... SELECT is fine).
- Never write INSERT, UPDATE, DELETE, ALTER, DROP, CREATE, TRUNCATE, or any DDL/DML.
- Always include a LIMIT (<= {row_limit}). The dataset is large.
- Trip duration in seconds is dateDiff('second', pickup_datetime, dropoff_datetime).
- Use quantileTDigest(q)(...) for percentiles (e.g. p95 = quantileTDigest(0.95)(...)).
- Bucket time with toStartOfInterval(pickup_datetime, INTERVAL n MINUTE|HOUR) or
  toStartOfDay / toStartOfWeek / toStartOfMonth.
- Join taxi_trips to taxi_zones on taxi_zones.location_id = taxi_trips.pickup_location_id
  (or dropoff_location_id) to turn zone ids into names/boroughs.
- Revenue: sum(ifNull(total_amount, ifNull(fare_amount, 0) + ifNull(tip_amount, 0))).
- If the question cannot be answered with these tables, set sql to null and explain why.

Respond with a JSON object only, no markdown, with keys:
- "answer": a short plain-English description of what the query returns.
- "sql": the single SELECT statement (or null if not answerable).
- "chart": {{"type": "line"|"bar"|"none", "x": <column alias or null>, "y": <column alias or list of aliases, or null>}}.
  Use "line" for time series, "bar" for ranked categories, "none" otherwise.
  x and y must reference column aliases from the SELECT list.
"""

# Few-shot examples grounded in the repo's own query patterns (readme sections 4 and 6).
FEW_SHOTS: list[dict[str, str]] = [
    {
        "role": "user",
        "content": "How many trips per hour on 2022-07-02?",
    },
    {
        "role": "assistant",
        "content": json.dumps(
            {
                "answer": "Hourly trip counts on 2022-07-02.",
                "sql": (
                    "SELECT toStartOfInterval(pickup_datetime, INTERVAL 1 HOUR) AS ts, "
                    "count() AS trips FROM taxi_trips "
                    "WHERE pickup_datetime >= toDateTime('2022-07-02 00:00:00') "
                    "AND pickup_datetime < toDateTime('2022-07-03 00:00:00') "
                    "GROUP BY ts ORDER BY ts LIMIT 100"
                ),
                "chart": {"type": "line", "x": "ts", "y": "trips"},
            }
        ),
    },
    {
        "role": "user",
        "content": "Top 10 pickup zones by number of trips in July 2022.",
    },
    {
        "role": "assistant",
        "content": json.dumps(
            {
                "answer": "The 10 busiest pickup zones by trip count in July 2022.",
                "sql": (
                    "SELECT z.zone AS zone, z.borough AS borough, count() AS trips "
                    "FROM taxi_trips t "
                    "INNER JOIN taxi_zones z ON z.location_id = t.pickup_location_id "
                    "WHERE t.pickup_datetime >= toDateTime('2022-07-01 00:00:00') "
                    "AND t.pickup_datetime < toDateTime('2022-08-01 00:00:00') "
                    "GROUP BY zone, borough ORDER BY trips DESC LIMIT 10"
                ),
                "chart": {"type": "bar", "x": "zone", "y": "trips"},
            }
        ),
    },
    {
        "role": "user",
        "content": "What is the p95 trip duration by borough?",
    },
    {
        "role": "assistant",
        "content": json.dumps(
            {
                "answer": "95th-percentile trip duration (seconds) grouped by pickup borough.",
                "sql": (
                    "SELECT z.borough AS borough, "
                    "quantileTDigest(0.95)(dateDiff('second', t.pickup_datetime, t.dropoff_datetime)) "
                    "AS p95_duration_s FROM taxi_trips t "
                    "INNER JOIN taxi_zones z ON z.location_id = t.pickup_location_id "
                    "GROUP BY borough ORDER BY p95_duration_s DESC LIMIT 100"
                ),
                "chart": {"type": "bar", "x": "borough", "y": "p95_duration_s"},
            }
        ),
    },
    {
        "role": "user",
        "content": "Show daily revenue for July 2022.",
    },
    {
        "role": "assistant",
        "content": json.dumps(
            {
                "answer": "Daily revenue for July 2022 (total_amount, falling back to fare + tip).",
                "sql": (
                    "SELECT toStartOfDay(pickup_datetime) AS ts, "
                    "sum(ifNull(total_amount, ifNull(fare_amount, 0) + ifNull(tip_amount, 0))) AS revenue "
                    "FROM taxi_trips "
                    "WHERE pickup_datetime >= toDateTime('2022-07-01 00:00:00') "
                    "AND pickup_datetime < toDateTime('2022-08-01 00:00:00') "
                    "GROUP BY ts ORDER BY ts LIMIT 100"
                ),
                "chart": {"type": "line", "x": "ts", "y": "revenue"},
            }
        ),
    },
]


# --- SQL guardrails -------------------------------------------------------

class SqlGuardrailError(ValueError):
    """Raised when a model-generated statement fails the read-only guardrails."""


class SchemaNotSeededError(Exception):
    """Raised when a generated query references a table/database that does not
    exist yet -- the schema has not been created and seeded (module 02). Lets
    run_chat answer honestly instead of surfacing a raw ClickHouse error."""


# Whole-word write/DDL keywords that must never appear in a generated query.
_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|ALTER|DROP|CREATE|TRUNCATE|RENAME|ATTACH|DETACH|"
    r"OPTIMIZE|GRANT|REVOKE|SYSTEM|KILL|INTO\s+OUTFILE)\b",
    re.IGNORECASE,
)
_HAS_LIMIT = re.compile(r"\blimit\b", re.IGNORECASE)


def _strip_sql_comments(sql: str) -> str:
    # Remove /* ... */ block comments and -- line comments so they can't hide a
    # second statement or a forbidden keyword from the checks below.
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    sql = re.sub(r"--[^\n]*", " ", sql)
    return sql


def sanitize_select_sql(raw_sql: str) -> str:
    """Validate that raw_sql is a single read-only SELECT and return it with a LIMIT.

    Rejects multi-statement input, non-SELECT statements, and write/DDL keywords.
    """
    if not raw_sql or not raw_sql.strip():
        raise SqlGuardrailError("Model returned an empty query.")

    cleaned = _strip_sql_comments(raw_sql).strip()
    # Drop trailing semicolons; any remaining ';' means multiple statements.
    cleaned = cleaned.rstrip(";").strip()
    if ";" in cleaned:
        raise SqlGuardrailError("Only a single statement is allowed (found ';').")

    head = cleaned.lstrip("( \t\r\n").upper()
    if not (head.startswith("SELECT") or head.startswith("WITH")):
        raise SqlGuardrailError("Only SELECT (or WITH ... SELECT) queries are allowed.")

    forbidden = _FORBIDDEN.search(cleaned)
    if forbidden:
        raise SqlGuardrailError(f"Disallowed keyword in query: {forbidden.group(0)}")

    if not _HAS_LIMIT.search(cleaned):
        cleaned = f"{cleaned}\nLIMIT {settings.chat_row_limit}"

    return cleaned


# --- ClickHouse schema introspection (cached) -----------------------------

_schema_cache: str | None = None


def _describe_table(client: Client, table: str) -> str:
    result = client.query(f"DESCRIBE TABLE {table}")
    cols = [f"  {row[0]} {row[1]}" for row in result.result_rows]
    return f"{table} (\n" + ",\n".join(cols) + "\n)"


def get_schema_text(client: Client | None) -> str:
    """Return a cached, human-readable schema for the prompt.

    Introspects taxi_trips / taxi_zones once via DESCRIBE TABLE and caches the result.
    Falls back to the checked-in schema if the database is unreachable.
    """
    global _schema_cache
    if _schema_cache is not None:
        return _schema_cache

    if client is not None:
        try:
            blocks = [_describe_table(client, "taxi_trips"), _describe_table(client, "taxi_zones")]
            _schema_cache = "\n\n".join(blocks)
            return _schema_cache
        except ClickHouseError:
            pass  # fall back to the static schema below

    _schema_cache = FALLBACK_SCHEMA
    return _schema_cache


# --- LLM client (with optional Langfuse tracing) --------------------------

# Resolved once: whether the Langfuse-wrapped OpenAI client is in use.
_langfuse_enabled = bool(settings.langfuse_public_key and settings.langfuse_secret_key)


def _configure_langfuse() -> bool:
    """Configure the Langfuse singleton so the OpenAI drop-in traces to it.

    Returns True if Langfuse is active, False if unavailable (tracing disabled).
    """
    if not _langfuse_enabled:
        return False
    try:
        from langfuse import Langfuse

        Langfuse(
            public_key=settings.langfuse_public_key,
            secret_key=settings.langfuse_secret_key,
            host=settings.langfuse_base_url,
        )
        return True
    except Exception:  # noqa: BLE001 - never let tracing break the endpoint
        return False


_langfuse_active = _configure_langfuse()


def _chat_trace(fn):
    """Wrap a function with the Langfuse v4 @observe decorator when tracing is active.

    When Langfuse is not configured (or not installed) the function is returned
    unchanged, so no tracer is touched and no warnings are emitted.
    """
    if not _langfuse_active:
        return fn
    try:
        from langfuse import observe

        return observe(name="chat")(fn)
    except Exception:  # noqa: BLE001
        return fn


def _get_openai_client() -> Any:
    """Build an OpenAI client, preferring the Langfuse drop-in when tracing is active.

    The Langfuse wrapper is a transparent passthrough when tracing is disabled, so the
    plain client is only used when Langfuse is not configured or not installed.
    """
    if _langfuse_active:
        try:
            from langfuse.openai import OpenAI  # traced drop-in replacement
        except Exception:  # noqa: BLE001
            from openai import OpenAI
    else:
        from openai import OpenAI

    return OpenAI(api_key=settings.openai_api_key, base_url=settings.llm_base_url)


def shutdown_tracing() -> None:
    """Flush and stop the Langfuse client (safe no-op when tracing is disabled).

    Called from the FastAPI lifespan shutdown so buffered events are flushed on exit.
    """
    if not _langfuse_active:
        return
    try:
        from langfuse import get_client

        get_client().shutdown()
    except Exception:  # noqa: BLE001
        pass


@dataclass(frozen=True)
class ChatPlan:
    answer: str
    sql: str | None
    chart: dict[str, Any] | None


# Models behind a custom LLM_BASE_URL may not honor JSON mode and can wrap the object
# in a ```json code fence; strip fences and extract the first {...} block as a fallback.
_JSON_FENCE = re.compile(r"```(?:json)?\s*(.*?)\s*```", re.DOTALL)


def _parse_plan_json(content: str) -> dict[str, Any]:
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        pass
    match = _JSON_FENCE.search(content)
    candidate = match.group(1) if match else content
    start, end = candidate.find("{"), candidate.rfind("}")
    if start != -1 and end > start:
        try:
            return json.loads(candidate[start : end + 1])
        except json.JSONDecodeError:
            pass
    raise HTTPException(status_code=502, detail="LLM returned invalid JSON.")


def generate_plan(message: str, schema_text: str, conversation_id: str | None) -> ChatPlan:
    """Call the model to turn a question into an answer + SELECT + chart spec."""
    client = _get_openai_client()

    messages: list[dict[str, str]] = [
        {"role": "system", "content": SYSTEM_PROMPT.format(schema=schema_text, row_limit=settings.chat_row_limit)},
        *FEW_SHOTS,
        {"role": "user", "content": message},
    ]

    # The "name" kwarg is a Langfuse drop-in extension (names the generation) and is
    # rejected by the plain OpenAI client, so only attach it when tracing is active.
    extra: dict[str, Any] = {"name": "chat"} if _langfuse_active else {}

    # v4 wiring: propagate_attributes groups this generation under a session so a
    # multi-turn conversation (same conversation_id) shows as one Langfuse session.
    session_ctx: Any = nullcontext()
    if _langfuse_active and conversation_id:
        try:
            from langfuse import propagate_attributes

            session_ctx = propagate_attributes(session_id=conversation_id)
        except Exception:  # noqa: BLE001
            session_ctx = nullcontext()

    try:
        with session_ctx:
            completion = client.chat.completions.create(
                model=settings.llm_model,
                messages=messages,
                temperature=0,
                response_format={"type": "json_object"},
                **extra,
            )
    except Exception as e:  # noqa: BLE001 - surface provider/network errors as 502
        raise HTTPException(status_code=502, detail=f"LLM request failed: {e}") from e

    content = completion.choices[0].message.content or "{}"
    data = _parse_plan_json(content)

    chart = data.get("chart")
    if not isinstance(chart, dict):
        chart = None
    return ChatPlan(answer=str(data.get("answer", "")), sql=data.get("sql"), chart=chart)


# --- Read-only query execution -------------------------------------------

@dataclass(frozen=True)
class ChatQueryResult:
    rows: list[dict[str, Any]]
    elapsed_ms: int


def execute_readonly_select(client: Client, sql: str) -> ChatQueryResult:
    """Run a guardrailed SELECT with per-query safety settings.

    readonly=2 forbids writes but still allows the per-query settings below
    (max_execution_time, max_result_rows). readonly=1 would reject those settings
    because it also blocks any setting change, so 2 is required here.
    """
    start = time.perf_counter()
    try:
        result = client.query(
            sql,
            settings={
                "readonly": 2,
                "max_execution_time": settings.chat_query_timeout_seconds,
                "max_result_rows": settings.chat_max_result_rows,
            },
        )
    except ClickHouseError as e:
        if is_not_seeded_error(str(e)):
            raise SchemaNotSeededError() from e
        raise HTTPException(status_code=400, detail=f"Generated query failed: {e}") from e
    elapsed_ms = int((time.perf_counter() - start) * 1000)

    cols = list(result.column_names)
    rows = [dict(zip(cols, row)) for row in result.result_rows]
    return ChatQueryResult(rows=rows, elapsed_ms=elapsed_ms)


# --- Orchestration --------------------------------------------------------

@dataclass(frozen=True)
class ChatResult:
    answer: str
    sql: str | None
    rows: list[dict[str, Any]] | None
    chart: dict[str, Any] | None


@_chat_trace
def run_chat(client: Client, message: str, conversation_id: str | None) -> ChatResult:
    """Run one chat turn end to end (schema -> LLM -> guardrail -> execution).

    Decorated with Langfuse @observe (when active) so the whole turn is one trace.
    Raises SqlGuardrailError for the router to map to a 400.
    """
    schema_text = get_schema_text(client)
    plan = generate_plan(message, schema_text, conversation_id)

    # Conversational / out-of-scope answers come back without SQL.
    if not plan.sql:
        return ChatResult(answer=plan.answer, sql=None, rows=None, chart=None)

    safe_sql = sanitize_select_sql(plan.sql)
    try:
        result = execute_readonly_select(client, safe_sql)
    except SchemaNotSeededError:
        # The taxi tables are not there yet (before module 02 seeds them). Answer
        # honestly rather than leaking a raw ClickHouse "table does not exist" error.
        return ChatResult(
            answer=(
                "The taxi tables are not populated yet, so I could not run that query. "
                "Create and seed the schema in module 02 (and stream live data in "
                "module 03), then ask again."
            ),
            sql=safe_sql,
            rows=None,
            chart=None,
        )
    return ChatResult(answer=plan.answer, sql=safe_sql, rows=result.rows, chart=plan.chart)
