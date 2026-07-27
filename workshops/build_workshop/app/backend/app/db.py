from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError, OperationalError
from fastapi import HTTPException

from app.observability import start_span
from app.settings import settings

logger = logging.getLogger("app.db")

# ClickHouse SQL can be large; truncate before logging or attaching to a span.
_SQL_ATTR_MAXLEN = 500

# A ClickHouse Cloud service idle-scales to zero and takes ~5-30s to wake, so the
# first request after an idle period can read-time-out at the default 5s query
# timeout. run_query retries such transport timeouts once on a fresh client with
# this longer read timeout, and /api/health probes with it directly.
IDLE_WAKE_TIMEOUT_SECONDS = 30


def _categorize_clickhouse_error(msg: str) -> str:
    """Map a ClickHouse error message to a stable category used for span
    attributes, log context and the HTTP status chosen below."""
    if "TOO_MANY_ROWS" in msg or "error code 158" in msg:
        return "too_many_rows"
    if "max_execution_time" in msg or "TIMEOUT_EXCEEDED" in msg:
        return "timeout"
    return "query_failed"


def is_not_seeded_error(msg: str) -> bool:
    """True when a query failed only because the schema is not there yet -- the
    target database or table does not exist.

    On the workshop path the app is started in module 00 against an empty Cloud
    service, and the taxi schema is not created and seeded until module 02. Until
    then every dashboard query references a missing object. Rather than surfacing
    that as a wall of 500s, we treat it as "no data yet" and return an empty
    result, so the dashboards render empty exactly as the playbook says they
    will. The match is deliberately narrow (the two ClickHouse error names for a
    missing table/database) so it never masks a genuine query failure such as a
    bad column or SQL syntax error."""
    return "UNKNOWN_TABLE" in msg or "UNKNOWN_DATABASE" in msg


def get_client(send_receive_timeout: int | None = None) -> Client:
    timeout = send_receive_timeout or settings.query_timeout_seconds

    def _connect(read_timeout: int) -> Client:
        common = dict(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            secure=settings.clickhouse_secure_effective,
            connect_timeout=settings.clickhouse_connect_timeout,
            send_receive_timeout=read_timeout,
        )
        try:
            return clickhouse_connect.get_client(database=settings.clickhouse_database, **common)
        except OperationalError:
            raise  # let the caller retry transport timeouts with a longer read timeout
        except ClickHouseError as e:
            # clickhouse-connect binds the session to CLICKHOUSE_DATABASE at connect
            # time and runs a settings probe, so if that database does not exist yet
            # the client fails to construct at all -- and every endpoint calls
            # get_client() first, which is what turns the not-yet-seeded state
            # (workshop modules 00-01, before module 01 creates nyc_tlc_data) into a
            # wall of 500s. Fall back to connecting without a default database (the
            # server's own `default`); read queries then hit UNKNOWN_TABLE and
            # run_query() returns an empty result, so the dashboards render empty as
            # the playbook says. Once the schema exists, the primary connect succeeds.
            if is_not_seeded_error(str(e)):
                logger.info(
                    "configured database %r does not exist yet; connecting without a "
                    "default database so queries return empty until module 01 seeds it",
                    settings.clickhouse_database,
                )
                return clickhouse_connect.get_client(**common)
            raise

    try:
        return _connect(timeout)
    except OperationalError:
        # Transport-level connect/read timeout while constructing the client:
        # clickhouse-connect runs a settings-probe query at connect time, and the
        # first request to a Cloud service waking from idle can exceed the default
        # 5s read timeout. run_query() retries idle-wake timeouts for queries, but
        # this happens before any query runs, so without a retry here the very
        # first request after an idle period 500s. Retry once with the longer
        # idle-wake read timeout (the not-seeded fallback inside _connect still
        # applies on the retry, covering a service that is both waking and empty).
        logger.warning(
            "ClickHouse client construct timed out (likely idle-wake); retrying "
            "once with read timeout=%ds",
            IDLE_WAKE_TIMEOUT_SECONDS,
        )
        return _connect(IDLE_WAKE_TIMEOUT_SECONDS)


@dataclass(frozen=True)
class QueryMeta:
    elapsed_ms: int
    rows_returned: int
    cached: bool = False
    # The query as executed, with parameter placeholders inlined as literals so it
    # can be copy-pasted and run directly (see inline_sql). Surfaced to the UI via
    # the response `meta` so each dashboard panel can show its own SQL.
    sql: str | None = None


# Matches a clickhouse-connect server-side bind token, e.g. {start:DateTime} or
# {pickup_zone_ids:Array(UInt16)}. Only the parameter name is captured; the type
# annotation is discarded because the inlined literal carries its own type.
_PARAM_TOKEN = re.compile(r"\{(\w+):[^{}]+\}")


def _format_literal(value: Any) -> str:
    """Render a Python bind value as a ClickHouse SQL literal for display.

    Kept deliberately small and dependency-free (rather than reaching into
    clickhouse-connect internals) since it only has to cover the value types the
    query builders bind: datetimes, ints/floats, bools, strings and arrays of
    those. The result is semantically equivalent to the server-side-bound query,
    just inlined so it is runnable as-is."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        return "'" + value.strftime("%Y-%m-%d %H:%M:%S") + "'"
    if isinstance(value, date):
        return "'" + value.strftime("%Y-%m-%d") + "'"
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(_format_literal(v) for v in value) + "]"
    # Fall back to a single-quoted, escaped string literal.
    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return "'" + escaped + "'"


def inline_sql(sql: str, parameters: dict[str, Any] | None) -> str:
    """Inline bound parameters into `sql`, returning a runnable statement.

    Unknown tokens (a name absent from `parameters`) are left untouched so the
    output never silently drops a clause."""
    text = sql.strip()
    if not parameters:
        return text

    def _sub(match: re.Match[str]) -> str:
        name = match.group(1)
        if name in parameters:
            return _format_literal(parameters[name])
        return match.group(0)

    return _PARAM_TOKEN.sub(_sub, text)


def _execute(client: Client, sql: str, parameters: dict[str, Any] | None):
    return client.query(
        sql,
        parameters=parameters or {},
        settings={
            "max_execution_time": settings.query_timeout_seconds,
            "max_rows_to_read": settings.max_rows_to_read,
            "max_bytes_to_read": settings.max_bytes_to_read,
        },
    )


def _http_error_for(e: ClickHouseError, sql: str, start: float, span: Any) -> HTTPException:
    """Annotate the span, log, and build the HTTPException for a failed query.

    A transport-level connect/read timeout (clickhouse-connect raises
    OperationalError for these) means the service was unreachable or still
    waking, not that the query ran too long -> 503. Server-side failures keep
    their existing mapping: safety-limit hits -> 413, query timeouts -> 504,
    anything else -> 500.
    """
    msg = str(e)
    category = "unavailable" if isinstance(e, OperationalError) else _categorize_clickhouse_error(msg)
    elapsed_ms = int((time.perf_counter() - start) * 1000)
    span.set_attribute("error", True)
    span.set_attribute("error.category", category)
    span.set_attribute("db.elapsed_ms", elapsed_ms)
    span.record_exception(e)
    logger.error(
        "ClickHouse query failed (category=%s, elapsed_ms=%d): %s | sql=%s",
        category,
        elapsed_ms,
        msg,
        sql[:_SQL_ATTR_MAXLEN],
    )
    # Make safety-limit failures actionable (don't surface as a generic 500).
    if category == "too_many_rows":
        return HTTPException(
            status_code=413,
            detail=(
                "Query exceeded backend safety limits while scanning the dataset. "
                f"Increase MAX_ROWS_TO_READ/MAX_BYTES_TO_READ or reduce the time range. "
                f"(max_rows_to_read={settings.max_rows_to_read}, max_bytes_to_read={settings.max_bytes_to_read})"
            ),
        )
    if category == "timeout":
        return HTTPException(
            status_code=504,
            detail=(
                "Query timed out. Increase QUERY_TIMEOUT_SECONDS or reduce the time range / interval."
            ),
        )
    if category == "unavailable":
        return HTTPException(
            status_code=503,
            detail=(
                "ClickHouse was unreachable or still waking from idle (the first request "
                "to an idle Cloud service can take ~30s). Please retry in a few seconds; "
                "if it persists, check CLICKHOUSE_HOST/PASSWORD and network access to port 8443."
            ),
        )
    return HTTPException(status_code=500, detail=f"ClickHouse query failed: {msg}")


def _empty_not_seeded_result(
    sql: str, parameters: dict[str, Any] | None, start: float, span: Any
) -> tuple[list[dict[str, Any]], QueryMeta]:
    """Treat a missing database/table as an empty result set (see
    is_not_seeded_error). Annotates the span with a distinct category so the
    not-yet-seeded state is still visible in traces, then returns zero rows."""
    elapsed_ms = int((time.perf_counter() - start) * 1000)
    span.set_attribute("error.category", "not_seeded")
    span.set_attribute("db.elapsed_ms", elapsed_ms)
    span.set_attribute("db.rows_returned", 0)
    logger.info(
        "ClickHouse database/table not found (schema not seeded yet?); returning "
        "an empty result so the dashboards render empty instead of erroring "
        "(create + seed the schema in module 02) | sql=%s",
        sql[:_SQL_ATTR_MAXLEN],
    )
    return [], QueryMeta(
        elapsed_ms=elapsed_ms,
        rows_returned=0,
        cached=False,
        sql=inline_sql(sql, parameters),
    )


def run_query(
    client: Client,
    sql: str,
    parameters: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], QueryMeta]:
    start = time.perf_counter()
    with start_span("clickhouse.query") as span:
        span.set_attribute("db.system", "clickhouse")
        span.set_attribute("db.statement", sql[:_SQL_ATTR_MAXLEN])
        try:
            result = _execute(client, sql, parameters)
        except OperationalError as e:
            # Transport-level connect/read timeout, not a server-side query error
            # (clickhouse-connect maps those to DatabaseError, which the
            # ClickHouseError clause below still turns into a 504). The common
            # cause is a ClickHouse Cloud service waking from idle on the first
            # request. Retry exactly once on a fresh client with a longer read
            # timeout. The server-side max_execution_time in _execute is
            # unchanged, so a genuinely slow query still returns TIMEOUT_EXCEEDED
            # on the retry and maps to 504 -- the only cost of a misclassified
            # slow query is one wasted attempt.
            logger.warning(
                "ClickHouse transport timeout (likely idle-wake); retrying once with read timeout=%ds: %s",
                IDLE_WAKE_TIMEOUT_SECONDS,
                e,
            )
            span.set_attribute("clickhouse.idle_wake_retry", True)
            try:
                retry_client = get_client(send_receive_timeout=IDLE_WAKE_TIMEOUT_SECONDS)
                result = _execute(retry_client, sql, parameters)
            except ClickHouseError as retry_error:
                if is_not_seeded_error(str(retry_error)):
                    return _empty_not_seeded_result(sql, parameters, start, span)
                raise _http_error_for(retry_error, sql, start, span) from retry_error
        except ClickHouseError as e:
            if is_not_seeded_error(str(e)):
                return _empty_not_seeded_result(sql, parameters, start, span)
            raise _http_error_for(e, sql, start, span) from e

        elapsed_ms = int((time.perf_counter() - start) * 1000)
        rows = result.result_rows
        cols = list(result.column_names)
        out = [dict(zip(cols, row)) for row in rows]
        span.set_attribute("db.elapsed_ms", elapsed_ms)
        span.set_attribute("db.rows_returned", len(out))
        logger.debug("ClickHouse query ok (elapsed_ms=%d, rows=%d)", elapsed_ms, len(out))
        return out, QueryMeta(
            elapsed_ms=elapsed_ms,
            rows_returned=len(out),
            cached=False,
            sql=inline_sql(sql, parameters),
        )

