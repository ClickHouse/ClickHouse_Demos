from __future__ import annotations

import logging
import time
from dataclasses import dataclass
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


def get_client(send_receive_timeout: int | None = None) -> Client:
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
        secure=settings.clickhouse_secure_effective,
        connect_timeout=settings.clickhouse_connect_timeout,
        send_receive_timeout=send_receive_timeout or settings.query_timeout_seconds,
    )


@dataclass(frozen=True)
class QueryMeta:
    elapsed_ms: int
    rows_returned: int
    cached: bool = False


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
                raise _http_error_for(retry_error, sql, start, span) from retry_error
        except ClickHouseError as e:
            raise _http_error_for(e, sql, start, span) from e

        elapsed_ms = int((time.perf_counter() - start) * 1000)
        rows = result.result_rows
        cols = list(result.column_names)
        out = [dict(zip(cols, row)) for row in rows]
        span.set_attribute("db.elapsed_ms", elapsed_ms)
        span.set_attribute("db.rows_returned", len(out))
        logger.debug("ClickHouse query ok (elapsed_ms=%d, rows=%d)", elapsed_ms, len(out))
        return out, QueryMeta(elapsed_ms=elapsed_ms, rows_returned=len(out), cached=False)

