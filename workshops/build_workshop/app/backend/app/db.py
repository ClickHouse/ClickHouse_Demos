from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError
from fastapi import HTTPException

from app.observability import start_span
from app.settings import settings

logger = logging.getLogger("app.db")

# ClickHouse SQL can be large; truncate before logging or attaching to a span.
_SQL_ATTR_MAXLEN = 500


def _categorize_clickhouse_error(msg: str) -> str:
    """Map a ClickHouse error message to a stable category used for span
    attributes, log context and the HTTP status chosen below."""
    if "TOO_MANY_ROWS" in msg or "error code 158" in msg:
        return "too_many_rows"
    if "max_execution_time" in msg or "TIMEOUT_EXCEEDED" in msg:
        return "timeout"
    return "query_failed"


def get_client() -> Client:
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
        database=settings.clickhouse_database,
        secure=settings.clickhouse_secure_effective,
        connect_timeout=settings.clickhouse_connect_timeout,
        send_receive_timeout=settings.query_timeout_seconds,
    )


@dataclass(frozen=True)
class QueryMeta:
    elapsed_ms: int
    rows_returned: int
    cached: bool = False


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
            result = client.query(
                sql,
                parameters=parameters or {},
                settings={
                    "max_execution_time": settings.query_timeout_seconds,
                    "max_rows_to_read": settings.max_rows_to_read,
                    "max_bytes_to_read": settings.max_bytes_to_read,
                },
            )
        except ClickHouseError as e:
            msg = str(e)
            category = _categorize_clickhouse_error(msg)
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
                raise HTTPException(
                    status_code=413,
                    detail=(
                        "Query exceeded backend safety limits while scanning the dataset. "
                        f"Increase MAX_ROWS_TO_READ/MAX_BYTES_TO_READ or reduce the time range. "
                        f"(max_rows_to_read={settings.max_rows_to_read}, max_bytes_to_read={settings.max_bytes_to_read})"
                    ),
                ) from e
            if category == "timeout":
                raise HTTPException(
                    status_code=504,
                    detail=(
                        "Query timed out. Increase QUERY_TIMEOUT_SECONDS or reduce the time range / interval."
                    ),
                ) from e
            raise HTTPException(status_code=500, detail=f"ClickHouse query failed: {msg}") from e

        elapsed_ms = int((time.perf_counter() - start) * 1000)
        rows = result.result_rows
        cols = list(result.column_names)
        out = [dict(zip(cols, row)) for row in rows]
        span.set_attribute("db.elapsed_ms", elapsed_ms)
        span.set_attribute("db.rows_returned", len(out))
        logger.debug("ClickHouse query ok (elapsed_ms=%d, rows=%d)", elapsed_ms, len(out))
        return out, QueryMeta(elapsed_ms=elapsed_ms, rows_returned=len(out), cached=False)

