from __future__ import annotations

from datetime import datetime

import pytest
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError
from fastapi import HTTPException

import app.db as db
from app.db import IDLE_WAKE_TIMEOUT_SECONDS, inline_sql, run_query


@pytest.fixture(scope="session", autouse=True)
def wait_for_api() -> None:
    # Override the integration-test fixture from conftest.py: these are pure unit
    # tests for the idle-wake retry logic and do not need a running API / ClickHouse.
    return None


class _FakeResult:
    """Minimal stand-in for a clickhouse-connect QueryResult."""

    def __init__(self, column_names, result_rows) -> None:
        self.column_names = column_names
        self.result_rows = result_rows


class _FakeClient:
    """A client whose query() runs a scripted list of side effects (raise or return)."""

    def __init__(self, *effects) -> None:
        self._effects = list(effects)
        self.calls = 0

    def query(self, *_args, **_kwargs):
        self.calls += 1
        effect = self._effects.pop(0)
        if isinstance(effect, Exception):
            raise effect
        return effect


def _transport_timeout() -> OperationalError:
    # Shape of the idle-wake error clickhouse-connect raises for a socket read
    # timeout (see httpclient._raw_request).
    return OperationalError("Error Read timed out (read timeout=5) executing HTTP request attempt 1")


def _server_timeout() -> DatabaseError:
    # Shape of a genuine server-side max_execution_time timeout (error code 159).
    return DatabaseError(
        "HTTPDriver received ClickHouse error code 159\n Code: 159. DB::Exception: Timeout exceeded: "
        "elapsed 5.0 seconds, maximum: 5. (TIMEOUT_EXCEEDED)"
    )


def test_retries_once_on_transport_timeout_then_succeeds(monkeypatch) -> None:
    first = _FakeClient(_transport_timeout())
    retry = _FakeClient(_FakeResult(["zone"], [("Midtown",), ("SoHo",)]))

    created = {}

    def fake_get_client(send_receive_timeout=None):
        created["timeout"] = send_receive_timeout
        return retry

    monkeypatch.setattr(db, "get_client", fake_get_client)

    rows, meta = run_query(first, "SELECT zone FROM taxi_zones")

    assert rows == [{"zone": "Midtown"}, {"zone": "SoHo"}]
    assert meta.rows_returned == 2
    # Exactly one retry, on a fresh client built with the longer idle-wake timeout.
    assert first.calls == 1
    assert retry.calls == 1
    assert created["timeout"] == IDLE_WAKE_TIMEOUT_SECONDS


def test_genuine_server_timeout_maps_to_504_without_retry(monkeypatch) -> None:
    client = _FakeClient(_server_timeout())

    def fail_get_client(send_receive_timeout=None):
        raise AssertionError("server-side timeout must not trigger a retry client")

    monkeypatch.setattr(db, "get_client", fail_get_client)

    with pytest.raises(HTTPException) as excinfo:
        run_query(client, "SELECT count() FROM taxi_trips")

    assert excinfo.value.status_code == 504
    assert client.calls == 1


def test_retry_also_times_out_maps_to_503(monkeypatch) -> None:
    first = _FakeClient(_transport_timeout())
    retry = _FakeClient(_transport_timeout())

    monkeypatch.setattr(db, "get_client", lambda send_receive_timeout=None: retry)

    with pytest.raises(HTTPException) as excinfo:
        run_query(first, "SELECT 1")

    assert excinfo.value.status_code == 503
    assert first.calls == 1
    assert retry.calls == 1


def test_too_many_rows_maps_to_413_without_retry(monkeypatch) -> None:
    client = _FakeClient(
        DatabaseError("Code: 158. DB::Exception: Limit for rows to read exceeded (TOO_MANY_ROWS)")
    )

    monkeypatch.setattr(
        db,
        "get_client",
        lambda send_receive_timeout=None: pytest.fail("safety-limit error must not retry"),
    )

    with pytest.raises(HTTPException) as excinfo:
        run_query(client, "SELECT * FROM taxi_trips")

    assert excinfo.value.status_code == 413
    assert client.calls == 1


def test_success_first_try_does_not_build_retry_client(monkeypatch) -> None:
    client = _FakeClient(_FakeResult(["n"], [(7,)]))

    monkeypatch.setattr(
        db,
        "get_client",
        lambda send_receive_timeout=None: pytest.fail("no retry client on the happy path"),
    )

    rows, meta = run_query(client, "SELECT 7 AS n")

    assert rows == [{"n": 7}]
    assert meta.rows_returned == 1
    assert client.calls == 1


def test_inline_sql_formats_scalars_dates_and_arrays() -> None:
    sql = """
        SELECT * FROM taxi_trips
        WHERE vendor_id = {vendor_id:UInt8}
          AND pickup_datetime >= {start:DateTime}
          AND pickup_zone_id IN {zone_ids:Array(UInt16)}
    """

    rendered = inline_sql(
        sql,
        {
            "vendor_id": 1,
            "start": datetime(2022, 7, 2, 20, 0, 0),
            "zone_ids": [161, 162],
        },
    )

    assert "vendor_id = 1" in rendered
    assert "pickup_datetime >= '2022-07-02 20:00:00'" in rendered
    assert "pickup_zone_id IN [161, 162]" in rendered
    assert "{" not in rendered


def test_inline_sql_escapes_display_strings_and_preserves_unknown_tokens() -> None:
    rendered = inline_sql(
        "SELECT {name:String}, {missing:String}",
        {"name": "O'Reilly\\books"},
    )

    assert rendered == "SELECT 'O\\'Reilly\\\\books', {missing:String}"
