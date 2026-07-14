from __future__ import annotations

import pytest
from fastapi import HTTPException

from app.chat_service import (
    SqlGuardrailError,
    _parse_plan_json,
    get_schema_text,
    sanitize_select_sql,
)
from app.settings import settings


@pytest.fixture(scope="session", autouse=True)
def wait_for_api() -> None:
    # Override the integration-test fixture from conftest.py: these are pure unit
    # tests for the SQL guardrails and do not need a running API / ClickHouse.
    return None


# --- SELECT-only enforcement ---------------------------------------------

def test_accepts_plain_select() -> None:
    sql = "SELECT count() AS trips FROM taxi_trips LIMIT 10"
    assert sanitize_select_sql(sql) == sql


def test_accepts_with_cte() -> None:
    sql = (
        "WITH a AS (SELECT pickup_location_id AS z, count() AS c FROM taxi_trips GROUP BY z) "
        "SELECT z, c FROM a ORDER BY c DESC LIMIT 5"
    )
    assert sanitize_select_sql(sql) == sql


def test_strips_trailing_semicolon() -> None:
    out = sanitize_select_sql("SELECT 1 AS x LIMIT 1;")
    assert out == "SELECT 1 AS x LIMIT 1"


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO taxi_trips VALUES (1)",
        "UPDATE taxi_trips SET fare_amount = 0",
        "DELETE FROM taxi_trips",
        "DROP TABLE taxi_trips",
        "CREATE TABLE t (a Int)",
        "ALTER TABLE taxi_trips DELETE WHERE 1",
        "TRUNCATE TABLE taxi_trips",
        "SYSTEM RELOAD CONFIG",
    ],
)
def test_rejects_non_select(sql: str) -> None:
    with pytest.raises(SqlGuardrailError):
        sanitize_select_sql(sql)


def test_rejects_write_keyword_after_select() -> None:
    # A SELECT that smuggles a forbidden keyword must still be rejected.
    with pytest.raises(SqlGuardrailError):
        sanitize_select_sql("SELECT 1 INTO OUTFILE '/tmp/x' LIMIT 1")


def test_rejects_multi_statement() -> None:
    with pytest.raises(SqlGuardrailError):
        sanitize_select_sql("SELECT 1 AS x LIMIT 1; DROP TABLE taxi_trips")


def test_rejects_comment_hidden_second_statement() -> None:
    # Comment stripping must expose the hidden DROP so the multi-statement check fires.
    with pytest.raises(SqlGuardrailError):
        sanitize_select_sql("SELECT 1 AS x LIMIT 1 -- ok\n; DROP TABLE taxi_trips")


def test_rejects_empty() -> None:
    with pytest.raises(SqlGuardrailError):
        sanitize_select_sql("   ")


# --- LIMIT injection ------------------------------------------------------

def test_appends_limit_when_absent() -> None:
    out = sanitize_select_sql("SELECT zone FROM taxi_zones")
    assert out.rstrip().endswith(f"LIMIT {settings.chat_row_limit}")


def test_keeps_existing_limit() -> None:
    out = sanitize_select_sql("SELECT zone FROM taxi_zones LIMIT 3")
    assert out.count("LIMIT") == 1
    assert out.rstrip().endswith("LIMIT 3")


def test_limit_case_insensitive() -> None:
    out = sanitize_select_sql("SELECT zone FROM taxi_zones limit 3")
    # An existing lowercase limit must not trigger a second appended LIMIT.
    assert out.lower().count("limit") == 1


# --- Plan JSON parsing (JSON mode + fenced fallback) ----------------------

def test_parse_plain_json() -> None:
    assert _parse_plan_json('{"answer": "hi", "sql": null}') == {"answer": "hi", "sql": None}


def test_parse_json_in_code_fence() -> None:
    content = '```json\n{"answer": "hi", "sql": "SELECT 1"}\n```'
    assert _parse_plan_json(content) == {"answer": "hi", "sql": "SELECT 1"}


def test_parse_json_with_surrounding_prose() -> None:
    content = 'Here is the result:\n{"answer": "hi", "sql": "SELECT 1"}\nHope that helps.'
    assert _parse_plan_json(content)["sql"] == "SELECT 1"


def test_parse_invalid_json_raises() -> None:
    with pytest.raises(HTTPException):
        _parse_plan_json("not json at all")


# --- Schema fallback ------------------------------------------------------

def test_schema_fallback_without_client() -> None:
    text = get_schema_text(None)
    assert "taxi_trips" in text
    assert "taxi_zones" in text
