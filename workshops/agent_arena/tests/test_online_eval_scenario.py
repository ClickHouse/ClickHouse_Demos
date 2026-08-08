from types import SimpleNamespace

import pytest

from scripts.check_online_eval_scenario import (
    _require_stale_classification,
    classify_active_customer_sql,
    verify_reference_counts,
)


def test_classifies_stale_signup_definition():
    assert classify_active_customer_sql(
        "SELECT count() FROM v_customers "
        "WHERE signup_date >= today()-INTERVAL 90 DAY"
    ) == "policy-v1"


def test_classifies_current_order_definition():
    sql = """SELECT uniqExact(customer_id) FROM v_orders
             WHERE order_ts >= now()-INTERVAL 30 DAY
             AND status NOT IN ('cancelled','returned')"""
    assert classify_active_customer_sql(sql) == "policy-v2"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT uniqExact(customer_id) FROM v_orders "
        "WHERE order_ts >= now()-INTERVAL 30 DAY",
        "SELECT uniqExact(customer_id) FROM v_orders "
        "WHERE order_ts >= now()-INTERVAL 30 DAY "
        "AND status != 'cancelled'",
        "SELECT count() FROM v_customers "
        "WHERE signup_date >= today()-INTERVAL 30 DAY",
        "SELECT count() FROM v_customers "
        "WHERE signup_date >= today()-INTERVAL 90 DAY "
        "AND customer_id IN (SELECT customer_id FROM v_orders)",
        "SELECT uniqExact(customer_id) FROM v_orders "
        "WHERE order_ts >= now()-INTERVAL 30 DAY "
        "AND status NOT IN ('cancelled','returned') "
        "AND customer_id IN (SELECT customer_id FROM v_customers "
        "WHERE signup_date >= today()-INTERVAL 90 DAY)",
    ],
)
def test_rejects_partial_and_hybrid_definitions(sql):
    assert classify_active_customer_sql(sql) == "unknown"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count() FROM v_customers c "
        "WHERE c.signup_date >= today() - INTERVAL 90 DAYS",
        "select count(*) from arena.v_customers "
        "where signup_date >= now() - interval 90 day",
    ],
)
def test_classifies_stale_format_variants(sql):
    assert classify_active_customer_sql(sql) == "policy-v1"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count(DISTINCT customer_id) FROM v_orders "
        "WHERE order_ts >= today() - INTERVAL 30 DAYS "
        "AND status NOT IN ('returned', 'cancelled')",
        "SELECT uniqExact(o.customer_id) FROM arena.v_orders AS o "
        "WHERE o.order_ts >= now() - INTERVAL 30 DAY "
        "AND o.status != 'cancelled' AND o.status != 'returned'",
    ],
)
def test_classifies_current_format_variants(sql):
    assert classify_active_customer_sql(sql) == "policy-v2"


def test_rejects_equal_reference_counts():
    with pytest.raises(RuntimeError, match="same value"):
        verify_reference_counts(12, 12)


def test_accepts_different_reference_counts():
    verify_reference_counts(12, 13)


def test_reports_model_block_without_leaking_provider_error():
    result = SimpleNamespace(
        sql=None,
        error="provider payload containing sensitive request details",
        outcome_hint="model_error",
    )

    with pytest.raises(RuntimeError, match="provider/model call failed") as error:
        _require_stale_classification(result, 2)

    assert result.error not in str(error.value)


def test_rejects_stale_shaped_sql_that_failed_to_execute():
    result = SimpleNamespace(
        sql=STALE_SQL_FOR_RESULT,
        error="database error containing the generated SQL",
        outcome_hint="sql_exec_error",
    )

    with pytest.raises(RuntimeError, match="agent execution failed") as error:
        _require_stale_classification(result, 1)

    assert result.error not in str(error.value)


STALE_SQL_FOR_RESULT = (
    "SELECT count() FROM v_customers "
    "WHERE signup_date >= today()-INTERVAL 90 DAY"
)
