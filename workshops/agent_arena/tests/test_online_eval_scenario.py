import sys
from types import SimpleNamespace

import pytest

import scripts.check_online_eval_scenario as scenario
from scripts.check_online_eval_scenario import (
    CURRENT_SQL,
    SEEDED_QUESTIONS,
    STALE_SQL,
    _require_stale_classification,
    _resolve_config,
    _run,
    _scalar_count,
    classify_active_customer_sql,
    main,
    verify_reference_counts,
)


class FakeConfig:
    def __init__(self):
        self.clickhouse = SimpleNamespace()
        self.openrouter = SimpleNamespace(
            base_url="https://provider.invalid/api", api_key="unused",
            inference={"temperature": 0.8, "max_tokens": 64},
        )
        self.eval = SimpleNamespace(default_max_retries=1)
        self.model = SimpleNamespace(name="winner", id="provider/model")
        self.prompt = SimpleNamespace(name="P1_zeroshot", self_correct=False)

    def model_by_name(self, name):
        if name != "winner":
            raise StopIteration
        return self.model

    def prompt_by_name(self, name):
        if name != "P1_zeroshot":
            raise StopIteration
        return self.prompt


def _install_success_boundaries(monkeypatch, run_agent_fn):
    import agents.chclient as chclient
    import agents.llm as llm
    import agents.loop as loop
    import arena.config as config

    class FakeROClient:
        def __init__(self, cfg):
            self.cfg = cfg

        def query(self, sql):
            if sql == STALE_SQL:
                return SimpleNamespace(rows=[(1923,)])
            if sql == CURRENT_SQL:
                return SimpleNamespace(rows=[(1564,)])
            raise AssertionError("unexpected reference SQL")

    class FakeLLM:
        def __init__(self, base_url, api_key):
            self.base_url = base_url

    monkeypatch.setattr(config, "load_config", FakeConfig)
    monkeypatch.setattr(chclient, "ROClickHouseClient", FakeROClient)
    monkeypatch.setattr(llm, "OpenRouterClient", FakeLLM)
    monkeypatch.setattr(loop, "run_agent", run_agent_fn)


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


def test_rejects_raw_rows_even_when_policy_tokens_are_present():
    sql = """SELECT customer_id FROM v_orders
             WHERE order_ts >= now()-INTERVAL 30 DAY
             AND status NOT IN ('cancelled','returned')"""
    assert classify_active_customer_sql(sql) == "unknown"


def test_rejects_current_count_without_distinct_customer_aggregation():
    sql = """SELECT count(customer_id) FROM v_orders
             WHERE order_ts >= now()-INTERVAL 30 DAY
             AND status NOT IN ('cancelled','returned')"""
    assert classify_active_customer_sql(sql) == "unknown"


def test_rejects_or_tautology_in_status_filter():
    sql = """SELECT uniqExact(customer_id) FROM v_orders
             WHERE order_ts >= now()-INTERVAL 30 DAY
             AND status != 'cancelled' AND status != 'returned' OR 1=1"""
    assert classify_active_customer_sql(sql) == "unknown"


def test_rejects_or_tautology_in_stale_window_filter():
    sql = """SELECT count() FROM v_customers
             WHERE signup_date >= today()-INTERVAL 90 DAY OR 1=1"""
    assert classify_active_customer_sql(sql) == "unknown"


def test_rejects_extra_join_that_can_multiply_the_scalar_count():
    sql = """SELECT count() FROM v_customers AS c
             JOIN v_products AS p ON 1=1
             WHERE c.signup_date >= today()-INTERVAL 90 DAY"""
    assert classify_active_customer_sql(sql) == "unknown"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count() FROM v_customers AS c1 "
        "JOIN v_customers AS c2 ON c1.customer_id = c2.customer_id "
        "WHERE c1.signup_date >= today()-INTERVAL 90 DAY",
        "SELECT uniqExact(o1.customer_id) FROM v_orders AS o1 "
        "JOIN v_orders AS o2 ON o1.order_id = o2.order_id "
        "WHERE o1.order_ts >= now()-INTERVAL 30 DAY "
        "AND o1.status NOT IN ('cancelled','returned')",
    ],
)
def test_rejects_same_view_self_joins(sql):
    assert classify_active_customer_sql(sql) == "unknown"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count() FROM v_customers AS c, v_products AS p "
        "WHERE c.signup_date >= today()-INTERVAL 90 DAY",
        "SELECT uniqExact(o.customer_id) FROM v_orders AS o, v_products AS p "
        "WHERE o.order_ts >= now()-INTERVAL 30 DAY "
        "AND o.status NOT IN ('cancelled','returned')",
    ],
)
def test_rejects_comma_join_relations(sql):
    assert classify_active_customer_sql(sql) == "unknown"


@pytest.mark.parametrize("suffix", ["1=0", "0=1", "false", "0"])
@pytest.mark.parametrize(
    "base_sql",
    [
        "SELECT count() FROM v_customers "
        "WHERE signup_date >= today()-INTERVAL 90 DAY AND ",
        "SELECT uniqExact(customer_id) FROM v_orders "
        "WHERE order_ts >= now()-INTERVAL 30 DAY "
        "AND status NOT IN ('cancelled','returned') AND ",
    ],
)
def test_rejects_unconditional_contradictions_for_both_policies(base_sql, suffix):
    assert classify_active_customer_sql(base_sql + suffix) == "unknown"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT uniqExact(customer_id) FROM v_orders "
        "WHERE order_ts >= now()-INTERVAL 30 DAY "
        "AND NOT status != 'cancelled' AND status != 'returned'",
        "SELECT uniqExact(customer_id) FROM v_orders "
        "WHERE order_ts >= now()-INTERVAL 30 DAY "
        "AND status NOT IN ('cancelled','returned') AND status = 'cancelled'",
    ],
)
def test_rejects_negated_or_contradictory_status_filters(sql):
    assert classify_active_customer_sql(sql) == "unknown"


def test_comment_markers_inside_literals_do_not_hide_real_policy_sql():
    sql = """SELECT count() FROM v_customers
             WHERE '-- still a string' != '/* also a string */'
             AND signup_date >= today()-INTERVAL 90 DAY"""
    assert classify_active_customer_sql(sql) == "policy-v1"


def test_rejects_comment_and_string_policy_decoys():
    sql = """SELECT customer_id FROM v_orders
             WHERE note = 'uniqExact(customer_id) status NOT IN (returned,cancelled)'
             /* order_ts >= now()-INTERVAL 30 DAY */"""
    assert classify_active_customer_sql(sql) == "unknown"


def test_classifies_schema_alias_and_backtick_status_identifier():
    sql = """SELECT uniqExact(o.customer_id) FROM arena.v_orders AS o
             WHERE o.order_ts >= now()-INTERVAL 30 DAY
             AND o.`status` NOT IN ('returned', 'cancelled')"""
    assert classify_active_customer_sql(sql) == "policy-v2"


def test_rejects_cte_wrapping_otherwise_valid_stale_query():
    sql = """WITH 1 AS marker
             SELECT count() FROM v_customers AS c
             WHERE c.signup_date >= today()-INTERVAL 90 DAY"""
    assert classify_active_customer_sql(sql) == "unknown"


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT count() FROM v_customers AS c "
        "WHERE c.signup_date >= today()-INTERVAL 90 DAY "
        "AND c.customer_id IN (SELECT 1)",
        "SELECT uniqExact(o.customer_id) FROM v_orders AS o "
        "WHERE o.order_ts >= now()-INTERVAL 30 DAY "
        "AND o.status NOT IN ('cancelled','returned') "
        "AND o.customer_id IN (SELECT 1)",
    ],
)
def test_rejects_fromless_nested_select_predicates(sql):
    assert classify_active_customer_sql(sql) == "unknown"


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        (
            "SELECT count() FROM arena.v_customers AS c "
            "WHERE c.signup_date >= today()-INTERVAL 90 DAY",
            "policy-v1",
        ),
        (
            "SELECT uniqExact(o.customer_id) FROM arena.v_orders AS o "
            "WHERE o.order_ts >= now()-INTERVAL 30 DAY "
            "AND o.`status` NOT IN ('cancelled','returned')",
            "policy-v2",
        ),
    ],
)
def test_preserves_one_schema_qualified_aliased_relation(sql, expected):
    assert classify_active_customer_sql(sql) == expected


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


def test_scalar_count_accepts_one_integer_cell():
    assert _scalar_count(SimpleNamespace(rows=[(17,)]), "stale") == 17


@pytest.mark.parametrize(
    "rows",
    [[], [(1,), (2,)], [(1, 2)], [(True,)], [("17",)]],
)
def test_scalar_count_rejects_non_scalar_or_non_integer_results(rows):
    with pytest.raises(RuntimeError, match="stale reference query"):
        _scalar_count(SimpleNamespace(rows=rows), "stale")


def test_config_resolution_sanitizes_lookup_exceptions():
    class ExplosiveConfig:
        def model_by_name(self, name):
            raise ValueError("SECRET_MARKER model lookup payload")

    with pytest.raises(RuntimeError, match="configuration selection failed") as error:
        _resolve_config(ExplosiveConfig(), "winner__P1_zeroshot")
    assert "SECRET_MARKER" not in str(error.value)


def test_config_resolution_rejects_malformed_id():
    with pytest.raises(RuntimeError, match="invalid config-id format"):
        _resolve_config(FakeConfig(), "not-a-pair")


def test_config_resolution_rejects_unknown_selection_without_echoing_it():
    supplied = "SECRET_MARKER__P1_zeroshot"
    with pytest.raises(RuntimeError, match="unknown selected config") as error:
        _resolve_config(FakeConfig(), supplied)
    assert supplied not in str(error.value)


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


def _agent_result(sql=STALE_SQL, *, error=None, outcome_hint="ok"):
    return SimpleNamespace(sql=sql, error=error, outcome_hint=outcome_hint)


def _scripted_agent(calls, responses):
    remaining = {question: list(results) for question, results in responses.items()}

    def run_agent(question, *args, **kwargs):
        calls.append(question)
        result = remaining[question].pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    return run_agent


def test_retries_one_ok_unknown_question_once_and_uses_policy_v1_retry(
    monkeypatch, capsys,
):
    calls = []
    responses = {
        SEEDED_QUESTIONS[0]: [_agent_result()],
        SEEDED_QUESTIONS[1]: [_agent_result()],
        SEEDED_QUESTIONS[2]: [
            _agent_result("SELECT customer_id FROM v_customers"),
            _agent_result(),
        ],
    }
    _install_success_boundaries(monkeypatch, _scripted_agent(calls, responses))

    _run("winner__P1_zeroshot")

    assert calls == [*SEEDED_QUESTIONS, SEEDED_QUESTIONS[2]]
    assert capsys.readouterr().out.splitlines() == [
        "stale_count=1923",
        "current_count=1564",
        "selected_config=winner__P1_zeroshot",
        "classification_1=policy-v1",
        "classification_2=policy-v1",
        "classification_3=policy-v1",
        "OK: seeded online-evaluation incident is reproducible",
    ]


def test_ok_unknown_retry_stops_after_second_unknown_and_reports_final_result(
    monkeypatch,
):
    calls = []
    unknown = _agent_result("SELECT customer_id FROM v_customers")
    responses = {
        SEEDED_QUESTIONS[0]: [_agent_result()],
        SEEDED_QUESTIONS[1]: [_agent_result()],
        SEEDED_QUESTIONS[2]: [unknown, unknown],
    }
    _install_success_boundaries(monkeypatch, _scripted_agent(calls, responses))

    with pytest.raises(RuntimeError, match="q3=ok/unknown"):
        _run("winner__P1_zeroshot")

    assert calls == [*SEEDED_QUESTIONS, SEEDED_QUESTIONS[2]]


def test_ok_unknown_retry_to_policy_v2_fails_with_final_safe_classification(
    monkeypatch,
):
    calls = []
    responses = {
        SEEDED_QUESTIONS[0]: [_agent_result()],
        SEEDED_QUESTIONS[1]: [_agent_result()],
        SEEDED_QUESTIONS[2]: [
            _agent_result("SELECT customer_id FROM v_customers"),
            _agent_result(CURRENT_SQL),
        ],
    }
    _install_success_boundaries(monkeypatch, _scripted_agent(calls, responses))

    with pytest.raises(RuntimeError, match="q3=ok/policy-v2"):
        _run("winner__P1_zeroshot")

    assert calls == [*SEEDED_QUESTIONS, SEEDED_QUESTIONS[2]]


def test_policy_v2_first_result_fails_without_retry(monkeypatch):
    calls = []
    responses = {
        SEEDED_QUESTIONS[0]: [_agent_result()],
        SEEDED_QUESTIONS[1]: [_agent_result()],
        SEEDED_QUESTIONS[2]: [_agent_result(CURRENT_SQL)],
    }
    _install_success_boundaries(monkeypatch, _scripted_agent(calls, responses))

    with pytest.raises(RuntimeError, match="q3=ok/policy-v2"):
        _run("winner__P1_zeroshot")

    assert calls == SEEDED_QUESTIONS


@pytest.mark.parametrize(
    ("final_result", "safe_failure"),
    [
        (RuntimeError("SECRET_MARKER provider payload"), "q3=call_error/unknown"),
        (
            _agent_result(
                None,
                error="SECRET_MARKER model payload",
                outcome_hint="model_error",
            ),
            "q3=model_error/unknown",
        ),
    ],
)
def test_call_and_model_errors_are_not_retried(
    monkeypatch, final_result, safe_failure,
):
    calls = []
    responses = {
        SEEDED_QUESTIONS[0]: [_agent_result()],
        SEEDED_QUESTIONS[1]: [_agent_result()],
        SEEDED_QUESTIONS[2]: [final_result],
    }
    _install_success_boundaries(monkeypatch, _scripted_agent(calls, responses))

    with pytest.raises(RuntimeError, match=safe_failure) as error:
        _run("winner__P1_zeroshot")

    assert calls == SEEDED_QUESTIONS
    assert "SECRET_MARKER" not in str(error.value)


def test_attempts_all_questions_after_bad_and_thrown_results(monkeypatch):
    calls = []

    def fake_run_agent(question, *args, **kwargs):
        calls.append(question)
        if len(calls) == 1:
            return SimpleNamespace(
                sql=CURRENT_SQL,
                error=None,
                outcome_hint="ok",
            )
        if len(calls) == 2:
            raise RuntimeError("SECRET_MARKER provider payload")
        return SimpleNamespace(sql=STALE_SQL, error=None, outcome_hint="ok")

    _install_success_boundaries(monkeypatch, fake_run_agent)

    with pytest.raises(RuntimeError, match="seeded verification failed") as error:
        _run("winner__P1_zeroshot")

    assert calls == SEEDED_QUESTIONS
    assert "q1=ok/policy-v2" in str(error.value)
    assert "q2=call_error/unknown" in str(error.value)
    assert "SECRET_MARKER" not in str(error.value)


def test_propagates_stale_policy_and_zero_temperature_with_allowlisted_output(
    monkeypatch, capsys,
):
    calls = []

    def fake_run_agent(*args, **kwargs):
        calls.append((args, kwargs))
        return SimpleNamespace(sql=STALE_SQL, error=None, outcome_hint="ok")

    _install_success_boundaries(monkeypatch, fake_run_agent)
    _run("winner__P1_zeroshot")

    assert len(calls) == 3
    assert [call[0][0] for call in calls] == SEEDED_QUESTIONS
    assert all("Business metric policy (policy-v1)" in call[0][3] for call in calls)
    assert all(call[0][6]["temperature"] == 0.0 for call in calls)
    assert capsys.readouterr().out.splitlines() == [
        "stale_count=1923",
        "current_count=1564",
        "selected_config=winner__P1_zeroshot",
        "classification_1=policy-v1",
        "classification_2=policy-v1",
        "classification_3=policy-v1",
        "OK: seeded online-evaluation incident is reproducible",
    ]


def test_policy_boundary_exception_is_sanitized(monkeypatch):
    import agents.policy as policy
    import arena.config as config

    monkeypatch.setattr(config, "load_config", FakeConfig)
    monkeypatch.setattr(
        policy,
        "load_policy",
        lambda version: (_ for _ in ()).throw(ValueError("SECRET_MARKER policy")),
    )

    with pytest.raises(RuntimeError, match="policy context construction failed") as error:
        _run("winner__P1_zeroshot")
    assert "SECRET_MARKER" not in str(error.value)


@pytest.mark.parametrize("failing_sql, expected_phase", [
    (STALE_SQL, "stale reference query failed"),
    (CURRENT_SQL, "current reference query failed"),
])
def test_reference_query_exceptions_are_phase_specific_and_sanitized(
    monkeypatch, failing_sql, expected_phase,
):
    import agents.chclient as chclient
    import arena.config as config

    class ExplosiveROClient:
        def __init__(self, cfg):
            pass

        def query(self, sql):
            if sql == failing_sql:
                raise RuntimeError("SECRET_MARKER ClickHouse payload")
            return SimpleNamespace(rows=[(1,)])

    monkeypatch.setattr(config, "load_config", FakeConfig)
    monkeypatch.setattr(chclient, "ROClickHouseClient", ExplosiveROClient)

    with pytest.raises(RuntimeError, match=expected_phase) as error:
        _run("winner__P1_zeroshot")
    assert "SECRET_MARKER" not in str(error.value)


def test_main_turns_unexpected_exception_into_fixed_blocked_message(monkeypatch, capsys):
    monkeypatch.setattr(
        scenario,
        "_run",
        lambda config_id: (_ for _ in ()).throw(
            ValueError("SECRET_MARKER unexpected payload")
        ),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["check_online_eval_scenario", "--config-id", "winner__P1_zeroshot"],
    )

    with pytest.raises(SystemExit) as error:
        main()

    assert error.value.code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == "BLOCKED: unexpected preflight failure\n"
    assert "SECRET_MARKER" not in captured.err
    assert "Traceback" not in captured.err


@pytest.mark.parametrize(
    ("supplied", "expected"),
    [
        ("SECRET_MARKER", "BLOCKED: invalid config-id format\n"),
        ("SECRET_MARKER__P1_zeroshot", "BLOCKED: unknown selected config\n"),
    ],
)
def test_cli_redacts_malformed_and_unknown_config_ids(
    monkeypatch, capsys, supplied, expected,
):
    import arena.config as config

    monkeypatch.setattr(config, "load_config", FakeConfig)
    monkeypatch.setattr(
        sys,
        "argv",
        ["check_online_eval_scenario", "--config-id", supplied],
    )

    with pytest.raises(SystemExit) as error:
        main()

    assert error.value.code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == expected
    assert "SECRET_MARKER" not in captured.err
    assert "Traceback" not in captured.err


STALE_SQL_FOR_RESULT = (
    "SELECT count() FROM v_customers "
    "WHERE signup_date >= today()-INTERVAL 90 DAY"
)
