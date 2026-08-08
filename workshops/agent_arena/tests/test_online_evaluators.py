import importlib
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from agents.policy import load_policy
from eval.langfuse_evaluators.sql_execution_success import evaluate
import scripts.provision_online_evaluators as online


def _context(output):
    return SimpleNamespace(observation=SimpleNamespace(output=output))


def test_sql_execution_success_scores_valid_root_output():
    ctx = _context({
        "sql": "SELECT 1",
        "rows": [[1]],
        "error": None,
        "outcome_hint": "ok",
    })

    score = evaluate(ctx).scores[0]

    assert (score.name, score.value, score.data_type) == (
        "sql-execution-success",
        True,
        "BOOLEAN",
    )
    assert "not semantic correctness" in score.comment


@pytest.mark.parametrize(
    "output",
    [
        {"sql": "", "rows": [], "error": None, "outcome_hint": "ok"},
        {"sql": None, "rows": [], "error": None, "outcome_hint": "ok"},
        {"sql": "SELECT 1", "rows": [], "error": "query failed", "outcome_hint": "ok"},
        {"sql": "SELECT 1", "rows": [], "error": None, "outcome_hint": "wrong_result"},
    ],
)
def test_sql_execution_success_rejects_non_executed_outputs(output):
    score = evaluate(_context(output)).scores[0]

    assert (score.name, score.value, score.data_type) == (
        "sql-execution-success",
        False,
        "BOOLEAN",
    )


@pytest.mark.parametrize("outcome_hint", [None, "", "ok"])
def test_sql_execution_success_accepts_execution_hints(outcome_hint):
    output = {
        "sql": " SELECT 1 ",
        "rows": [],
        "error": None,
        "outcome_hint": outcome_hint,
    }

    assert evaluate(_context(output)).scores[0].value is True


@pytest.mark.parametrize(
    "ctx",
    [
        None,
        SimpleNamespace(),
        SimpleNamespace(observation=None),
        SimpleNamespace(observation=SimpleNamespace()),
        _context(None),
        _context("not-an-output-object"),
        _context([{"sql": "SELECT 1"}]),
    ],
)
def test_sql_execution_success_scores_malformed_context_false(ctx):
    result = evaluate(ctx)

    assert len(result.scores) == 1
    score = result.scores[0]
    assert (score.name, score.value, score.data_type) == (
        "sql-execution-success",
        False,
        "BOOLEAN",
    )
    assert "not semantic correctness" in score.comment


def test_business_policy_prompt_renders_catalog_and_preserves_runtime_variables():
    prompt = online.business_policy_prompt()

    assert "{{policy_catalog}}" not in prompt
    assert "{{question}}" in prompt
    assert "{{generated_sql}}" in prompt
    for metric, definition in load_policy("policy-v2").metrics.items():
        assert metric in prompt
        assert definition in prompt
    assert "Do not invent policy" in prompt
    assert "PASS" in prompt
    assert "FAIL" in prompt
    assert "NOT_APPLICABLE" in prompt


def test_business_policy_evaluator_body_is_exact():
    prompt = "rendered policy prompt"

    assert online.business_policy_evaluator_body(prompt) == {
        "type": "llm_as_judge",
        "name": "business-policy-adherence",
        "prompt": prompt,
        "outputDefinition": {
            "dataType": "CATEGORICAL",
            "score": {
                "description": (
                    "Whether generated SQL follows every applicable governed metric."
                ),
                "categories": ["PASS", "FAIL", "NOT_APPLICABLE"],
                "shouldAllowMultipleMatches": False,
            },
            "reasoning": {
                "description": "Name the applicable policy and explain the decision."
            },
        },
        "modelConfig": {
            "provider": "agent-arena-openrouter",
            "model": "openai/gpt-5.6-luna",
        },
    }


def test_business_policy_experiment_rule_is_enabled_and_dataset_scoped():
    assert online.business_policy_experiment_rule_body("dataset-arena-golden") == {
        "name": "business-policy-adherence",
        "evaluator": {
            "name": "business-policy-adherence",
            "scope": "project",
            "type": "llm_as_judge",
        },
        "target": "experiment",
        "enabled": True,
        "sampling": 1,
        "filter": [
            {
                "type": "stringOptions",
                "column": "datasetId",
                "operator": "any of",
                "value": ["dataset-arena-golden"],
            }
        ],
        "mapping": [
            {"variable": "question", "source": "input", "jsonPath": "$.question"},
            {
                "variable": "generated_sql",
                "source": "output",
                "jsonPath": "$.sql",
            },
        ],
    }


def test_business_policy_online_rule_stays_disabled_until_explicit_enable():
    expected = {
        "name": "agent-arena-business-policy-online",
        "evaluator": {
            "name": "business-policy-adherence",
            "scope": "project",
            "type": "llm_as_judge",
        },
        "target": "observation",
        "enabled": False,
        "sampling": 1,
        "filter": [
            {
                "type": "boolean",
                "column": "isRootObservation",
                "operator": "=",
                "value": True,
            },
            {
                "type": "stringOptions",
                "column": "traceName",
                "operator": "any of",
                "value": ["chat_turn"],
            },
        ],
        "mapping": [
            {"variable": "question", "source": "input", "jsonPath": "$.question"},
            {
                "variable": "generated_sql",
                "source": "output",
                "jsonPath": "$.sql",
            },
        ],
    }

    assert online.business_policy_online_rule_body() == expected
    assert online.business_policy_online_rule_body(enabled=True) == {
        **expected,
        "enabled": True,
    }


def _numbered_page(data, page=1, total_pages=1):
    return {
        "data": data,
        "meta": {"page": page, "totalPages": total_pages, "limit": 100},
    }


def _cursor_page(data, cursor=None):
    return {"data": data, "meta": {"cursor": cursor}}


class BusinessPolicyAdmin:
    def __init__(
        self,
        *,
        datasets=None,
        dataset_pages=None,
        scores=None,
        rules=None,
        experiment_pages=None,
    ):
        self.datasets = datasets or []
        self.dataset_pages = dataset_pages
        self.scores = scores or []
        self.rules = rules or []
        self.experiment_pages = experiment_pages or {}
        self.calls = []

    def list_named(self, path, name):
        rows = self.rules if path == online.RULES_PATH else []
        return [row for row in rows if row.get("name") == name]

    def call(self, method, path, body=None):
        self.calls.append((method, path, body))
        if method == "GET" and path.startswith("/api/public/v2/datasets"):
            page = int(parse_qs(urlsplit(path).query).get("page", [1])[0])
            if self.dataset_pages is not None:
                return self.dataset_pages[page - 1]
            return _numbered_page(self.datasets)
        if method == "GET" and path.startswith("/api/public/experiment-items"):
            cursor = parse_qs(urlsplit(path).query).get("cursor", [None])[0]
            return self.experiment_pages.get(cursor, _cursor_page([]))
        if method == "GET" and path.startswith("/api/public/v3/scores"):
            return {"data": self.scores}
        return {"id": "created-id", **(body or {})}


def test_business_policy_experiment_provisioning_creates_disabled_online_rule(
    monkeypatch,
):
    api = BusinessPolicyAdmin(
        datasets=[{"id": "dataset-1", "name": "arena-golden"}]
    )
    monkeypatch.setattr(online, "_admin", api)

    evaluator, experiment_rule, online_rule = (
        online.provision_business_policy_experiments(
            openrouter_api_key="provider-secret",
            openrouter_base_url="https://provider.invalid/v1",
        )
    )

    assert evaluator["name"] == "business-policy-adherence"
    assert experiment_rule["enabled"] is True
    assert online_rule["enabled"] is False
    assert api.calls[0] == (
        "PUT",
        "/api/public/llm-connections",
        {
            "provider": "agent-arena-openrouter",
            "adapter": "openai",
            "secretKey": "provider-secret",
            "baseURL": "https://provider.invalid/v1",
            "customModels": ["openai/gpt-5.6-luna"],
            "withDefaultModels": False,
        },
    )
    created_rules = [
        body for method, path, body in api.calls
        if method == "POST" and path == online.RULES_PATH
    ]
    assert [body["name"] for body in created_rules] == [
        "business-policy-adherence",
        "agent-arena-business-policy-online",
    ]
    assert created_rules[1]["enabled"] is False


def test_business_policy_provisioning_finds_dataset_on_later_page(monkeypatch):
    api = BusinessPolicyAdmin(dataset_pages=[
        _numbered_page([{"id": "other", "name": "other"}], 1, 2),
        _numbered_page([{"id": "dataset-1", "name": "arena-golden"}], 2, 2),
    ])
    monkeypatch.setattr(online, "_admin", api)

    online.provision_business_policy_experiments(
        openrouter_api_key="provider-secret",
        openrouter_base_url="https://provider.invalid/v1",
    )

    dataset_calls = [path for method, path, _ in api.calls
                     if method == "GET" and path.startswith("/api/public/v2/datasets")]
    assert dataset_calls == [
        "/api/public/v2/datasets?page=1&limit=100",
        "/api/public/v2/datasets?page=2&limit=100",
    ]


def test_business_policy_dataset_paging_stops_on_repeated_page(monkeypatch):
    api = BusinessPolicyAdmin(dataset_pages=[
        _numbered_page([{"id": "other-1", "name": "other"}], 1, 3),
        _numbered_page([{"id": "other-2", "name": "other"}], 1, 3),
    ])
    monkeypatch.setattr(online, "_admin", api)

    with pytest.raises(RuntimeError, match="arena-golden"):
        online.provision_business_policy_experiments(
            openrouter_api_key="provider-secret",
            openrouter_base_url="https://provider.invalid/v1",
        )

    dataset_calls = [path for method, path, _ in api.calls
                     if method == "GET" and path.startswith("/api/public/v2/datasets")]
    assert len(dataset_calls) == 2


def _experiment_item(*, dataset_id=None, score_name="business-policy-adherence"):
    item = {
        "experimentId": "experiment-1",
        "experimentName": "online-loop-candidate--policy-v2__winner",
        "traceId": "trace-1",
        "scores": [{"name": score_name, "value": "PASS"}],
    }
    if dataset_id is not None:
        item["experimentDatasetId"] = dataset_id
    return item


def test_business_policy_enable_rejects_global_or_wrong_source_score(monkeypatch):
    api = BusinessPolicyAdmin(
        datasets=[{"id": "dataset-1", "name": "arena-golden"}],
        scores=[{"name": "business-policy-adherence", "value": "PASS"}],
        experiment_pages={None: _cursor_page([
            _experiment_item(dataset_id="different-dataset"),
        ])},
    )
    monkeypatch.setattr(online, "_admin", api)

    assert online._business_policy_experiment_score_exists() is False


def test_business_policy_enable_rejects_score_without_experiment_provenance(
    monkeypatch,
):
    api = BusinessPolicyAdmin(
        datasets=[{"id": "dataset-1", "name": "arena-golden"}],
        experiment_pages={None: _cursor_page([_experiment_item()])},
    )
    monkeypatch.setattr(online, "_admin", api)

    assert online._business_policy_experiment_score_exists() is False


def test_business_policy_enable_finds_valid_experiment_score_on_later_page(
    monkeypatch,
):
    api = BusinessPolicyAdmin(
        datasets=[{"id": "dataset-1", "name": "arena-golden"}],
        experiment_pages={
            None: _cursor_page([
                _experiment_item(
                    dataset_id="dataset-1",
                    score_name="business-policy-adherence-extra",
                ),
            ], cursor="next page"),
            "next page": _cursor_page([
                _experiment_item(dataset_id="dataset-1"),
            ]),
        },
    )
    monkeypatch.setattr(online, "_admin", api)
    monkeypatch.setattr(
        online,
        "_experiment_items_from_start",
        lambda: "2026-07-09T00:00:00Z",
        raising=False,
    )

    assert online._business_policy_experiment_score_exists() is True
    experiment_calls = [path for method, path, _ in api.calls
                        if method == "GET" and path.startswith("/api/public/experiment-items")]
    assert experiment_calls == [
        "/api/public/experiment-items?fromStartTime=2026-07-09T00%3A00%3A00Z&fields=core%2Cdataset%2Cio%2Cmetadata%2CitemMetadata%2CexperimentMetadata%2Cscores&limit=100&scoreLimit=50",
        "/api/public/experiment-items?fromStartTime=2026-07-09T00%3A00%3A00Z&fields=core%2Cdataset%2Cio%2Cmetadata%2CitemMetadata%2CexperimentMetadata%2Cscores&limit=100&scoreLimit=50&cursor=next+page",
    ]


def test_business_policy_online_enable_refuses_without_experiment_score(monkeypatch):
    api = BusinessPolicyAdmin(
        datasets=[{"id": "dataset-1", "name": "arena-golden"}],
    )
    monkeypatch.setattr(online, "_admin", api)

    with pytest.raises(RuntimeError, match="experiment score"):
        online.enable_business_policy_online()

    assert not any(
        method in {"POST", "PATCH"} and path == online.RULES_PATH
        for method, path, _body in api.calls
    )


def test_business_policy_online_enable_accepts_exact_named_score(monkeypatch):
    existing = {
        "id": "online-rule-1",
        **online.business_policy_online_rule_body(enabled=False),
    }
    api = BusinessPolicyAdmin(
        datasets=[{"id": "dataset-1", "name": "arena-golden"}],
        experiment_pages={None: _cursor_page([
            _experiment_item(dataset_id="dataset-1"),
        ])},
        rules=[existing],
    )
    monkeypatch.setattr(online, "_admin", api)

    online.enable_business_policy_online()

    assert api.calls[-1] == (
        "PATCH",
        "/api/public/unstable/evaluation-rules/online-rule-1",
        {"enabled": True},
    )


def test_score_verifier_normalizes_boolean_and_categorical_values():
    verifier = importlib.import_module("scripts.verify_online_scores")

    assert verifier.normalized_score_value(
        {"dataType": "BOOLEAN", "value": 1}
    ) == "true"
    assert verifier.normalized_score_value(
        {"dataType": "BOOLEAN", "value": False}
    ) == "false"
    assert verifier.normalized_score_value(
        {"dataType": "CATEGORICAL", "value": "PASS"}
    ) == "PASS"
    assert verifier.normalized_score_value(
        {"dataType": "CATEGORICAL", "value": None, "string": "NOT_APPLICABLE"}
    ) == "NOT_APPLICABLE"


def test_score_verifier_reports_only_safe_fields(capsys):
    verifier = importlib.import_module("scripts.verify_online_scores")
    scores = [
        {
            "name": "business-policy-adherence",
            "dataType": "CATEGORICAL",
            "value": "PASS",
            "comment": "revenue policy followed",
            "traceId": "trace-1",
            "input": {"question": "secret body"},
        }
    ]

    verifier.print_scores("trace-1", scores)

    output = capsys.readouterr().out
    assert output == (
        "trace_id=trace-1\n"
        "business-policy-adherence=PASS comment=revenue policy followed\n"
    )
    assert "secret body" not in output


def test_score_verifier_finds_missing_and_mismatched_names():
    verifier = importlib.import_module("scripts.verify_online_scores")
    scores = [
        {"name": "sql-execution-success", "dataType": "BOOLEAN", "value": True},
        {
            "name": "business-policy-adherence",
            "dataType": "CATEGORICAL",
            "value": "FAIL",
        },
    ]

    assert verifier.score_mismatches(
        scores,
        {
            "sql-execution-success": "true",
            "business-policy-adherence": "PASS",
            "correctness": "1",
        },
    ) == [
        "business-policy-adherence expected PASS got FAIL",
        "correctness missing",
    ]


def test_score_verifier_reads_later_pages_and_filters_trace_exactly():
    verifier = importlib.import_module("scripts.verify_online_scores")

    class PagedAdmin:
        def __init__(self):
            self.calls = []

        def call(self, method, path, body=None):
            self.calls.append((method, path, body))
            cursor = parse_qs(urlsplit(path).query).get("cursor", [None])[0]
            if cursor is None:
                return _cursor_page([
                    {"traceId": "other", "name": "correctness", "value": 0},
                    {"traceId": "trace 1/a", "name": "correctness", "value": 1},
                ], cursor="next page")
            return _cursor_page([
                {"traceId": "trace 1/a", "name": "business-policy-adherence",
                 "value": "PASS", "dataType": "CATEGORICAL"},
            ])

    api = PagedAdmin()

    assert [score["name"] for score in verifier._fetch_scores(api, "trace 1/a")] == [
        "correctness",
        "business-policy-adherence",
    ]
    assert [path for _, path, _ in api.calls] == [
        "/api/public/v3/scores?traceId=trace+1%2Fa&fields=subject&limit=100",
        "/api/public/v3/scores?traceId=trace+1%2Fa&fields=subject&limit=100&cursor=next+page",
    ]


def test_score_verifier_stops_when_cursor_repeats():
    verifier = importlib.import_module("scripts.verify_online_scores")

    class RepeatingAdmin:
        def __init__(self):
            self.calls = 0

        def call(self, method, path, body=None):
            self.calls += 1
            return _cursor_page([], cursor="same")

    api = RepeatingAdmin()

    assert verifier._fetch_scores(api, "trace-1") == []
    assert api.calls == 2


def test_score_verifier_accepts_live_terminal_metadata_and_subject_trace():
    verifier = importlib.import_module("scripts.verify_online_scores")

    class LiveShapeAdmin:
        def call(self, method, path, body=None):
            return {
                "data": [{
                    "subject": {"kind": "trace", "traceId": "trace-1"},
                    "name": "business-policy-adherence",
                    "value": "PASS",
                    "dataType": "CATEGORICAL",
                }],
                "meta": {"limit": 100},
            }

    scores = verifier._fetch_scores(LiveShapeAdmin(), "trace-1")

    assert [score["name"] for score in scores] == [
        "business-policy-adherence",
    ]
