import importlib
from types import SimpleNamespace

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


class BusinessPolicyAdmin:
    def __init__(self, *, datasets=None, scores=None, rules=None):
        self.datasets = datasets or []
        self.scores = scores or []
        self.rules = rules or []
        self.calls = []

    def list_named(self, path, name):
        rows = self.rules if path == online.RULES_PATH else []
        return [row for row in rows if row.get("name") == name]

    def call(self, method, path, body=None):
        self.calls.append((method, path, body))
        if method == "GET" and path.startswith("/api/public/v2/datasets"):
            return {"data": self.datasets}
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


def test_business_policy_online_enable_refuses_without_experiment_score(monkeypatch):
    api = BusinessPolicyAdmin(scores=[])
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
        scores=[
            {"name": "business-policy-adherence-extra", "value": "PASS"},
            {"name": "business-policy-adherence", "value": "PASS"},
        ],
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
