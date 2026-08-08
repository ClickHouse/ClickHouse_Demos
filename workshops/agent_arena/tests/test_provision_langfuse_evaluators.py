from pathlib import Path

import scripts.provision_online_evaluators as online
from scripts.provision_langfuse_evaluators import judge_prompt


def test_judge_prompt_extracts_runtime_prompt_only():
    prompt = judge_prompt()
    assert "You are a senior ClickHouse SQL reviewer" in prompt
    assert "{{question}}" in prompt
    assert "Set this up in" not in prompt


def test_operational_evaluator_create_body_is_exact():
    source = "def evaluate(ctx): pass\n"

    assert online.operational_evaluator_body(source) == {
        "type": "code",
        "name": "sql-execution-success",
        "sourceCode": source,
        "sourceCodeLanguage": "PYTHON",
    }


def test_operational_rule_create_body_is_exact_and_has_no_mapping():
    body = online.operational_rule_body()

    assert body == {
        "name": "agent-arena-sql-execution-online",
        "evaluator": {
            "name": "sql-execution-success",
            "scope": "project",
            "type": "code",
        },
        "target": "observation",
        "enabled": True,
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
    }
    assert "mapping" not in body


class FakeAdmin:
    def __init__(self, existing_by_path=None):
        self.existing_by_path = existing_by_path or {}
        self.calls = []

    def list_named(self, path, name):
        return [
            row for row in self.existing_by_path.get(path, [])
            if row.get("name") == name
        ]

    def call(self, method, path, body=None):
        self.calls.append((method, path, body))
        return {"id": "created-id", **(body or {})}


def test_ensure_evaluator_creates_missing_family(monkeypatch):
    api = FakeAdmin()
    monkeypatch.setattr(online, "_admin", api)
    desired = online.operational_evaluator_body("source-v1")

    result = online.ensure_evaluator("sql-execution-success", desired)

    assert result["id"] == "created-id"
    assert api.calls == [
        ("POST", "/api/public/unstable/evaluators", desired),
    ]


def test_ensure_evaluator_does_not_version_unchanged_family(monkeypatch):
    desired = online.operational_evaluator_body("source-v1")
    existing = {
        "id": "evaluator-v1",
        "name": "sql-execution-success",
        "version": 1,
        **desired,
    }
    api = FakeAdmin({"/api/public/unstable/evaluators": [existing]})
    monkeypatch.setattr(online, "_admin", api)

    assert online.ensure_evaluator("sql-execution-success", desired) == existing
    assert api.calls == []


def test_ensure_evaluator_versions_changed_source(monkeypatch):
    desired = online.operational_evaluator_body("source-v2")
    existing = {
        "id": "evaluator-v1",
        "name": "sql-execution-success",
        "version": 1,
        **online.operational_evaluator_body("source-v1"),
    }
    api = FakeAdmin({"/api/public/unstable/evaluators": [existing]})
    monkeypatch.setattr(online, "_admin", api)

    online.ensure_evaluator("sql-execution-success", desired)

    assert api.calls == [
        ("POST", "/api/public/unstable/evaluators", desired),
    ]


def test_ensure_rule_creates_missing_rule(monkeypatch):
    api = FakeAdmin()
    monkeypatch.setattr(online, "_admin", api)
    desired = online.operational_rule_body()

    online.ensure_rule("agent-arena-sql-execution-online", desired)

    assert api.calls == [
        ("POST", "/api/public/unstable/evaluation-rules", desired),
    ]


def test_ensure_rule_does_not_patch_unchanged_rule(monkeypatch):
    desired = online.operational_rule_body()
    existing = {
        "id": "rule-1",
        "status": "ACTIVE",
        **desired,
        "evaluator": {"id": "evaluator-v1", **desired["evaluator"]},
        "mapping": [{"source": "input", "jsonPath": "$"}],
    }
    api = FakeAdmin({"/api/public/unstable/evaluation-rules": [existing]})
    monkeypatch.setattr(online, "_admin", api)

    assert online.ensure_rule("agent-arena-sql-execution-online", desired) == existing
    assert api.calls == []


def test_ensure_rule_patches_only_changed_fields(monkeypatch):
    desired = online.operational_rule_body()
    existing = {"id": "rule-1", **desired, "enabled": False, "sampling": 0.25}
    api = FakeAdmin({"/api/public/unstable/evaluation-rules": [existing]})
    monkeypatch.setattr(online, "_admin", api)

    online.ensure_rule("agent-arena-sql-execution-online", desired)

    assert api.calls == [
        (
            "PATCH",
            "/api/public/unstable/evaluation-rules/rule-1",
            {"enabled": True, "sampling": 1},
        ),
    ]


def test_operational_evaluator_source_is_loaded_verbatim():
    expected = Path("eval/langfuse_evaluators/sql_execution_success.py").read_text()

    assert online.operational_evaluator_body()["sourceCode"] == expected
