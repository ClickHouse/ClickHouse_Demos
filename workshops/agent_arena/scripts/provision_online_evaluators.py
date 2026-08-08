"""Provision the online SQL execution evaluator and observation rule."""

import argparse
import os
from pathlib import Path

from dotenv import load_dotenv

from scripts.langfuse_admin import LangfuseAdmin

EVALUATOR = "sql-execution-success"
RULE = "agent-arena-sql-execution-online"
EVALUATORS_PATH = "/api/public/unstable/evaluators"
RULES_PATH = "/api/public/unstable/evaluation-rules"
SOURCE_PATH = (
    Path(__file__).resolve().parents[1]
    / "eval"
    / "langfuse_evaluators"
    / "sql_execution_success.py"
)

_admin: LangfuseAdmin | None = None


def operational_evaluator_body(source: str | None = None) -> dict:
    if source is None:
        source = SOURCE_PATH.read_text()
    return {
        "type": "code",
        "name": EVALUATOR,
        "sourceCode": source,
        "sourceCodeLanguage": "PYTHON",
    }


def operational_rule_body() -> dict:
    return {
        "name": RULE,
        "evaluator": {"name": EVALUATOR, "scope": "project", "type": "code"},
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


def _require_admin() -> LangfuseAdmin:
    if _admin is None:
        raise RuntimeError("Langfuse administration client is not configured")
    return _admin


def _latest_evaluator(rows: list[dict]) -> dict:
    versions = [row.get("latestVersion", row) for row in rows]
    return max(versions, key=lambda row: row.get("version", 0))


def _matches_desired(actual, desired) -> bool:
    if isinstance(desired, dict):
        return isinstance(actual, dict) and all(
            key in actual and _matches_desired(actual[key], value)
            for key, value in desired.items()
        )
    if isinstance(desired, list):
        return (
            isinstance(actual, list)
            and len(actual) == len(desired)
            and all(
                _matches_desired(actual_item, desired_item)
                for actual_item, desired_item in zip(actual, desired)
            )
        )
    return actual == desired


def ensure_evaluator(name: str, desired_body: dict) -> dict:
    api = _require_admin()
    existing = api.list_named(EVALUATORS_PATH, name)
    if existing:
        latest = _latest_evaluator(existing)
        if _matches_desired(latest, desired_body):
            return latest
    return api.call("POST", EVALUATORS_PATH, desired_body)


def ensure_rule(name: str, desired_body: dict) -> dict:
    api = _require_admin()
    existing = api.list_named(RULES_PATH, name)
    if not existing:
        return api.call("POST", RULES_PATH, desired_body)

    rule = existing[0]
    changed = {
        key: value
        for key, value in desired_body.items()
        if not _matches_desired(rule.get(key), value)
    }
    if not changed:
        return rule
    return api.call("PATCH", f"{RULES_PATH}/{rule['id']}", changed)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--operational", action="store_true")
    args = parser.parse_args(argv)
    if not args.operational:
        parser.error("select the --operational provisioning phase")

    load_dotenv()
    required = ["LANGFUSE_BASE_URL", "LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise SystemExit(
            "missing required environment variables: " + ", ".join(missing)
        )

    global _admin
    _admin = LangfuseAdmin(
        os.environ["LANGFUSE_BASE_URL"],
        os.environ["LANGFUSE_PUBLIC_KEY"],
        os.environ["LANGFUSE_SECRET_KEY"],
    )
    evaluator = ensure_evaluator(EVALUATOR, operational_evaluator_body())
    rule = ensure_rule(RULE, operational_rule_body())
    print(
        f"OK: evaluator {evaluator.get('name', EVALUATOR)}; "
        f"rule {rule.get('name', RULE)} enabled={rule.get('enabled', True)}"
    )


if __name__ == "__main__":
    main()
