"""Provision AgentArena's online and experiment evaluator rules."""

import argparse
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode

from dotenv import load_dotenv

from agents.policy import load_policy
from scripts.langfuse_admin import (
    LangfuseAdmin,
    iter_cursor_pages,
    iter_numbered_pages,
)

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
PROMPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "eval"
    / "langfuse_evaluators"
    / "business_policy_adherence_prompt.md"
)
BUSINESS_POLICY_EVALUATOR = "business-policy-adherence"
BUSINESS_POLICY_EXPERIMENT_RULE = "business-policy-adherence"
BUSINESS_POLICY_ONLINE_RULE = "agent-arena-business-policy-online"
OPENROUTER_CONNECTION = "agent-arena-openrouter"
JUDGE_MODEL = "openai/gpt-5.6-luna"
DATASET_NAME = "arena-golden"

BUSINESS_POLICY_MAPPING = [
    {"variable": "question", "source": "input", "jsonPath": "$.question"},
    {"variable": "generated_sql", "source": "output", "jsonPath": "$.sql"},
]

_admin: LangfuseAdmin | None = None


def business_policy_prompt(policy_version="policy-v2") -> str:
    template = PROMPT_PATH.read_text()
    return template.replace(
        "{{policy_catalog}}", load_policy(policy_version).rendered
    )


def business_policy_evaluator_body(prompt: str | None = None) -> dict:
    if prompt is None:
        prompt = business_policy_prompt()
    return {
        "type": "llm_as_judge",
        "name": BUSINESS_POLICY_EVALUATOR,
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
            "provider": OPENROUTER_CONNECTION,
            "model": JUDGE_MODEL,
        },
    }


def business_policy_experiment_rule_body(dataset_id: str) -> dict:
    return {
        "name": BUSINESS_POLICY_EXPERIMENT_RULE,
        "evaluator": {
            "name": BUSINESS_POLICY_EVALUATOR,
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
                "value": [dataset_id],
            }
        ],
        "mapping": list(BUSINESS_POLICY_MAPPING),
    }


def business_policy_online_rule_body(enabled: bool = False) -> dict:
    return {
        "name": BUSINESS_POLICY_ONLINE_RULE,
        "evaluator": {
            "name": BUSINESS_POLICY_EVALUATOR,
            "scope": "project",
            "type": "llm_as_judge",
        },
        "target": "observation",
        "enabled": enabled,
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
        "mapping": list(BUSINESS_POLICY_MAPPING),
    }


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


def _find_dataset(name: str) -> dict | None:
    api = _require_admin()

    def fetch(page: int) -> dict:
        query = urlencode({"page": page, "limit": 100})
        return api.call("GET", f"/api/public/v2/datasets?{query}")

    for payload in iter_numbered_pages(fetch):
        dataset = next(
            (row for row in payload.get("data", []) or [] if row.get("name") == name),
            None,
        )
        if dataset is not None:
            return dataset
    return None


def _experiment_items_from_start() -> str:
    return (
        datetime.now(timezone.utc) - timedelta(days=30)
    ).isoformat().replace("+00:00", "Z")


def provision_business_policy_experiments(
    *, openrouter_api_key: str, openrouter_base_url: str
) -> tuple[dict, dict, dict]:
    """Provision the policy-v2 judge and its gated rollout rules."""
    api = _require_admin()
    api.call("PUT", "/api/public/llm-connections", {
        "provider": OPENROUTER_CONNECTION,
        "adapter": "openai",
        "secretKey": openrouter_api_key,
        "baseURL": openrouter_base_url,
        "customModels": [JUDGE_MODEL],
        "withDefaultModels": False,
    })

    dataset = _find_dataset(DATASET_NAME)
    if dataset is None:
        raise RuntimeError(
            f"dataset {DATASET_NAME!r} must exist before policy evaluator provisioning"
        )

    evaluator = ensure_evaluator(
        BUSINESS_POLICY_EVALUATOR, business_policy_evaluator_body()
    )
    experiment_rule = ensure_rule(
        BUSINESS_POLICY_EXPERIMENT_RULE,
        business_policy_experiment_rule_body(dataset["id"]),
    )
    online_rule = ensure_rule(
        BUSINESS_POLICY_ONLINE_RULE,
        business_policy_online_rule_body(enabled=False),
    )
    return evaluator, experiment_rule, online_rule


def _business_policy_experiment_score_exists() -> bool:
    api = _require_admin()
    dataset = _find_dataset(DATASET_NAME)
    if dataset is None or not dataset.get("id"):
        return False

    def fetch(cursor: str | None) -> dict:
        params = {
            "fromStartTime": _experiment_items_from_start(),
            "fields": (
                "core,dataset,io,metadata,itemMetadata,"
                "experimentMetadata,scores"
            ),
            "limit": 100,
            "scoreLimit": 50,
        }
        if cursor is not None:
            params["cursor"] = cursor
        return api.call(
            "GET", f"/api/public/experiment-items?{urlencode(params)}"
        )

    for payload in iter_cursor_pages(fetch):
        for item in payload.get("data", []) or []:
            if (
                item.get("experimentDatasetId") != dataset["id"]
                or not item.get("experimentId")
                or not item.get("experimentName")
                or not item.get("traceId")
            ):
                continue
            if any(
                score.get("name") == BUSINESS_POLICY_EVALUATOR
                and score.get("value") is not None
                for score in item.get("scores", []) or []
            ):
                return True
    return False


def enable_business_policy_online() -> dict:
    """Enable root-observation evaluation after experiment calibration exists."""
    if not _business_policy_experiment_score_exists():
        raise RuntimeError(
            "no business-policy-adherence experiment score exists; "
            "run and validate calibration before enabling online evaluation"
        )
    return ensure_rule(
        BUSINESS_POLICY_ONLINE_RULE,
        business_policy_online_rule_body(enabled=True),
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    phases = parser.add_mutually_exclusive_group(required=True)
    phases.add_argument("--operational", action="store_true")
    phases.add_argument("--business-policy-experiments", action="store_true")
    phases.add_argument("--enable-business-policy-online", action="store_true")
    args = parser.parse_args(argv)

    load_dotenv()
    required = ["LANGFUSE_BASE_URL", "LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY"]
    if args.business_policy_experiments:
        required.extend(["OPENROUTER_API_KEY", "OPENROUTER_BASE_URL"])
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
    if args.operational:
        evaluator = ensure_evaluator(EVALUATOR, operational_evaluator_body())
        rule = ensure_rule(RULE, operational_rule_body())
        print(
            f"OK: evaluator {evaluator.get('name', EVALUATOR)}; "
            f"rule {rule.get('name', RULE)} enabled={rule.get('enabled', True)}"
        )
    elif args.business_policy_experiments:
        evaluator, experiment_rule, online_rule = (
            provision_business_policy_experiments(
                openrouter_api_key=os.environ["OPENROUTER_API_KEY"],
                openrouter_base_url=os.environ["OPENROUTER_BASE_URL"],
            )
        )
        print(
            f"OK: evaluator {evaluator.get('name', BUSINESS_POLICY_EVALUATOR)}; "
            f"experiment rule enabled={experiment_rule.get('enabled', True)}; "
            f"online rule enabled={online_rule.get('enabled', False)}"
        )
    else:
        rule = enable_business_policy_online()
        print(
            f"OK: rule {rule.get('name', BUSINESS_POLICY_ONLINE_RULE)} "
            f"enabled={rule.get('enabled', True)} sampling={rule.get('sampling', 1)}"
        )


if __name__ == "__main__":
    main()
