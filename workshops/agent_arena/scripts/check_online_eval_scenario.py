"""Prove that the seeded stale-policy incident is reproducible.

Usage:
    source .env
    python -m scripts.check_online_eval_scenario --config-id "$WINNER_CONFIG_ID"
"""

import argparse
import re
import sys
from pathlib import Path


STALE_SQL = (
    "SELECT count() FROM v_customers "
    "WHERE signup_date >= today() - INTERVAL 90 DAY"
)
CURRENT_SQL = (
    "SELECT uniqExact(customer_id) FROM v_orders "
    "WHERE order_ts >= now() - INTERVAL 30 DAY "
    "AND status NOT IN ('cancelled', 'returned')"
)

SEEDED_QUESTIONS = [
    "How many active customers do we have?",
    "What is our active customer count right now?",
    "How many customers qualify as active under our business definition?",
]

_COMMENT = re.compile(r"/\*.*?\*/|--[^\n]*|\#[^\n]*", re.DOTALL)
_STRING = re.compile(r"'(?:''|[^'])*'")


def _sql_shape(sql: str) -> tuple[str, str]:
    without_comments = _COMMENT.sub(" ", sql or "").lower().replace("`", "")
    shape = _STRING.sub("''", without_comments)
    return without_comments, re.sub(r"\s+", " ", shape)


def _has_view(shape: str, view: str) -> bool:
    return bool(re.search(rf"(?<![a-z0-9_])(?:[a-z_][a-z0-9_]*\.)?{view}\b", shape))


def _has_day_window(shape: str, column: str, days: int) -> bool:
    column_ref = rf"(?:[a-z_][a-z0-9_]*\.)?{column}\b"
    interval = rf"\binterval\s+{days}\s+days?\b"
    return bool(re.search(column_ref, shape) and re.search(interval, shape))


def _excludes_status(sql: str, value: str) -> bool:
    status_ref = r"(?:[a-z_][a-z0-9_]*\.)?status\b"
    comparison = rf"{status_ref}\s*(?:!=|<>)\s*'{value}'"
    if re.search(comparison, sql):
        return True
    for match in re.finditer(rf"{status_ref}\s+not\s+in\s*\(([^)]*)\)", sql):
        values = {item.replace("''", "'") for item in _STRING.findall(match.group(1))}
        if f"'{value}'" in values:
            return True
    return False


def classify_active_customer_sql(sql: str) -> str:
    """Classify complete seeded policy definitions, rejecting mixed signals."""
    original, shape = _sql_shape(sql)
    has_customers = _has_view(shape, "v_customers")
    has_orders = _has_view(shape, "v_orders")
    stale_window = _has_day_window(shape, "signup_date", 90)
    current_window = _has_day_window(shape, "order_ts", 30)
    excludes_cancelled = _excludes_status(original, "cancelled")
    excludes_returned = _excludes_status(original, "returned")

    stale = (
        has_customers
        and stale_window
        and not has_orders
        and not current_window
        and not excludes_cancelled
        and not excludes_returned
    )
    current = (
        has_orders
        and current_window
        and excludes_cancelled
        and excludes_returned
        and not has_customers
        and not stale_window
    )
    if stale == current:
        return "unknown"
    return "policy-v1" if stale else "policy-v2"


def verify_reference_counts(stale: int, current: int) -> None:
    if stale == current:
        raise RuntimeError("reference queries returned the same value")


def _require_stale_classification(result, question_number: int) -> str:
    if result.outcome_hint == "model_error":
        raise RuntimeError(
            f"provider/model call failed for seeded question {question_number}"
        )
    if result.error is not None:
        known_hints = {"sql_policy_rejected", "sql_exec_error"}
        hint = result.outcome_hint if result.outcome_hint in known_hints else "agent_error"
        raise RuntimeError(
            f"agent execution failed for seeded question {question_number} ({hint})"
        )
    classification = classify_active_customer_sql(result.sql or "")
    if classification != "policy-v1":
        raise RuntimeError(
            f"seeded question {question_number} classified as {classification}"
        )
    return classification


def _scalar_count(result, label: str) -> int:
    if len(result.rows) != 1 or len(result.rows[0]) != 1:
        raise RuntimeError(f"{label} reference query did not return one scalar")
    value = result.rows[0][0]
    if isinstance(value, bool) or not isinstance(value, int):
        raise RuntimeError(f"{label} reference query returned a non-integer scalar")
    return value


def _resolve_config(cfg, config_id: str):
    parts = config_id.split("__")
    if len(parts) != 2 or not all(parts):
        raise RuntimeError(f"unknown config-id {config_id!r}")
    model_name, prompt_name = parts
    try:
        return cfg.model_by_name(model_name), cfg.prompt_by_name(prompt_name)
    except StopIteration:
        raise RuntimeError(f"unknown config-id {config_id!r}") from None


def _run(config_id: str) -> None:
    from dotenv import load_dotenv

    from agents.chclient import ROClickHouseClient
    from agents.llm import OpenRouterClient
    from agents.loop import run_agent
    from agents.policy import load_policy, with_policy
    from arena.config import load_config

    if not Path(".env").is_file():
        raise RuntimeError("ignored .env file is missing")
    load_dotenv(".env")
    try:
        cfg = load_config()
    except KeyError as exc:
        raise RuntimeError("configuration is missing a required environment variable") from None
    except Exception as exc:
        raise RuntimeError(
            f"configuration could not be loaded ({type(exc).__name__})"
        ) from None

    model_cfg, prompt_cfg = _resolve_config(cfg, config_id)
    policy = load_policy("policy-v1")
    try:
        schema_context = Path("schema/schema_context.md").read_text()
    except Exception as exc:
        raise RuntimeError(f"schema could not be loaded ({type(exc).__name__})") from None
    agent_context = with_policy(schema_context, policy)

    try:
        ro = ROClickHouseClient(cfg.clickhouse)
        stale = _scalar_count(ro.query(STALE_SQL), "stale")
        current = _scalar_count(ro.query(CURRENT_SQL), "current")
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError(
            f"ClickHouse reference query failed ({type(exc).__name__})"
        ) from None
    verify_reference_counts(stale, current)

    llm = OpenRouterClient(cfg.openrouter.base_url, cfg.openrouter.api_key)
    inference = dict(cfg.openrouter.inference)
    inference["temperature"] = 0.0
    classifications = []
    for number, question in enumerate(SEEDED_QUESTIONS, start=1):
        result = run_agent(
            question,
            model_cfg,
            prompt_cfg,
            agent_context,
            ro,
            llm,
            inference,
            max_retries=cfg.eval.default_max_retries,
        )
        classification = _require_stale_classification(result, number)
        classifications.append(classification)

    print(f"stale_count={stale}")
    print(f"current_count={current}")
    print(f"selected_config={config_id}")
    for number, classification in enumerate(classifications, start=1):
        print(f"classification_{number}={classification}")
    print("OK: seeded online-evaluation incident is reproducible")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-id", required=True, metavar="MODEL__PROMPT")
    args = parser.parse_args()
    try:
        _run(args.config_id)
    except RuntimeError as exc:
        print(f"BLOCKED: {exc}", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
