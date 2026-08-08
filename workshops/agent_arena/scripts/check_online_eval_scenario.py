"""Prove that the seeded stale-policy incident is reproducible.

Usage:
    source .env
    .venv/bin/python -m scripts.check_online_eval_scenario --config-id "$WINNER_CONFIG_ID"
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

class PreflightError(RuntimeError):
    """A pre-sanitized failure safe to show at the CLI boundary."""


def _lex_sql(sql: str):
    """Return conservative SQL tokens, skipping comments only outside literals."""
    if not isinstance(sql, str):
        return None
    tokens = []
    i = 0
    while i < len(sql):
        char = sql[i]
        if char.isspace():
            i += 1
            continue
        if char == "'":
            i += 1
            value = []
            while i < len(sql):
                if sql[i] == "'":
                    if i + 1 < len(sql) and sql[i + 1] == "'":
                        value.append("'")
                        i += 2
                        continue
                    i += 1
                    tokens.append(("string", "".join(value).lower()))
                    break
                if sql[i] == "\\" and i + 1 < len(sql):
                    value.append(sql[i + 1])
                    i += 2
                    continue
                value.append(sql[i])
                i += 1
            else:
                return None
            continue
        if char in {"`", '"'}:
            quote = char
            i += 1
            value = []
            while i < len(sql):
                if sql[i] == quote:
                    if i + 1 < len(sql) and sql[i + 1] == quote:
                        value.append(quote)
                        i += 2
                        continue
                    i += 1
                    tokens.append(("ident", "".join(value).lower()))
                    break
                value.append(sql[i])
                i += 1
            else:
                return None
            continue
        if sql.startswith("--", i) or char == "#":
            newline = sql.find("\n", i + 1)
            i = len(sql) if newline < 0 else newline + 1
            continue
        if sql.startswith("/*", i):
            end = sql.find("*/", i + 2)
            if end < 0:
                return None
            i = end + 2
            continue
        if char.isalpha() or char == "_":
            end = i + 1
            while end < len(sql) and (sql[end].isalnum() or sql[end] in {"_", "$"}):
                end += 1
            tokens.append(("ident", sql[i:end].lower()))
            i = end
            continue
        if char.isdigit():
            end = i + 1
            while end < len(sql) and (sql[end].isdigit() or sql[end] == "."):
                end += 1
            tokens.append(("number", sql[i:end]))
            i = end
            continue
        operator = sql[i:i + 2]
        if operator in {">=", "<=", "!=", "<>", "=="}:
            tokens.append(("symbol", operator))
            i += 2
            continue
        tokens.append(("symbol", char))
        i += 1
    return tokens


def _values(tokens) -> list[str]:
    return [value if kind != "string" else "?" for kind, value in tokens]


def _find_top_level(tokens, value: str, start: int = 0):
    depth = 0
    for index in range(start, len(tokens)):
        token = tokens[index][1]
        if token == "(":
            depth += 1
        elif token == ")":
            depth -= 1
            if depth < 0:
                return None
        elif depth == 0 and token == value:
            return index
    return None


def _simple_column(tokens, column: str) -> bool:
    return (
        len(tokens) == 1
        and tokens[0] == ("ident", column)
    ) or (
        len(tokens) == 3
        and tokens[0][0] == "ident"
        and tokens[1][1] == "."
        and tokens[2] == ("ident", column)
    )


def _scalar_count_kind(tokens):
    if not tokens or tokens[0] != ("ident", "select"):
        return None
    from_index = _find_top_level(tokens, "from", 1)
    if from_index is None:
        return None
    expression = tokens[1:from_index]
    if len(expression) < 3 or expression[0][0] != "ident" or expression[1][1] != "(":
        return None

    depth = 0
    close_index = None
    for index in range(1, len(expression)):
        if expression[index][1] == "(":
            depth += 1
        elif expression[index][1] == ")":
            depth -= 1
            if depth == 0:
                close_index = index
                break
    if close_index is None:
        return None
    suffix = expression[close_index + 1:]
    if suffix and not (
        len(suffix) == 1 and suffix[0][0] == "ident"
    ) and not (
        len(suffix) == 2
        and suffix[0] == ("ident", "as")
        and suffix[1][0] == "ident"
    ):
        return None

    function = expression[0][1]
    arguments = expression[2:close_index]
    stale_argument = (
        not arguments
        or _simple_column(arguments, "customer_id")
        or len(arguments) == 1 and arguments[0][1] in {"*", "1"}
    )
    if function == "count" and stale_argument:
        return "count"
    if function in {"uniqexact", "countdistinct"} and _simple_column(
        arguments, "customer_id"
    ):
        return "distinct_customer_count"
    if (
        function == "count"
        and arguments[:1] == [("ident", "distinct")]
        and _simple_column(arguments[1:], "customer_id")
    ):
        return "distinct_customer_count"
    return None


def _relation_references(tokens) -> list[str]:
    relations = []
    for index, token in enumerate(tokens):
        if token not in {("ident", "from"), ("ident", "join")}:
            continue
        cursor = index + 1
        if cursor >= len(tokens) or tokens[cursor][0] != "ident":
            continue
        parts = [tokens[cursor][1]]
        cursor += 1
        while (
            cursor + 1 < len(tokens)
            and tokens[cursor][1] == "."
            and tokens[cursor + 1][0] == "ident"
        ):
            parts.append(tokens[cursor + 1][1])
            cursor += 2
        relations.append(parts[-1])
    return relations


def _single_relation(tokens):
    from_indexes = [
        index for index, token in enumerate(tokens)
        if token == ("ident", "from")
    ]
    if len(from_indexes) != 1 or ("ident", "join") in tokens:
        return None
    from_index = _find_top_level(tokens, "from", 1)
    where_index = _find_top_level(tokens, "where", from_index + 1) \
        if from_index is not None else None
    if from_index is None or where_index is None:
        return None

    cursor = from_index + 1
    if cursor >= where_index or tokens[cursor][0] != "ident":
        return None
    parts = [tokens[cursor][1]]
    cursor += 1
    while (
        cursor + 1 < where_index
        and tokens[cursor][1] == "."
        and tokens[cursor + 1][0] == "ident"
    ):
        parts.append(tokens[cursor + 1][1])
        cursor += 2

    suffix = tokens[cursor:where_index]
    has_safe_alias = (
        not suffix
        or len(suffix) == 1 and suffix[0][0] == "ident"
        or len(suffix) == 2
        and suffix[0] == ("ident", "as")
        and suffix[1][0] == "ident"
    )
    return parts[-1] if has_safe_alias else None


def _where_tokens(tokens):
    from_index = _find_top_level(tokens, "from", 1)
    if from_index is None:
        return None
    where_index = _find_top_level(tokens, "where", from_index + 1)
    if where_index is None:
        return None
    end = len(tokens)
    for keyword in ("group", "having", "order", "limit", "settings", "format", "union"):
        found = _find_top_level(tokens, keyword, where_index + 1)
        if found is not None:
            end = min(end, found)
    return tokens[where_index + 1:end]


def _has_interval(tokens, days: int) -> bool:
    values = _values(tokens)
    return any(
        values[index:index + 3] in (
            ["interval", str(days), "day"],
            ["interval", str(days), "days"],
        )
        for index in range(len(values) - 2)
    )


def _has_day_window(where, column: str, days: int) -> bool:
    shape = " ".join(_values(where))
    qualifier = r"(?:[a-z_][a-z0-9_$]*\s+\.\s+)?"
    column_ref = rf"{qualifier}{column}"
    wrapped_column = rf"(?:(?:todate|todatetime)\s+\(\s*)?{column_ref}\s*\)?"
    return bool(re.search(
        rf"{wrapped_column}\s*(?:>=|>)\s*(?:now|today)\s*\(\s*\)\s*-\s*"
        rf"interval\s+{days}\s+days?\b",
        shape,
    ))


def _has_obvious_contradiction(where) -> bool:
    if ("ident", "false") in where:
        return True
    for kind, value in where:
        if kind == "number":
            try:
                if float(value) == 0:
                    return True
            except ValueError:
                return True

    comparisons = {
        "=": lambda left, right: left == right,
        "==": lambda left, right: left == right,
        "!=": lambda left, right: left != right,
        "<>": lambda left, right: left != right,
        ">": lambda left, right: left > right,
        "<": lambda left, right: left < right,
        ">=": lambda left, right: left >= right,
        "<=": lambda left, right: left <= right,
    }
    for index in range(len(where) - 2):
        left, operator, right = where[index:index + 3]
        if left[0] != "number" or right[0] != "number":
            continue
        if operator[1] not in comparisons:
            continue
        try:
            if not comparisons[operator[1]](float(left[1]), float(right[1])):
                return True
        except ValueError:
            return True
    return False


def _status_exclusions_are_conjunctive(where) -> bool:
    values = _values(where)
    if "or" in values or "case" in values or "if" in values or "multiif" in values:
        return False
    for index, value in enumerate(values):
        if value == "not" and not (
            index > 0
            and values[index - 1] == "status"
            and index + 1 < len(values)
            and values[index + 1] == "in"
        ):
            return False

    excluded_positions = {"cancelled": [], "returned": []}
    combined_exclusion = False
    for index, token in enumerate(where):
        if token != ("ident", "status"):
            continue
        cursor = index + 1
        if cursor >= len(where):
            return False
        operator = where[cursor][1]
        if operator in {"!=", "<>"}:
            if cursor + 1 >= len(where) or where[cursor + 1][0] != "string":
                return False
            status = where[cursor + 1][1]
            if status not in excluded_positions:
                return False
            excluded_positions[status].append(index)
            continue
        if (
            operator == "not"
            and cursor + 2 < len(where)
            and where[cursor + 1] == ("ident", "in")
            and where[cursor + 2][1] == "("
        ):
            cursor += 3
            statuses = set()
            while cursor < len(where) and where[cursor][1] != ")":
                if where[cursor][0] == "string":
                    statuses.add(where[cursor][1])
                elif where[cursor][1] != ",":
                    return False
                cursor += 1
            if cursor >= len(where) or not statuses:
                return False
            if not statuses <= set(excluded_positions):
                return False
            for status in statuses:
                excluded_positions[status].append(index)
            combined_exclusion = set(excluded_positions) <= statuses
            continue
        return False

    if not all(excluded_positions.values()):
        return False
    if combined_exclusion:
        return True
    first = min(excluded_positions["cancelled"])
    second = min(excluded_positions["returned"])
    low, high = sorted((first, second))
    return "and" in values[low + 1:high]


def classify_active_customer_sql(sql: str) -> str:
    """Classify only complete, unambiguous seeded scalar count definitions."""
    tokens = _lex_sql(sql)
    if not tokens:
        return "unknown"
    semicolons = [index for index, token in enumerate(tokens) if token[1] == ";"]
    if semicolons and semicolons != [len(tokens) - 1]:
        return "unknown"
    if semicolons:
        tokens = tokens[:-1]
    values = _values(tokens)
    if values.count("select") != 1 or "with" in values:
        return "unknown"
    if any(keyword in values for keyword in ("group", "having", "union", "over")):
        return "unknown"

    count_kind = _scalar_count_kind(tokens)
    relations = _relation_references(tokens)
    relation = _single_relation(tokens)
    where = _where_tokens(tokens)
    if count_kind is None or where is None or len(relations) != 1:
        return "unknown"

    identifier_values = {value for kind, value in tokens if kind == "ident"}
    where_values = _values(where)
    stale_signals = (
        "v_customers" in identifier_values
        or "signup_date" in identifier_values
        or _has_interval(tokens, 90)
    )
    current_signals = (
        "v_orders" in identifier_values
        or "order_ts" in identifier_values
        or "status" in identifier_values
        or _has_interval(tokens, 30)
    )
    stale = (
        count_kind == "count"
        and relation == "v_customers"
        and _has_day_window(where, "signup_date", 90)
        and where_values.count("signup_date") == 1
        and "or" not in where_values
        and "not" not in where_values
        and not _has_obvious_contradiction(where)
        and not current_signals
    )
    current = (
        count_kind == "distinct_customer_count"
        and relation == "v_orders"
        and _has_day_window(where, "order_ts", 30)
        and where_values.count("order_ts") == 1
        and _status_exclusions_are_conjunctive(where)
        and not _has_obvious_contradiction(where)
        and not stale_signals
    )
    if stale == current:
        return "unknown"
    return "policy-v1" if stale else "policy-v2"


def verify_reference_counts(stale: int, current: int) -> None:
    if stale == current:
        raise PreflightError("reference queries returned the same value")


def _require_stale_classification(result, question_number: int) -> str:
    if result.outcome_hint == "model_error":
        raise PreflightError(
            f"provider/model call failed for seeded question {question_number}"
        )
    if result.error is not None:
        known_hints = {"sql_policy_rejected", "sql_exec_error"}
        hint = result.outcome_hint if result.outcome_hint in known_hints else "agent_error"
        raise PreflightError(
            f"agent execution failed for seeded question {question_number} ({hint})"
        )
    classification = classify_active_customer_sql(result.sql or "")
    if classification != "policy-v1":
        raise PreflightError(
            f"seeded question {question_number} classified as {classification}"
        )
    return classification


def _scalar_count(result, label: str) -> int:
    if len(result.rows) != 1 or len(result.rows[0]) != 1:
        raise PreflightError(f"{label} reference query did not return one scalar")
    value = result.rows[0][0]
    if isinstance(value, bool) or not isinstance(value, int):
        raise PreflightError(f"{label} reference query returned a non-integer scalar")
    return value


def _resolve_config(cfg, config_id: str):
    parts = config_id.split("__")
    if len(parts) != 2 or not all(parts):
        raise PreflightError("invalid config-id format")
    model_name, prompt_name = parts
    try:
        return cfg.model_by_name(model_name), cfg.prompt_by_name(prompt_name)
    except StopIteration:
        raise PreflightError("unknown selected config") from None
    except Exception:
        raise PreflightError("configuration selection failed") from None


def _safe_result_outcome(result) -> tuple[str, str]:
    try:
        sql = result.sql or ""
        error = result.error
        hint = result.outcome_hint
        classification = classify_active_customer_sql(sql)
    except Exception:
        return "result_error", "unknown"
    if error is None:
        return "ok", classification
    safe_hints = {"model_error", "sql_policy_rejected", "sql_exec_error"}
    return (hint if hint in safe_hints else "agent_error"), classification


def _run(config_id: str) -> None:
    from dotenv import load_dotenv

    from agents.chclient import ROClickHouseClient
    from agents.llm import OpenRouterClient
    from agents.loop import run_agent
    from agents.policy import load_policy, with_policy
    from arena.config import load_config

    try:
        env_exists = Path(".env").is_file()
    except Exception:
        raise PreflightError("environment loading failed") from None
    if not env_exists:
        raise PreflightError("ignored .env file is missing")
    try:
        load_dotenv(".env")
    except Exception:
        raise PreflightError("environment loading failed") from None
    try:
        cfg = load_config()
    except KeyError:
        raise PreflightError(
            "configuration is missing a required environment variable"
        ) from None
    except Exception:
        raise PreflightError("configuration loading failed") from None

    model_cfg, prompt_cfg = _resolve_config(cfg, config_id)
    try:
        policy = load_policy("policy-v1")
    except Exception:
        raise PreflightError("policy context construction failed") from None
    try:
        schema_context = Path("schema/schema_context.md").read_text()
    except Exception:
        raise PreflightError("schema context loading failed") from None
    try:
        agent_context = with_policy(schema_context, policy)
    except Exception:
        raise PreflightError("policy context construction failed") from None

    try:
        ro = ROClickHouseClient(cfg.clickhouse)
    except Exception:
        raise PreflightError("ClickHouse client construction failed") from None
    try:
        stale_result = ro.query(STALE_SQL)
    except Exception:
        raise PreflightError("stale reference query failed") from None
    try:
        stale = _scalar_count(stale_result, "stale")
    except PreflightError:
        raise
    except Exception:
        raise PreflightError("stale reference query result was invalid") from None
    try:
        current_result = ro.query(CURRENT_SQL)
    except Exception:
        raise PreflightError("current reference query failed") from None
    try:
        current = _scalar_count(current_result, "current")
    except PreflightError:
        raise
    except Exception:
        raise PreflightError("current reference query result was invalid") from None
    verify_reference_counts(stale, current)

    try:
        llm = OpenRouterClient(cfg.openrouter.base_url, cfg.openrouter.api_key)
    except Exception:
        raise PreflightError("LLM client construction failed") from None
    try:
        inference = dict(cfg.openrouter.inference)
        inference["temperature"] = 0.0
    except Exception:
        raise PreflightError("inference configuration failed") from None
    classifications = []
    outcomes = []
    for number, question in enumerate(SEEDED_QUESTIONS, start=1):
        try:
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
            outcome, classification = _safe_result_outcome(result)
        except Exception:
            outcome, classification = "call_error", "unknown"
        outcomes.append((number, outcome, classification))
        classifications.append(classification)

    failures = [entry for entry in outcomes if entry[1:] != ("ok", "policy-v1")]
    if failures:
        summary = "; ".join(
            f"q{number}={outcome}/{classification}"
            for number, outcome, classification in failures
        )
        raise PreflightError(f"seeded verification failed: {summary}")

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
    except PreflightError as exc:
        print(f"BLOCKED: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
    except Exception:
        print("BLOCKED: unexpected preflight failure", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
