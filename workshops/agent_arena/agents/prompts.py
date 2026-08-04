"""Prompt strategy builders. Each returns (system_text, messages).
messages use the Bedrock Converse shape: [{"role","content":[{"text"}]}]."""

_BASE = (
    "You are a senior analytics engineer. Translate the user's question into a "
    "single ClickHouse SQL query. Query ONLY the provided views. Return the SQL "
    "inside one fenced ```sql code block and nothing else."
)

_DIALECT = (
    "\n\nClickHouse dialect notes:\n"
    "- Dates: now(), today(), toStartOfQuarter(), INTERVAL N DAY, toDate(x).\n"
    "- Aggregates: count(), sum(), avg(), uniqExact(), quantile(), argMax(), countIf(cond).\n"
    "- Use ROUND(x, 2) for money. Window: avg(x) OVER (ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW).\n"
    "- No ILIKE; use lower(x) LIKE. Boolean expr like (a = 'x') yields 0/1."
)


def _user_msg(schema_ctx: str, question: str) -> list[dict]:
    content = f"Schema:\n{schema_ctx}\n\nQuestion: {question}"
    return [{"role": "user", "content": [{"text": content}]}]


def build_p1(schema_ctx, question, examples=None):
    return _BASE, _user_msg(schema_ctx, question)


def build_p3(schema_ctx, question, examples=None):
    return _BASE + _DIALECT, _user_msg(schema_ctx, question)


def build_p2(schema_ctx, question, examples=None):
    ex = examples or []
    shots = "\n\n".join(
        f"Q: {e['question']}\n```sql\n{e['golden_sql'].strip()}\n```" for e in ex)
    sys = _BASE + ("\n\nWorked examples:\n" + shots if shots else "")
    return sys, _user_msg(schema_ctx, question)


PROMPT_BUILDERS = {
    "P1_zeroshot": build_p1,
    "P2_fewshot": build_p2,
    "P3_dialect": build_p3,
}


def correction_turn(prev_sql: str, error: str) -> list[dict]:
    return [
        {"role": "assistant", "content": [{"text": f"```sql\n{prev_sql}\n```"}]},
        {"role": "user", "content": [{"text":
            f"That query failed with ClickHouse error:\n{error}\n"
            "Return a corrected single ClickHouse SQL query in one ```sql block."}]},
    ]
