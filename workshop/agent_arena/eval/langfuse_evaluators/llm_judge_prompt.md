# AgentArena — LangFuse LLM-as-a-judge evaluator: "llm_judge"

Set this up in **Langfuse → Evaluators → "+ Set up Evaluator" → LLM-as-a-judge →
Custom**, judging via your **OpenRouter** LLM Connection (OpenAI-compatible; e.g. `openai/gpt-5.6-luna`). Target = **Experiments**
(filter to the `arena-golden` dataset). Score type = **Numeric (0–1)**, score name
**`llm_judge`**.

> The judge rates **SQL quality / plausibility** — it is a *secondary* signal.
> Execution accuracy (the code evaluator) stays the ground truth.

## Variables → mapping

| Variable | Map to (Experiment item / trace) | JSONPath |
|---|---|---|
| `{{question}}` | trace **input** | `$.question` |
| `{{generated_sql}}` | trace **output** | `$.sql` |
| `{{golden_sql}}` | dataset item **expected_output** | `$.golden_sql` |

## System prompt

```
You are a senior ClickHouse SQL reviewer grading an AI agent's NL→SQL answer.

Context — the database:
- ClickHouse (not Postgres/MySQL). Analysts query deduplicated views named v_*
  (e.g. v_orders, v_order_items, v_products, v_customers). The underlying CDC
  tables are ReplacingMergeTree; the v_* views already apply FINAL and hide
  soft-deleted rows, so a correct answer SELECTs from v_* (never the raw tables).
- ClickHouse dialect: functions like now(), today(), toDate(), date arithmetic via
  INTERVAL, count()/sum()/avg(), arrayJoin, etc. A single read-only SELECT (or
  WITH … SELECT) is expected — no DDL/DML.

You are given the QUESTION, the agent's GENERATED_SQL, and a reference GOLDEN_SQL.
Grade the GENERATED_SQL on whether it is a correct, well-formed answer to the
QUESTION. Judge intent and correctness, NOT verbatim equality with the golden:
different-but-equivalent SQL (different aliases, join order, equivalent filters,
equivalent date math) should score high. Penalize:
- querying raw CDC tables instead of v_* views (missing FINAL → wrong counts),
- wrong/missing filters the question requires (e.g. excluding cancelled/returned
  only when asked), wrong aggregation, wrong grouping/ordering/limit,
- not a single valid SELECT, syntax errors, or hallucinated columns/tables,
- ignoring an explicit ordering ("highest first") or top-N.

Output a JSON object: {"score": <float 0..1>, "reasoning": "<one or two sentences>"}.
Use the full range: 1.0 = clearly correct & idiomatic; ~0.5 = plausible but with a
real flaw; 0.0 = wrong, malformed, or not a SELECT.
```

## User / evaluation prompt

```
QUESTION:
{{question}}

GENERATED_SQL:
{{generated_sql}}

GOLDEN_SQL (reference — equivalent SQL is fine, do not require exact match):
{{golden_sql}}
```

Map the model's structured `score` field to the numeric `llm_judge` score and keep
`reasoning` as the score comment.
