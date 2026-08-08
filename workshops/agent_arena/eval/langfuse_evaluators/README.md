# Langfuse evaluators — grading runs inside Langfuse

Agent Arena keeps evaluation in Langfuse rather than copying scores to another
store. The harness runs each `model × prompt` configuration as an Experiment on the
`arena-golden` dataset; the leaderboard reads its Experiment Items and scores through
the Langfuse Public API.

The evaluator contract is:

- `correctness` — deterministic execution accuracy. The Code Evaluator in
  [correctness_evaluator.py](correctness_evaluator.py) compares the generated result
  set with the dataset item's expected output and emits `correctness` (0/1) plus an
  `outcome` category. Code evaluators have no network access; Agent Arena executes the
  query before evaluation.
- `agent-arena-llm-judge` — the OpenRouter-backed SQL-quality judge described in
  [llm_judge_prompt.md](llm_judge_prompt.md). This is a secondary experiment signal.
- `sql-execution-success` — a Boolean online evaluator for root `chat_turn`
  observations. It detects a non-empty, successfully executed query, but cannot prove
  semantic correctness.
- `user-thumbs` — the idempotent Boolean score written by `POST /feedback` (`true` for
  thumbs-up and `false` for thumbs-down).
- `business-policy-adherence` — the general, catalog-grounded judge's Experiment
  score, returning `PASS`, `FAIL`, or `NOT_APPLICABLE` during calibration.
- `agent-arena-business-policy-online` — the same judge's distinct online observation
  rule and score name after safe enablement. This Langfuse deployment emits rule
  names as score names, so Experiment and online assertions deliberately use
  different exact names.

## One-time experiment setup

After `scripts/arena.sh up` seeds ClickHouse, provision `arena-golden`, the OpenRouter
connection, and the SQL-quality judge:

```bash
source .env
.venv/bin/python -m scripts.provision_langfuse_evaluators
```

The helper is idempotent and uses Langfuse's public evaluator APIs. Configure the
deterministic experiment evaluator once in the UI:

1. Open **Evaluators** → **Set up Evaluator** → **Code** and paste
   [correctness_evaluator.py](correctness_evaluator.py).
2. Set the entry point to `evaluate(ctx)`, target to **Experiments**, and filter the
   dataset to `arena-golden`.

If automatic SQL-quality provisioning is unavailable, use **LLM-as-a-judge → Custom**
with the prompts and exact mappings in [llm_judge_prompt.md](llm_judge_prompt.md),
target **Experiments**, dataset `arena-golden`, and numeric score name
`agent-arena-llm-judge`.

Run a normal benchmark from the lab root with:

```bash
source .env
.venv/bin/python -m eval.harness --run-id demo
```

## Online improvement evaluators

First install the deterministic operational evaluator and its enabled serving rule:

```bash
source .env
.venv/bin/python -m scripts.provision_online_evaluators --operational
```

A serving trace can have `sql-execution-success=true` and still be semantically
wrong. Submit a Boolean thumbs-down, then investigate that trace in a Langfuse Human
Annotation queue. The queue review is a UI-only human step; provisioning and
promotion commands do not perform it. The root [README](../../README.md) gives the
exact queue fields, production-provenance export, and tracked fixture workflow.

After approved production cases are promoted, install the policy judge for
Experiments. This command deliberately leaves the online policy rule disabled:

```bash
source .env
.venv/bin/python -m scripts.provision_online_evaluators \
  --business-policy-experiments
```

Calibrate `policy-v1` and `policy-v2` using the same winner and the same expanded
dataset:

```bash
source .env
export WINNER_MODEL="${WINNER_MODEL:-qwen3.7-flash}"
export WINNER_PROMPT="${WINNER_PROMPT:-P2_fewshot}"
.venv/bin/python -m eval.harness --run-id online-loop-baseline \
  --policy-version policy-v1 --models "$WINNER_MODEL" --prompts "$WINNER_PROMPT" \
  --wait-for-score business-policy-adherence
.venv/bin/python -m eval.harness --run-id online-loop-candidate \
  --policy-version policy-v2 --models "$WINNER_MODEL" --prompts "$WINNER_PROMPT" \
  --wait-for-score business-policy-adherence
```

Do not enable online evaluation until every trace has the exact required scores, the
candidate fixes the production-derived slice, relevant revenue and conversion cases
are `PASS`, a generic count is `NOT_APPLICABLE`, and aggregate correctness does not
regress. Enablement also fails closed unless it finds an exact
`business-policy-adherence` score on an `arena-golden` Experiment Item:

```bash
source .env
.venv/bin/python -m scripts.provision_online_evaluators \
  --enable-business-policy-online
```

Verify future trace scores without retrieving trace bodies:

```bash
source .env
.venv/bin/python -m scripts.verify_online_scores "$TRACE_ID" \
  sql-execution-success=true agent-arena-business-policy-online=PASS
```

Do not coalesce the two judge score names or add fuzzy aliases:
`business-policy-adherence` is exact for Experiments, while
`agent-arena-business-policy-online` is exact for serving observations.

The online rules use sampling `1` (100%) so every workshop request produces visible
evidence. In production, choose sampling based on traffic, evaluator cost, latency,
risk, and the coverage required for incident detection.

Keep [correctness_evaluator.py](correctness_evaluator.py) synchronized with
`eval/grading.py` and `eval/serialize.py`; their result-set normalization must remain
identical.
