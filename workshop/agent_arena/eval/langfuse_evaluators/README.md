# LangFuse evaluators — grading runs *inside* LangFuse

This moves NL→SQL grading off the harness and into **LangFuse evaluators**, so the
demo shows LangFuse doing the work:

- **`correctness`** — a server-side **Code Evaluator** ([correctness_evaluator.py](correctness_evaluator.py))
  that compares the agent's query **result set** (trace output) to the **golden**
  result set (dataset item `expected_output`) and emits execution-accuracy `correctness`
  (0/1) + an `outcome` category. *(Code evaluators have no network egress — the query
  runs in the agent; the evaluator makes the verdict.)*
- **`llm_judge`** — an **LLM-as-a-judge** evaluator ([llm_judge_prompt.md](llm_judge_prompt.md))
  that rates SQL quality via your **OpenRouter** LLM Connection. Secondary signal.

The harness runs each `model × prompt` config as a **Dataset Run (Experiment)** on the
`arena-golden` dataset, putting the result sets on the trace/dataset item; the
evaluators score each item. The leaderboard reads those Experiment Items and Scores
directly through the Langfuse Public API; there is no second results store.

## One-time LangFuse setup

After `scripts/arena.sh up` seeds ClickHouse, provision `arena-golden`, the OpenRouter
connection, and the `llm_judge` rule with:

```bash
source .env && python -m scripts.provision_langfuse_evaluators
```

The helper is idempotent and uses LangFuse's public evaluator APIs. The deterministic
code evaluator is still configured once in the UI:

1. **Code evaluator `correctness`** — Evaluators → *Set up Evaluator* → **Code** →
   paste [correctness_evaluator.py](correctness_evaluator.py) → **Target: Experiments**
   → filter dataset = `arena-golden`. (Entry point is `evaluate(ctx)`.)
2. **Manual fallback for `llm_judge`** — Evaluators → *Set up Evaluator* → **LLM-as-a-judge →
   Custom** → use the system/eval prompts and the variable mappings in
   [llm_judge_prompt.md](llm_judge_prompt.md) → **Target: Experiments**, dataset
   `arena-golden` → Numeric score `llm_judge`.

## Run it

```bash
source .env && python -m eval.harness --run-id demo   # needs OPENROUTER_API_KEY + LANGFUSE_* in .env
# harness runs the grid and waits until Langfuse evaluators finish
```

The evaluators are required: Langfuse is the workshop's evaluation store and the
leaderboard has no local/ClickHouse fallback.

> Keep [correctness_evaluator.py](correctness_evaluator.py) in sync with
> `eval/grading.py` + `eval/serialize.py` — they must normalize result sets identically.
