# Agent Arena

**Put a roster of LLMs into a contest and crown the winner — for NL→SQL over
ClickHouse — with Langfuse in the loop from the very beginning.**

Agent Arena runs a grid of **{model} × {prompt strategy}** against a
ground-truthed set of business questions over a live ClickHouse dataset, grades
every answer by **execution accuracy** (did the query return the right *result*,
not the right *SQL text*), and ranks the configurations by **cost per correct
answer**. It answers, with evidence:

> *Which model + prompt should we ship for NL→SQL, and what does correctness cost?*

The flow is the one you'd actually use to ship an AI feature: **select a base
model → measure quality → continuously improve → release to production.**
Langfuse isn't bolted on afterward — it grades the contest (evaluators +
LLM-as-a-judge), stores every result and trace, powers the leaderboard through
its Public API, and then carries the winning
config into a real chatbot, where user 👍/👎 feedback flows back in as scores.
ClickHouse holds the business data the agent queries; Langfuse is the single
source of truth for benchmark results.

---

## What you get

A focused web app (`web/`, http://localhost:5174) with two tabs:

- **Leaderboard** — the contest results: every model×prompt config ranked by
  accuracy, **cost-per-correct-answer** (the headline), latency, per-tier
  accuracy, and an outcome breakdown, with a **cost × accuracy** chart and a
  **best-value** ranking up top. Click any config to **drill into its
  per-question results**, each linking to its **Langfuse trace** (prompt →
  generated SQL → error → tokens → span timings). A **"View conversation"**
  button replays the agent's session **live from the Langfuse API**, and an
  **LLM-judge** column scores SQL quality.
- **Chat** — the production chatbot: ask a question against a picked
  model+prompt config and watch the SQL, cost, and latency; rate each answer
  👍/👎, which is written back to the trace as a **Langfuse score**.
Plus the serving API (`serving/api.py`): **`POST /ask`** (run the agent live and
return SQL/results/cost/latency, traced to Langfuse) and **`POST /feedback`**
(attach a 👍/👎 score to a trace) — the same endpoints the Chat tab calls.

## Architecture

```text
Golden dataset → benchmark harness → agent → OpenRouter
                                      │
                                      └─ read-only SQL → ClickHouse v_* views
                         │
                         └─ Experiment Items + scores → Langfuse
                                                        │ Public API
                                                        ▼
                                                  leaderboard API/UI
```

**The key idea — one service per job:**
- **ClickHouse** is the application database: it holds the business data and
  executes generated and golden SQL.
- **Langfuse** is the evaluation store: Experiments, result payloads, exact
  cost/latency measurements, evaluator scores, and conversations. The local
  leaderboard reads those Experiment Items through the Langfuse Public API.

**The stable contract is the `v_*` views.** Agents and golden SQL only ever query
`v_orders`, `v_customers`, etc. — views that apply `FINAL` (dedup the
`ReplacingMergeTree` tables the data generator seeds directly) and hide
soft-deleted rows.

**Read-only safety (agents run model-generated SQL):** queries execute as a
ClickHouse `readonly=1` user with server-side resource limits, behind a SELECT-only
validator (`agents/sqlguard.py`) that rejects anything that isn't a single
`SELECT`/`WITH … SELECT`.

**How a config is scored:** the harness sends the question (built per prompt
strategy) to the model via **OpenRouter**, extracts the SQL, runs it against the
`v_*` views, and compares the result set to the cached golden result (by column
position, with float rounding and row-set normalization). The complete result,
including exact OpenRouter cost and end-to-end latency, is stored on the
Langfuse Experiment Item; Langfuse evaluators attach correctness, outcome, and
LLM-judge scores there.

---

## Getting started

### Prerequisites
- **Python 3.11**, **Node 18+** (for the web UI).
- A **ClickHouse Cloud** service, a **Langfuse Cloud** project, and an
  **OpenRouter** API key — one OpenAI-compatible endpoint fronts every model
  family in the roster (Anthropic, OpenAI, Google, DeepSeek, Qwen, Z.ai), so
  there are no per-provider credentials to manage.
- For Qwen, OpenRouter **Settings → Privacy → Data Policies → Zero Data Retention → Non-frontier**
  must be off. The available Alibaba route is rejected when non-frontier ZDR is
  enforced; review your privacy requirements before changing this for real data.

### 1. Install
```bash
git clone --branch build-workshop-v1 --single-branch https://github.com/ClickHouse/ClickHouse_Demos.git
cd ClickHouse_Demos/workshops/agent_arena
```
```bash
.venv/bin/python --version >/dev/null 2>&1 || python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
(cd web && npm install)        # only if you want the web UI
```

### 2. Create `.env` (not committed)
See [`.env.example`](.env.example) for the full template:
```bash
# ClickHouse Cloud (business data queried by the agent)
export CLICKHOUSE_CLOUD_HOST=xxx.clickhouse.cloud
export CLICKHOUSE_CLOUD_USER=default
# Set the ClickHouse Cloud password in this ignored file.
export CLICKHOUSE_CLOUD_DATABASE=arena
export ARENA_RO_PASSWORD=...                 # password for the read-only agent user (created by setup)

# OpenRouter (LLM provider)
export OPENROUTER_API_KEY=replace-with-openrouter-api-key
export OPENROUTER_BASE_URL=https://openrouter.ai/api/v1

# Langfuse Cloud (eval store + tracing)
export LANGFUSE_BASE_URL=https://us.cloud.langfuse.com
export LANGFUSE_PUBLIC_KEY=replace-with-langfuse-public-key
export LANGFUSE_SECRET_KEY=replace-with-langfuse-secret-key
```

### 3. Verify connectivity
```bash
source .env
.venv/bin/python -m scripts.check_connectivity  # ClickHouse + Langfuse + OpenRouter reachable?
```

### 4. Bring the stack up
```bash
scripts/arena.sh up      # seed ClickHouse business tables/views, then start servers
# open http://localhost:5174  → run the harness (below) to populate the Leaderboard
```

### Lifecycle commands
```bash
scripts/arena.sh up                # bring the stack up (seed ClickHouse, start servers)
scripts/arena.sh down               # drop the arena ClickHouse database + read-only user
scripts/arena.sh serve              # (re)start the dashboard API + web UI
scripts/arena.sh serve --api-only   # (re)start ONLY the backend dashboard API — fastest
scripts/arena.sh stop               # stop just the local servers
scripts/arena.sh status             # show what's running
```

### Running the benchmark
```bash
source .env && .venv/bin/python -m eval.harness --run-id demo
```
- Provision the OpenRouter-backed Langfuse judge once with
  `.venv/bin/python -m scripts.provision_langfuse_evaluators`; it creates `arena-golden` before
  installing the filtered rule. The correctness code evaluator still follows
  `eval/langfuse_evaluators/README.md`.
- The correctness code evaluator + `agent-arena-llm-judge` run server-side in Langfuse. The
  harness waits until the scores arrive; it does not copy them to another store.
- Each invocation is a **run** (`run_id`) made up of Langfuse Experiments and
  Experiment Items. The web UI reads them through the Public API and its run
  selector picks which run to view.
- The grid (which models × which prompts) defaults to `config.yaml`'s `grid`;
  `--models`/`--prompts` override it.

Model names come from [`config.yaml`](config.yaml) — six models, three proprietary and
three open-weight: `claude-sonnet-5`, `gpt-5.6-luna`,
`gemini-flash-lite` (proprietary) and `deepseek-v4-flash`, `qwen3.7-flash`,
`glm-4.7-flash` (open-weight). The roster is deliberately low-cost, since NL→SQL is a
simple enough task that it doesn't need frontier models. Prompt strategies:
`P1_zeroshot`, `P2_fewshot`, `P3_dialect`.

---

## Continuous online improvement loop

Run every command below from the lab root (`workshops/agent_arena`). The selected
workshop winner is `qwen3.7-flash` with `P2_fewshot`; the non-secret defaults are also
in [`.env.example`](.env.example). These fallback exports let an older local `.env`
continue to work without editing it:

```bash
source .env
export WINNER_MODEL="${WINNER_MODEL:-qwen3.7-flash}"
export WINNER_PROMPT="${WINNER_PROMPT:-P2_fewshot}"
export WINNER_CONFIG_ID="${WINNER_CONFIG_ID:-qwen3.7-flash__P2_fewshot}"
```

The loop deliberately starts with an evaluator blind spot: executable SQL can earn
`sql-execution-success=true` while still using a stale business definition. A Boolean
thumbs-down prioritizes the trace for human investigation; the reviewer then turns a
confirmed production failure into provenance-bearing golden cases. Only after the
same dataset shows `policy-v2` beating `policy-v1` do we enable the general policy
judge online and verify future traces.

### 1. Start serving on `policy-v1`

Generate the schema context using module form. Then run the stale release in this
terminal and leave it running:

```bash
source .env
.venv/bin/python -m schema.gen_schema_context
AGENT_ARENA_POLICY_VERSION=policy-v1 \
  .venv/bin/uvicorn serving.api:app --port 8100
```

### 2. Run the seeded-incident preflight

In a second terminal, from the lab root:

```bash
source .env
export WINNER_CONFIG_ID="${WINNER_CONFIG_ID:-qwen3.7-flash__P2_fewshot}"
.venv/bin/python -m scripts.check_online_eval_scenario \
  --config-id "$WINNER_CONFIG_ID"
.venv/bin/python -m scripts.provision_online_evaluators --operational
```

The preflight must report three `policy-v1` classifications and an `OK` line. The
provisioner must report the `sql-execution-success` evaluator and one enabled
`agent-arena-sql-execution-online` rule.

### 3. Submit Boolean feedback

Ask the stale service for the governed metric, retain only its trace ID, and confirm
that the operational evaluator cannot see the semantic defect:

```bash
source .env
export WINNER_CONFIG_ID="${WINNER_CONFIG_ID:-qwen3.7-flash__P2_fewshot}"
ASK_BODY=$(.venv/bin/python -c \
  'import json,sys; print(json.dumps({"question": sys.argv[1], "config_id": sys.argv[2]}))' \
  "How many active customers do we have?" "$WINNER_CONFIG_ID")
TRACE_ID=$(curl -fsS http://localhost:8100/ask \
  -H 'content-type: application/json' -d "$ASK_BODY" | \
  .venv/bin/python -c \
  'import json,sys; data=json.load(sys.stdin); assert data["policy_version"] == "policy-v1"; print(data["trace_id"])')
.venv/bin/python -m scripts.verify_online_scores "$TRACE_ID" \
  sql-execution-success=true
```

Record an idempotent Boolean thumbs-down on that exact trace and verify both scores:

```bash
FEEDBACK_BODY=$(.venv/bin/python -c \
  'import json,sys; print(json.dumps({"trace_id": sys.argv[1], "value": False, "comment": "stale business definition"}))' \
  "$TRACE_ID")
curl -fsS http://localhost:8100/feedback \
  -H 'content-type: application/json' -d "$FEEDBACK_BODY" | \
  .venv/bin/python -c \
  'import json,sys; assert json.load(sys.stdin)["ok"] is True; print("feedback recorded")'
.venv/bin/python -m scripts.verify_online_scores "$TRACE_ID" \
  sql-execution-success=true user-thumbs=false
```

### 4. Investigate in the Langfuse annotation UI — manual step

This is an intentionally **UI-only human judgment step**; none of the commands above
creates or completes an annotation task. In Langfuse:

1. Create a Human Annotation queue named `production-investigation-<session>` and
   attach score configs `observed-issue` (TEXT), `failure-category` (CATEGORICAL,
   including `stale-business-policy`), and `approved-for-golden` (BOOLEAN).
2. Filter Tracing for `user-thumbs = false`, add the trace to the queue, and inspect
   its question, generated SQL, result, model, prompt, `policy_version`, and scores.
3. Compare the stale behavior with the governed definition. For this reviewed
   incident, the corrected SQL counts distinct customers with a qualifying order in
   the last 30 days and excludes both cancelled and returned orders.
4. Record the observed issue, set `failure-category = stale-business-policy`, set
   `approved-for-golden = true`, enter the corrected output, and complete the task.
   Preserve the source trace ID and annotation task ID.

The human decision is authoritative: negative feedback is a triage signal, not
ground truth. Queue names use a session suffix because an existing Langfuse queue may
not be reconfigurable.

### 5. Promote production-provenance golden cases

An operator normally exports approved tasks to the ignored `reviewed.json`. Each
record must contain `id`, `question`, `golden_sql`, `tier`, `ordered`, `source`,
`source_trace_id`, `failure_category`, `source_policy_version`, and `annotation_id`.
Production records use `source = production-feedback`.

The repository does not track mutable operator state. An operator can copy the
tracked shape to `reviewed.json`, fill it from completed annotation tasks, and review
the file before promotion. Do not overwrite an existing operator file:

```bash
test -e reviewed.json || \
  cp tests/fixtures/reviewed.production-example.json reviewed.json
```

For a reproducible learner continuation, promote the tracked, already-reviewed
incident directly. It contains three approved phrasings (`prod-active-001` through
`prod-active-003`) and leaves any existing `reviewed.json` untouched:

```bash
source .env
.venv/bin/python -m scripts.promote_to_golden \
  tests/fixtures/reviewed.production-example.json
```

Copying this fixture does **not** automate or replace the human annotation step. The
promotion command validates provenance, snapshots each corrected query's result from
ClickHouse, and idempotently upserts the cases into `arena-golden`.

### 6. Run baseline and candidate on the same dataset

Provision the general, catalog-based policy judge for experiments. This phase keeps
its online serving rule disabled until calibration is complete:

```bash
source .env
export WINNER_MODEL="${WINNER_MODEL:-qwen3.7-flash}"
export WINNER_PROMPT="${WINNER_PROMPT:-P2_fewshot}"
.venv/bin/python -m scripts.provision_online_evaluators \
  --business-policy-experiments
.venv/bin/python -m eval.harness --run-id online-loop-baseline \
  --policy-version policy-v1 --models "$WINNER_MODEL" --prompts "$WINNER_PROMPT" \
  --wait-for-score business-policy-adherence
.venv/bin/python -m eval.harness --run-id online-loop-candidate \
  --policy-version policy-v2 --models "$WINNER_MODEL" --prompts "$WINNER_PROMPT" \
  --wait-for-score business-policy-adherence
```

Both runs must contain the same 22 items and every trace must receive `correctness`,
`agent-arena-llm-judge`, and `business-policy-adherence`. In the calibrated workshop
snapshot, `policy-v1` scored 16/22 and `policy-v2` scored 19/22; the candidate fixed
all three production-derived items without an aggregate regression.

### 7. Calibrate the general judge and enable safely

In Langfuse Experiments, compare the two runs before enabling the online rule. Require
all three `prod-active-*` items to move from `FAIL`/incorrect under `policy-v1` to
`PASS`/correct under `policy-v2`, and spot-check the candidate's revenue and
view-to-purchase conversion items as `PASS`; a generic product/customer count should
be `NOT_APPLICABLE`. Do not enable if those gates or score completeness fail.

After the checks pass, the command below independently refuses to enable unless it
finds a dataset-scoped experiment score with the exact name
`business-policy-adherence`:

```bash
source .env
.venv/bin/python -m scripts.provision_online_evaluators \
  --enable-business-policy-online
```

The expected rule is `agent-arena-business-policy-online`, enabled for root
`chat_turn` observations. Langfuse names Experiment scores after the Experiment rule
(`business-policy-adherence`) and online observation scores after this distinct
online rule (`agent-arena-business-policy-online`). Keep both exact names
fail-closed; do not treat them as fuzzy aliases.

### 8. Verify future live scores

Stop the `policy-v1` server with Ctrl-C. Start the candidate in the first terminal:

```bash
source .env
AGENT_ARENA_POLICY_VERSION=policy-v2 \
  .venv/bin/uvicorn serving.api:app --port 8100
```

In the second terminal, define a helper that sends `/ask`, confirms the candidate
policy and successful execution, and returns only the trace ID:

```bash
source .env
export WINNER_CONFIG_ID="${WINNER_CONFIG_ID:-qwen3.7-flash__P2_fewshot}"
ask_trace() {
  local question="$1"
  local body
  body=$(.venv/bin/python -c \
    'import json,sys; print(json.dumps({"question": sys.argv[1], "config_id": sys.argv[2]}))' \
    "$question" "$WINNER_CONFIG_ID")
  curl -fsS http://localhost:8100/ask \
    -H 'content-type: application/json' -d "$body" | \
    .venv/bin/python -c \
    'import json,sys; data=json.load(sys.stdin); assert data["policy_version"] == "policy-v2" and data["outcome"] == "ok"; print(data["trace_id"])'
}
```

Ask the four final questions and assert the exact operational and online-rule score
names on every trace:

```bash
ACTIVE_TRACE=$(ask_trace "How many active customers do we have?")
.venv/bin/python -m scripts.verify_online_scores "$ACTIVE_TRACE" \
  sql-execution-success=true agent-arena-business-policy-online=PASS

REVENUE_TRACE=$(ask_trace "What was revenue in the last 30 days?")
.venv/bin/python -m scripts.verify_online_scores "$REVENUE_TRACE" \
  sql-execution-success=true agent-arena-business-policy-online=PASS

CONVERSION_TRACE=$(ask_trace \
  "What is our view-to-purchase conversion rate for the last 7 days?")
.venv/bin/python -m scripts.verify_online_scores "$CONVERSION_TRACE" \
  sql-execution-success=true agent-arena-business-policy-online=PASS

PRODUCT_TRACE=$(ask_trace "How many products are there?")
.venv/bin/python -m scripts.verify_online_scores "$PRODUCT_TRACE" \
  sql-execution-success=true agent-arena-business-policy-online=NOT_APPLICABLE
```

### 9. Reset behavior and sampling

Stop the foreground serving process with Ctrl-C, or stop all workshop services with
`scripts/arena.sh stop`. `scripts/arena.sh down` additionally drops the workshop
ClickHouse database and read-only user; it does not delete Langfuse datasets,
experiments, annotation queues, or scores. Promotion and evaluator provisioning are
idempotent, while `reviewed.json` remains ignored operator state.

The evaluator rules use 100% sampling so every workshop trace produces visible
evidence. That is a teaching setting, not a production recommendation: choose a
production sampling rate from traffic volume, evaluator cost, latency, risk, and the
coverage needed for incident detection.

---

## Configuration

Everything tunable lives in [`config.yaml`](config.yaml):
- **`models`** — OpenRouter model id, display name, family, and per-1M-token input/output prices (used for cost).
- **`prompts`** — the strategies `P1_zeroshot` … `P3_dialect`.
- **`grid`** — which models × prompts to actually run (`["*"]` = all).
- **`clickhouse.query_limits`** — the server-side caps enforced on agent SQL.

Adding a model or prompt is a config edit, not a code change.

---

## Workshop guide

The step-by-step, dual-track (learner + instructor) guide for this workshop is published
with the other ClickHouse workshops at
[/docs/agent-arena](https://workshop.demohouse.cloud/docs/agent-arena), and the landing
page is at [/agent-arena](https://workshop.demohouse.cloud/agent-arena). Its source lives
in this repository at `site/content/docs/agent-arena/`; see
`site/README.md` to run the site locally.

---

## Deploy

The arena runs locally for the workshop: `scripts/arena.sh up` seeds ClickHouse and starts
the dashboard API (`:8000`), the serving API (`:8100`), and the web UI (`:5174`). There is
no hosted deployment in this repository — point the two FastAPI apps
(`dashboard/app.py`, `serving/api.py`) at any Python host if you want a shared instance,
and build `web/` with `npm run build` for a static bundle.

---

## Troubleshooting

- **Port 8000 in use** — the dashboard API uses `:8000`; `arena.sh up` warns if it can't
  bind. Free the port (or set `API_PORT=...`) and re-run `scripts/arena.sh serve`.
- **Leaderboard tab says "can't reach API"** — start the JSON API:
  `source .env && .venv/bin/uvicorn dashboard.app:app --port 8000`.
- **Chat tab can't reach the serving API** — start it:
  `source .env && .venv/bin/uvicorn serving.api:app --port 8100`.
- **Langfuse drill-down empty** — a run created *before* the Langfuse integration won't
  have trace links/sessions; run a fresh grid with the current harness.
- **Leaderboard is empty although the harness ran** — confirm the Langfuse keys belong
  to the project that received the Experiments and that its evaluators produced a
  `correctness` score. The dashboard reads Langfuse directly and has no ClickHouse
  fallback.

---

## Docs
- Workshop guide: [/docs/agent-arena](https://workshop.demohouse.cloud/docs/agent-arena)
- Web UI details: [`web/README.md`](web/README.md)
