# AgentArena

**Put a roster of LLMs into a contest and crown the winner — for NL→SQL over
ClickHouse — with LangFuse in the loop from the very beginning.**

AgentArena runs a grid of **{model} × {prompt strategy}** against a
ground-truthed set of business questions over a live ClickHouse dataset, grades
every answer by **execution accuracy** (did the query return the right *result*,
not the right *SQL text*), and ranks the configurations by **cost per correct
answer**. It answers, with evidence:

> *Which model + prompt should we ship for NL→SQL, and what does correctness cost?*

The flow is the one you'd actually use to ship an AI feature: **select a base
model → measure quality → continuously improve → release to production.**
LangFuse isn't bolted on afterward — it grades the contest (evaluators +
LLM-as-a-judge), stores every result and trace, powers the leaderboard through
its Public API, and then carries the winning
config into a real chatbot, where user 👍/👎 feedback flows back in as scores.
ClickHouse holds the business data the agent queries; LangFuse is the single
source of truth for benchmark results.

---

## What you get

A single web app (`web/`, http://localhost:5174) with four tabs:

- **Leaderboard** — the contest results: every model×prompt config ranked by
  accuracy, **cost-per-correct-answer** (the headline), latency, per-tier
  accuracy, and an outcome breakdown, with a **cost × accuracy** chart and a
  **best-value** ranking up top. Click any config to **drill into its
  per-question results**, each linking to its **LangFuse trace** (prompt →
  generated SQL → error → tokens → span timings). A **"View conversation"**
  button replays the agent's session **live from the LangFuse API**, and an
  **LLM-judge** column scores SQL quality.
- **Countdown** — a live-event "stage" screen for demos: a presenter countdown
  timer, the model families as contenders, and the current best accuracy per
  family (read live from the latest run).
- **Chat** — the production chatbot: ask a question against a picked
  model+prompt config and watch the SQL, cost, and latency; rate each answer
  👍/👎, which is written back to the trace as a **LangFuse score**.
- **Architecture** — an animated React Flow diagram of the whole system.

Plus the serving API (`serving/api.py`): **`POST /ask`** (run the agent live and
return SQL/results/cost/latency, traced to LangFuse) and **`POST /feedback`**
(attach a 👍/👎 score to a trace) — the same endpoints the Chat tab calls.

## Architecture

```text
Golden dataset → benchmark harness → agent → OpenRouter
                                      │
                                      └─ read-only SQL → ClickHouse v_* views
                         │
                         └─ Experiment Items + scores → LangFuse
                                                        │ Public API
                                                        ▼
                                                  leaderboard API/UI
```

The web app's **Architecture** tab renders the live component graph from
`web/src/diagram/graph.js`.

**The key idea — one service per job:**
- **ClickHouse** is the application database: it holds the business data and
  executes generated and golden SQL.
- **LangFuse** is the evaluation store: Experiments, result payloads, exact
  cost/latency measurements, evaluator scores, and conversations. The local
  leaderboard reads those Experiment Items through the LangFuse Public API.

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
LangFuse Experiment Item; LangFuse evaluators attach correctness, outcome, and
LLM-judge scores there.

---

## Getting started

### Prerequisites
- **Python 3.11**, **Node 18+** (for the web UI).
- A **ClickHouse Cloud** service, a **LangFuse Cloud** project, and an
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
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
(cd web && npm install)        # only if you want the web UI
```

### 2. Create `.env` (not committed)
See [`.env.example`](.env.example) for the full template:
```bash
# ClickHouse Cloud (business data queried by the agent)
export CLICKHOUSE_CLOUD_HOST=xxx.clickhouse.cloud
export CLICKHOUSE_CLOUD_USER=default
export CLICKHOUSE_CLOUD_PASSWORD=
export CLICKHOUSE_CLOUD_DATABASE=arena
export ARENA_RO_PASSWORD=...                 # password for the read-only agent user (created by setup)

# OpenRouter (LLM provider)
export OPENROUTER_API_KEY=sk-or-...
export OPENROUTER_BASE_URL=https://openrouter.ai/api/v1

# LangFuse Cloud (eval store + tracing)
export LANGFUSE_BASE_URL=https://us.cloud.langfuse.com
export LANGFUSE_PUBLIC_KEY=pk-lf-...
export LANGFUSE_SECRET_KEY=sk-lf-...
```

### 3. Verify connectivity
```bash
source .env
python -m scripts.check_connectivity          # ClickHouse + LangFuse + OpenRouter reachable?
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
source .env && python -m eval.harness --run-id demo
```
- Provision the OpenRouter-backed LangFuse judge once with
  `python -m scripts.provision_langfuse_evaluators`; it creates `arena-golden` before
  installing the filtered rule. The correctness code evaluator still follows
  `eval/langfuse_evaluators/README.md`.
- The correctness code evaluator + `llm_judge` run server-side in LangFuse. The
  harness waits until the scores arrive; it does not copy them to another store.
- Each invocation is a **run** (`run_id`) made up of LangFuse Experiments and
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

## Configuration

Everything tunable lives in [`config.yaml`](config.yaml):
- **`models`** — OpenRouter model id, display name, family, and per-1M-token input/output prices (used for cost).
- **`prompts`** — the strategies `P1_zeroshot` … `P3_dialect`.
- **`grid`** — which models × prompts to actually run (`["*"]` = all).
- **`profiles`** — curated presets (Budget tier / Frontier / Everything) for the web UI's "Run benchmark" panel.
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
  `source .env && uvicorn dashboard.app:app --port 8000`.
- **Chat tab can't reach the serving API** — start it:
  `source .env && uvicorn serving.api:app --port 8100`.
- **LangFuse drill-down empty** — a run created *before* the LangFuse integration won't
  have trace links/sessions; run a fresh grid with the current harness.
- **Leaderboard is empty although the harness ran** — confirm the LangFuse keys belong
  to the project that received the Experiments and that its evaluators produced a
  `correctness` score. The dashboard reads LangFuse directly and has no ClickHouse
  fallback.

---

## Docs
- Workshop guide: [/docs/agent-arena](https://workshop.demohouse.cloud/docs/agent-arena)
- Web UI details: [`web/README.md`](web/README.md)
