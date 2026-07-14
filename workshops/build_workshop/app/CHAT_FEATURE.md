# AI Chat (NL-to-SQL) + Langfuse observability

This adds a "chat with the data" feature to the NYC Taxi Ops War Room. A participant
types a plain-English question, an OpenAI model turns it into a single read-only
ClickHouse `SELECT`, the backend runs it under guardrails, and the dashboard renders
the answer, the generated SQL, a table, and (when applicable) a chart. Every chat turn
is traced to Langfuse Cloud so participants can inspect prompts, model output, latency,
and cost.

## Architecture

```
Browser (Ops dashboard)
  ChatPanel  -- POST /api/chat {message, conversation_id} -->
      |
      v
FastAPI backend (backend/app)
  - chat.py            router: POST /api/chat, orchestration + HTTP errors
  - chat_service.py    schema introspection (cached), NL-to-SQL prompt, SQL guardrails,
                       OpenAI client (Langfuse drop-in), read-only execution
  - schemas.py         ChatRequest / ChatResponse / ChatChartSpec
  - settings.py        OPENAI_/LLM_/LANGFUSE_ env config
      |
      |-- OpenAI Chat Completions (via langfuse.openai)  -->  Langfuse Cloud (traces)
      |-- ClickHouse (clickhouse-connect)                -->  nyc_tlc_data.taxi_trips / taxi_zones
```

Request/response contract:

```jsonc
// request
{ "message": "top pickup zones by trips in July 2022", "conversation_id": "optional-uuid" }

// response
{
  "answer": "The busiest pickup zones by trip count.",
  "sql": "SELECT z.zone AS zone, count() AS trips FROM taxi_trips t ... LIMIT 100",
  "rows": [ { "zone": "JFK Airport", "trips": 1200 }, ... ],
  "chart": { "type": "bar", "x": "zone", "y": "trips" }   // or null
}
```

### NL-to-SQL flow

1. On the first request the backend introspects `taxi_trips` and `taxi_zones` with
   `DESCRIBE TABLE` and caches the column list. If ClickHouse is unreachable it falls
   back to an embedded copy of the schema (matching `db/cloud/001_cloud_schema.sql`),
   so the prompt is always grounded.
2. The system prompt embeds that schema plus four few-shot NL-to-SQL examples using the
   app's own query-builder patterns (`toStartOfInterval`, `quantileTDigest`, zone joins,
   `total_amount` revenue fallback — see `backend/app/query_builders.py`).
3. The model returns a strict JSON object `{answer, sql, chart}`. The default path uses
   OpenAI JSON mode (`response_format={"type": "json_object"}`). If you point
   `LLM_BASE_URL` at a provider that does not support JSON mode, the parser also accepts a
   JSON object wrapped in a ```` ```json ```` code fence or surrounded by prose as a
   fallback, but a JSON-mode-capable chat model is still the recommended path.
   A conversational or out-of-scope question comes back with `sql: null` and is
   answered without touching ClickHouse.

### Guardrails (defense in depth)

Applied in `chat_service.sanitize_select_sql` before anything runs:

- **Single statement**: comments are stripped, trailing `;` removed; any remaining `;`
  is rejected (blocks multi-statement injection).
- **SELECT only**: the statement must start with `SELECT` or `WITH` (CTEs are used by
  the app's own queries). Write/DDL keywords (`INSERT`, `UPDATE`, `DELETE`, `ALTER`,
  `DROP`, `CREATE`, `TRUNCATE`, `SYSTEM`, `INTO OUTFILE`, ...) are rejected.
- **LIMIT enforcement**: if the query has no `LIMIT`, `LIMIT 100` is appended.
- **Per-query ClickHouse settings**: `max_execution_time=30`, `max_result_rows=1000`,
  and `readonly=2`.
  - `readonly=1` forbids writes **and** blocks changing any setting, so it would reject
    the `max_execution_time` / `max_result_rows` we pass on the same request.
  - `readonly=2` forbids writes but still allows those per-query settings — which is
    why it is used here.

The generated SQL is always returned to the client (showing it is a workshop teaching
point).

### Langfuse instrumentation

`chat_service.py` uses the Langfuse v4 SDK. When `LANGFUSE_PUBLIC_KEY` and
`LANGFUSE_SECRET_KEY` are set, the Langfuse singleton is configured with
`LANGFUSE_BASE_URL` (the v4 env name; `LANGFUSE_HOST` is accepted as a fallback alias) and:

- One chat turn runs inside `run_chat`, decorated with the v4 `@observe(name="chat")`
  decorator, so schema lookup, the model call, guardrails, and execution group into a
  single trace.
- The OpenAI call goes through the `langfuse.openai` drop-in wrapper, so the completion is
  captured automatically as a generation named `chat` (`name="chat"` on the call).
- `conversation_id` is wired to the trace via `langfuse.propagate_attributes(session_id=...)`
  (v4 replaced `update_current_trace`), so a multi-turn chat groups into one Langfuse session.
- Buffered events are flushed on process exit via a FastAPI lifespan shutdown that calls
  `get_client().shutdown()`.

When the keys are absent, `@observe` is a passthrough and the plain `openai.OpenAI` client
is used, so nothing touches Langfuse and no warnings are emitted. v4 requires Python >=3.10
and Pydantic v2; the backend already uses `pydantic` 2.x / `pydantic-settings`, so it is
compatible. The deprecated `start_span` / `start_generation` helpers are not used (the
drop-in wrapper plus `@observe` is sufficient).

## Graceful degradation

- **No `OPENAI_API_KEY`**: `POST /api/chat` returns `503` with a setup hint; the rest of
  the dashboard and API are unaffected.
- **No Langfuse keys**: chat works normally, tracing is disabled (no warnings, no errors).
- **ClickHouse down at prompt time**: schema introspection falls back to the static schema.

## Environment variables (for the compose owner)

Another teammate owns `docker-compose.yml`. Add the following to the **backend** service's
`environment:` block. All are optional at the process level — the app boots without them
(chat just returns 503 until `OPENAI_API_KEY` is set). Values use compose interpolation so
participants supply their own keys via a root `.env` file or the shell.

```yaml
    environment:
      # ... existing CLICKHOUSE_* / API_CORS_ORIGINS / QUERY_* vars ...
      - OPENAI_API_KEY=${OPENAI_API_KEY:-}
      - LLM_MODEL=${LLM_MODEL:-gpt-5.4-mini}
      - LLM_BASE_URL=${LLM_BASE_URL:-https://api.openai.com/v1}
      - LANGFUSE_PUBLIC_KEY=${LANGFUSE_PUBLIC_KEY:-}
      - LANGFUSE_SECRET_KEY=${LANGFUSE_SECRET_KEY:-}
      - LANGFUSE_BASE_URL=${LANGFUSE_BASE_URL:-https://us.cloud.langfuse.com}
```

`LANGFUSE_BASE_URL` is the Langfuse v4 env name. `LANGFUSE_HOST` is also accepted as a
fallback alias, so an existing `LANGFUSE_HOST` value still works.

Example root `.env` for a participant:

```bash
OPENAI_API_KEY=sk-...
# LLM_MODEL=gpt-5.4-mini              # any JSON-mode-capable chat model
# LLM_BASE_URL=https://api.openai.com/v1
LANGFUSE_PUBLIC_KEY=pk-lf-...
LANGFUSE_SECRET_KEY=sk-lf-...
LANGFUSE_BASE_URL=https://us.cloud.langfuse.com   # or https://cloud.langfuse.com for EU
```

New Python dependencies (already pinned in `backend/requirements.txt`): `openai==2.45.0`,
`langfuse==4.14.0`.

## Testing locally

The backend targets Python 3.11 (see `backend/Dockerfile`).

### Guardrail unit tests (no keys, no ClickHouse)

```bash
cd backend
python3.11 -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest tests/test_chat_guardrails.py -v
```

These cover SELECT-only enforcement, multi-statement / comment-hidden rejection,
write/DDL rejection, LIMIT injection, and the schema fallback. They run in isolation and
do not need the live API (the integration `wait_for_api` fixture is overridden).

### Without keys (503 path)

```bash
# with the stack running (or `uvicorn app.main:app` from backend/)
curl -s -X POST localhost:8000/api/chat -H 'content-type: application/json' \
  -d '{"message":"top zones"}'
# -> 503 {"detail":"AI chat is not configured. Set OPENAI_API_KEY ..."}
```

### With keys (full path)

```bash
export OPENAI_API_KEY=sk-...
# optionally: export LANGFUSE_PUBLIC_KEY=... LANGFUSE_SECRET_KEY=... LANGFUSE_BASE_URL=...
# start ClickHouse + backend (docker compose up -d clickhouse backend), then:
curl -s -X POST localhost:8000/api/chat -H 'content-type: application/json' \
  -d '{"message":"top 10 pickup zones by trips in July 2022","conversation_id":"demo-1"}' | jq
```

In the UI, open the Ops dashboard (`http://localhost:8080/`), click **Ask AI** at the
bottom-right, and ask a question. Each answer has a **Show SQL** toggle, a results table,
and a chart when the model returns a chart spec.

### Frontend build

```bash
cd frontend
npm install
npm run build   # tsc -b && vite build
```

## Langfuse trace walkthrough (what a participant sees)

1. Ask a question in the chat panel (or via `curl`) with Langfuse keys set.
2. Open Langfuse Cloud > your project > **Tracing / Traces**.
3. A new trace named **`chat`** appears within a few seconds (events are batched in the
   background and flushed on shutdown; there is no per-request blocking flush).
4. Open the trace to see the `run_chat` span with one generation named **`chat`** (the
   OpenAI chat completion) inside it:
   - **Input**: the system prompt (schema + few-shot NL-to-SQL examples) and the user's
     question.
   - **Output**: the model's JSON `{answer, sql, chart}`.
   - **Model / latency / token usage / cost**: captured automatically by the drop-in.
5. Reuse the same `conversation_id` across turns and they group under one **session**
   (Sessions view), so a whole chat conversation is one timeline.

This is the teaching arc for the workshop: a natural-language question becomes grounded
SQL, guardrails keep execution safe and read-only, and Langfuse gives full observability
of the LLM call behind it.

If traces do not appear, confirm `LANGFUSE_BASE_URL` matches your project's region
(`https://us.cloud.langfuse.com` vs `https://cloud.langfuse.com`) and that both keys
are set on the backend.
