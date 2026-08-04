# AgentArena — Web UI

A focused [React](https://react.dev) ([Vite](https://vite.dev)) SPA with two tabs:
- **Leaderboard** — the contest results, read from LangFuse Experiments through its
  Public API:
  run selector, winner cards, sortable leaderboard ranked by
  **cost-per-correct-answer**, a per-tier heatmap, and an outcome breakdown — plus
  the **LangFuse-powered** layer:
  - an **LLM-judge** column (an OpenRouter-scored SQL-quality dimension stored as a
    LangFuse score);
  - **click any config row** to drill into its per-question results, each linking
    to its **LangFuse trace** (prompt → SQL → error → span timings → tokens);
  - **"View conversation (LangFuse)"** loads the config's session **live from the
    LangFuse API** and renders the full multi-turn conversation in-app;
  - **session ↗** and **Open experiment in LangFuse ↗** deep-link to LangFuse's
    native session + Dataset/Experiment comparison views.
- **Chat** — the production chatbot: ask a question against a picked model+prompt
  config, see the generated SQL/results/cost/latency, and rate each answer
  👍/👎 — feedback is written back to the trace as a LangFuse score.

## Run

```bash
cd web
npm install
npm run dev            # → http://localhost:5174
```

The **Leaderboard** and **Chat** tabs need the dashboard/serving APIs running:

```bash
# from the repo root, in other shells:
source .env && uvicorn dashboard.app:app --port 8000
source .env && uvicorn serving.api:app --port 8100
```

The UI calls `http://localhost:8000` (dashboard) and `http://localhost:8100`
(serving) by default; override with `VITE_API_BASE=http://host:8000` and
`VITE_SERVING_BASE=http://host:8100` when running `npm run dev`. Both APIs have
CORS enabled for the SPA.

## Files
- `src/App.jsx` — tab shell (Leaderboard / Chat).
- `src/ui.jsx` — shared presentational atoms (brand lock, icons, ClickHouse logomark).
- `src/api.js` — dashboard/serving API bases + fetch helpers (`VITE_API_BASE`, `VITE_SERVING_BASE`).
- `src/leaderboard/Leaderboard.jsx` — leaderboard UI (fetches the dashboard API).
- `src/chat/Chat.jsx` — the production chat UI (calls the serving API's `/ask` + `/feedback`).

> The leaderboard data comes from `dashboard/app.py` (a FastAPI adapter over the
> LangFuse Public API). The chat data comes from `serving/api.py`. This React
> UI is the only front-end.
