"""Leaderboard JSON API. Reads Langfuse experiments as its source of truth.
Consumed by the React web UI (../web, Leaderboard tab). ClickHouse remains the
agent's query target, but benchmark results are not duplicated there.
Usage: source .env && uvicorn dashboard.app:app --reload --port 8000
"""
import os
import time

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from arena.config import load_config
from dashboard.langfuse_results import (
    build_read_model as _build_read_model,
    fetch_experiment_rows as _fetch_experiment_rows,
    normalize_experiment_item as _normalize_experiment_item,
)

app = FastAPI(title="AgentArena Leaderboard API")

# Lazy LangFuse client (only built when an /api/lf/* endpoint is hit).
_lf = None
_lf_base = None


def _langfuse():
    global _lf, _lf_base
    if _lf is None:
        from langfuse import Langfuse
        from eval.langfuse_adapter import _project_base
        lc = _cfg.langfuse
        _lf = Langfuse(public_key=lc.public_key, secret_key=lc.secret_key, host=lc.host)
        _lf_base = _project_base(lc)
    return _lf


def _lf_get(path: str, **params):
    """Call the LangFuse public REST API (version-proof — the v2 fetch_* SDK
    methods don't exist in the OTEL SDK v3+)."""
    import base64
    import json
    import urllib.request
    from urllib.parse import urlencode
    _langfuse()  # ensures _lf_base is resolved
    lc = _cfg.langfuse
    auth = base64.b64encode(f"{lc.public_key}:{lc.secret_key}".encode()).decode()
    url = f"{lc.host}/api/public/{path}"
    if params:
        url += "?" + urlencode(params)
    req = urllib.request.Request(url, headers={"Authorization": f"Basic {auth}"})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())
# Allow the React SPA (Vite dev server / static build) to call this JSON API.
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["GET", "POST"], allow_headers=["*"],
)
_cfg = load_config()
_results_cache = {"loaded_at": 0.0, "model": None}


def _read_model():
    """Fetch and briefly cache the small AgentArena experiment result set."""
    now = time.monotonic()
    if _results_cache["model"] is not None and now - _results_cache["loaded_at"] < 2:
        return _results_cache["model"]
    _langfuse()
    rows = _fetch_experiment_rows(
        lambda params: _lf_get("experiment-items", **params),
        _lf_base,
        os.getenv("LANGFUSE_LEADERBOARD_FROM"),
    )
    model = _build_read_model(rows)
    _results_cache.update(loaded_at=now, model=model)
    return model


def _selected_run(model, run_id):
    return run_id or (model["runs"][0] if model["runs"] else None)


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/api/runs")
def runs():
    return _read_model()["runs"]


@app.get("/api/leaderboard")
def leaderboard(run_id: str | None = None):
    model = _read_model()
    return model["leaderboard"].get(_selected_run(model, run_id), [])


@app.get("/api/tiers")
def tiers(run_id: str | None = None):
    model = _read_model()
    return model["tiers"].get(_selected_run(model, run_id), [])


@app.get("/api/outcomes")
def outcomes(run_id: str | None = None):
    model = _read_model()
    return model["outcomes"].get(_selected_run(model, run_id), [])


@app.get("/api/questions")
def questions(run_id: str, config_id: str):
    """Per-question drill-down for one config, sourced from Langfuse."""
    return _read_model()["questions"].get((run_id, config_id), [])


@app.get("/api/lf/session")
def lf_session(session_id: str):
    """List a session's exchanges from Langfuse experiment items."""
    if "__" not in session_id:
        return {"session_id": session_id, "source": "langfuse", "exchanges": []}
    rid, cid = session_id.split("__", 1)
    rows = _read_model()["questions"].get((rid, cid), [])
    exchanges = [{
        "question_id": row["question_id"],
        "question": row.get("question"),
        "sql": row["sql"],
        "outcome": row["outcome"],
        "trace_id": row["trace_id"],
        "trace_url": row.get("trace_url"),
    } for row in rows]
    return {"session_id": session_id, "source": "langfuse", "exchanges": exchanges}


@app.get("/api/lf/trace")
def lf_trace(trace_id: str):
    """Return the transcript already present on a Langfuse experiment item."""
    turns = []
    for rows in _read_model()["questions"].values():
        row = next((candidate for candidate in rows if candidate["trace_id"] == trace_id), None)
        if row:
            turns = [{"role": m.get("role"), "content": m.get("content")}
                     for m in row.get("transcript", []) if isinstance(m, dict)]
            break
    return {"trace_id": trace_id, "turns": turns}


@app.get("/api/meta")
def meta():
    """Surface the LangFuse project base + dataset for experiment deep links."""
    _langfuse()
    from eval.langfuse_adapter import DATASET_NAME
    return {"langfuse_base": _lf_base, "dataset": DATASET_NAME,
            "datasets_url": f"{_lf_base}/datasets" if _lf_base else None}


@app.get("/api/grid-options")
def grid_options():
    """Prompt descriptions used by leaderboard labels and tooltips."""
    return {
        "prompts": [{"name": p.name, "desc": p.desc or p.name} for p in _cfg.prompts],
    }
