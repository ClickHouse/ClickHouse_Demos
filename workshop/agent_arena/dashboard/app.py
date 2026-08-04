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


# --- Run the benchmark from the UI (spawns the harness as a subprocess) -------
# Local/demo use only: this lets the web app trigger `python -m eval.harness`.
# The API process must have an OpenRouter API key in its environment.
import subprocess
import sys
import threading
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_run_state = {"running": False, "run_id": None, "started_at": None,
              "lines": [], "returncode": None}
_run_lock = threading.Lock()


@app.get("/api/grid-options")
def grid_options():
    """Prompts available to run (from config.yaml), with descriptions for hovers.
    Models now come from the catalog endpoint /api/models."""
    return {
        "prompts": [{"name": p.name, "desc": p.desc or p.name} for p in _cfg.prompts],
    }


@app.get("/api/profiles")
def profiles():
    """Preset model selections (config.yaml `profiles`) resolved to invocable ids."""
    by_name = {m.name: m.id for m in _cfg.models}
    return [{"name": p.name, "desc": p.desc,
             "model_ids": [by_name[n] for n in p.models if n in by_name]}
            for p in _cfg.profiles]


# --- Model catalog ------------------------------------------------------------
@app.get("/api/models")
def models():
    """Curated config.yaml models grouped by family, with LIVE OpenRouter prices
    (falls back to config.yaml prices with a `degraded` reason if unreachable)."""
    prices, degraded = {}, None
    try:
        from agents.llm import fetch_openrouter_prices
        prices = fetch_openrouter_prices(_cfg.openrouter.base_url)
    except Exception as e:  # noqa: BLE001
        degraded = str(e)
    fam_map = {}
    for m in _cfg.models:
        pin, pout = prices.get(m.id, (m.price_per_1m_in, m.price_per_1m_out))
        fam_map.setdefault(m.family, []).append(
            {"id": m.id, "name": m.name, "price_per_1m_in": pin,
             "price_per_1m_out": pout, "in_default": True})
    families = [{"family": fam, "models": ms} for fam, ms in fam_map.items()]
    return {"provider": "openrouter", "region": "OpenRouter",
            "families": families, "degraded": degraded}


def _stream_harness(cmd: list[str]):
    proc = subprocess.Popen(cmd, cwd=str(_ROOT), env=os.environ.copy(),
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True, bufsize=1)
    for line in proc.stdout:
        line = line.rstrip()
        with _run_lock:
            _run_state["lines"].append(line)
            _run_state["lines"] = _run_state["lines"][-400:]  # keep tail
    proc.wait()
    with _run_lock:
        _run_state["running"] = False
        _run_state["returncode"] = proc.returncode
        _results_cache.update(loaded_at=0.0, model=None)


@app.post("/api/run")
def start_run(body: dict):
    with _run_lock:
        if _run_state["running"]:
            return {"ok": False, "error": "a run is already in progress",
                    "run_id": _run_state["run_id"]}
        run_id = body.get("run_id") or f"ui-{int(time.time())}"
        cmd = [sys.executable, "-m", "eval.harness", "--run-id", run_id]
        models = body.get("models") or []
        if models and isinstance(models[0], dict):
            # full specs from the live-catalog browser → temp models-file
            import json
            specs = [{"id": m["id"], "name": m["name"], "family": m.get("family", "other"),
                      "price_per_1m_in": float(m.get("price_in") or 0),
                      "price_per_1m_out": float(m.get("price_out") or 0)} for m in models]
            (_ROOT / ".run").mkdir(exist_ok=True)
            mf = _ROOT / ".run" / f"models-{run_id}.json"
            mf.write_text(json.dumps(specs))
            cmd += ["--models-file", str(mf)]
        elif models:
            cmd += ["--models", ",".join(models)]
        if body.get("prompts"):
            cmd += ["--prompts", ",".join(body["prompts"])]
        _run_state.update(running=True, run_id=run_id, started_at=time.time(),
                          lines=[f"$ {' '.join(cmd[2:])}"], returncode=None)
    threading.Thread(target=_stream_harness, args=(cmd,), daemon=True).start()
    return {"ok": True, "run_id": run_id}


@app.get("/api/run/status")
def run_status():
    with _run_lock:
        return dict(_run_state)
