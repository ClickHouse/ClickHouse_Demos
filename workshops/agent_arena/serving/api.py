"""Live Chat BI serving API (design §11, C11).

POST /ask {question, config_id} -> runs the agent live and returns
{sql, columns, rows, cost_usd, latency_ms, outcome}. Reuses the same agent core
and read-only sandbox as the benchmark harness, so the demo and the benchmark
share one code path. Traced to Langfuse (chat_turn + llm_call).

  source .env && .venv/bin/uvicorn serving.api:app --port 8100
  curl -s localhost:8100/ask -H 'content-type: application/json' \
    -d '{"question":"How many customers are there?","config_id":"claude-sonnet-5__P1_zeroshot"}'
"""
import os
import time
import uuid
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from langfuse import get_client
from arena.config import load_config
from agents.chclient import ROClickHouseClient
from agents.llm import OpenRouterClient, cost_usd
from agents.loop import run_agent
from agents.policy import load_policy, with_policy
from agents.sqlguard import validate_select_only  # noqa: F401  (exercised via loop)
from eval.langfuse_adapter import LangfuseTracer, emit_agent_trace

app = FastAPI(title="Agent Arena — Serving API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"],
                    allow_headers=["*"])
_cfg = load_config()
_ro = ROClickHouseClient(_cfg.clickhouse)
_llm = OpenRouterClient(_cfg.openrouter.base_url, _cfg.openrouter.api_key)
# Configure the global Langfuse client (get_client() uses it inside /ask).
_lf = LangfuseTracer(_cfg.langfuse)

with open("schema/schema_context.md") as f:
    _schema_ctx = f.read()
_policy = load_policy(os.getenv("AGENT_ARENA_POLICY_VERSION", "policy-v2"))
_agent_ctx = with_policy(_schema_ctx, _policy)


class AskRequest(BaseModel):
    question: str
    config_id: str  # "<model_name>__<prompt_name>", e.g. claude-sonnet-5__P1_zeroshot
    session_id: str | None = None  # group turns of one conversation; auto if omitted


class AskResponse(BaseModel):
    config_id: str
    policy_version: str
    sql: str | None
    columns: list[str] | None
    rows: list | None
    cost_usd: float
    latency_ms: int
    outcome: str
    error: str | None
    trace_id: str          # Langfuse trace id (for attaching user feedback)
    session_id: str


class FeedbackRequest(BaseModel):
    trace_id: str
    value: bool
    comment: str | None = None


def _split_config(config_id: str):
    try:
        model_name, prompt_name = config_id.split("__", 1)
        return _cfg.model_by_name(model_name), _cfg.prompt_by_name(prompt_name)
    except (ValueError, StopIteration):
        raise HTTPException(400, f"unknown config_id '{config_id}'")


@app.get("/configs")
def configs():
    models, prompts = _cfg.resolved_grid()
    return {"config_ids": [f"{m}__{p}" for m in models for p in prompts]}


@app.post("/ask", response_model=AskResponse)
def ask(req: AskRequest):
    mcfg, pcfg = _split_config(req.config_id)
    session_id = req.session_id or f"ask-{uuid.uuid4().hex[:12]}"
    client = get_client()
    trace_id = ""
    with client.start_as_current_observation(name="chat_turn", as_type="span"):
        t0 = time.time()
        ar = run_agent(req.question, mcfg, pcfg, _agent_ctx, _ro, _llm,
                       dict(_cfg.openrouter.inference),
                       max_retries=_cfg.eval.default_max_retries)
        latency_ms = int((time.time() - t0) * 1000)
        c = cost_usd(ar.usage, mcfg.price_per_1m_in, mcfg.price_per_1m_out)
        # cap returned rows so a wide result doesn't bloat the response
        rows = [list(r) for r in (ar.rows or [])][:200]
        payload = {"sql": ar.sql, "columns": ar.cols, "rows": rows,
                   "error": ar.error, "outcome_hint": ar.outcome_hint}
        trace_id = emit_agent_trace(
            trace_name="chat_turn", session_id=session_id,
            tags=[req.config_id, _policy.version, mcfg.name, pcfg.name, "serving"],
            metadata={"config_id": req.config_id, "model": mcfg.name,
                      "prompt": pcfg.name, "latency_ms": latency_ms,
                      "cost_usd": round(c, 6),
                      "policy_version": _policy.version, "release": "serving"},
            question=req.question, model=mcfg.id, transcript=ar.transcript,
            sql=ar.sql, output_payload=payload, usage=ar.usage)
    try:
        client.flush()  # make the trace visible promptly for the demo
    except Exception:  # noqa: BLE001
        pass
    return AskResponse(
        config_id=req.config_id, policy_version=_policy.version,
        sql=ar.sql, columns=ar.cols, rows=rows,
        cost_usd=round(c, 6), latency_ms=latency_ms,
        outcome="ok" if ar.error is None else ar.outcome_hint, error=ar.error,
        trace_id=trace_id, session_id=session_id)


@app.post("/feedback")
def feedback(req: FeedbackRequest):
    if not req.trace_id:
        raise HTTPException(400, "trace_id required")
    client = get_client()
    try:
        client.create_score(score_id=f"user-thumbs-{req.trace_id}",
                            name="user-thumbs", value=req.value,
                            trace_id=req.trace_id, data_type="BOOLEAN",
                            comment=req.comment)
        client.flush()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"failed to record feedback: {e}")
    return {"ok": True}
