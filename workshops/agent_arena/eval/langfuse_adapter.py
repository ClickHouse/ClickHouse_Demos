"""All LangFuse SDK calls live here so a version bump touches one file.

Targets the **OTEL-based Python SDK v4** (langfuse==4.7.1) — required so LangFuse's
server-side evaluators (the correctness Code Evaluator + the llm_judge
LLM-as-a-judge) actually run on our data. (SDK v2 used the legacy ingestion path
and produced no OTEL data, so evaluators saw nothing.)

How the pieces fit:
  - Datasets/Experiments: the golden set is a Dataset; each model×prompt config is
    run via `dataset.run_experiment(...)`, which creates a Dataset Run that the
    UI-configured evaluators score automatically.
  - The agent's RESULT SET is returned from the experiment task, so it becomes the
    run item's observation OUTPUT — what the correctness Code Evaluator reads and
    compares against the dataset item's expected_output (the golden result set).
  - Sessions: the task sets session_id = run_id__config_id so a session is one
    config's whole pass over the golden set.
"""
import base64
import json
import re
import urllib.request
from urllib.parse import quote, urlencode
from langfuse import Langfuse, get_client, propagate_attributes
from arena.config import LangfuseCfg

DATASET_NAME = "arena-golden"


def _str_meta(d: dict) -> dict:
    """propagate_attributes requires dict[str,str] with alphanumeric keys and
    values <=200 chars; coerce/sanitize so nothing is silently dropped."""
    out = {}
    for k, v in (d or {}).items():
        key = re.sub(r"[^A-Za-z0-9]", "", str(k))
        if key:
            out[key] = str(v)[:200]
    return out


def emit_agent_trace(*, trace_name, session_id, tags, metadata, question, model,
                     transcript, sql, output_payload, usage) -> str:
    """Record the current agent turn on LangFuse using the v4 API and return the
    trace_id ('' on failure). Sets trace name/session/tags/metadata via
    propagate_attributes, adds a child 'llm_call' generation with token usage,
    and sets the root observation input/output so the trace derives them (the
    llm_judge evaluator reads $.question / $.sql off the trace). Best-effort:
    never raises. Caller must have an active root observation in context."""
    try:
        client = get_client()
        with propagate_attributes(trace_name=trace_name, session_id=session_id,
                                  tags=tags, metadata=_str_meta(metadata)):
            with client.start_as_current_observation(
                    name="llm_call", as_type="generation", model=model,
                    input=transcript, output=sql or "",
                    usage_details={"input": usage.input_tokens,
                                   "output": usage.output_tokens}):
                pass
            client.update_current_span(input={"question": question},
                                       output=output_payload)
            return client.get_current_trace_id() or ""
    except Exception:  # noqa: BLE001 - tracing is best-effort
        return ""


def _project_base(cfg: LangfuseCfg) -> str:
    """Return '{host}/project/{projectId}' for building trace/session deep links."""
    try:
        auth = base64.b64encode(f"{cfg.public_key}:{cfg.secret_key}".encode()).decode()
        req = urllib.request.Request(f"{cfg.host}/api/public/projects",
                                     headers={"Authorization": f"Basic {auth}"})
        with urllib.request.urlopen(req, timeout=10) as r:
            pid = json.loads(r.read())["data"][0]["id"]
        return f"{cfg.host}/project/{pid}"
    except Exception:  # noqa: BLE001
        return cfg.host  # fall back to host root


class LangfuseTracer:
    def __init__(self, cfg: LangfuseCfg):
        # v4: instantiating Langfuse() configures the global OTEL client that
        # get_client() returns elsewhere (e.g. inside the experiment task).
        self._client = Langfuse(public_key=cfg.public_key,
                                secret_key=cfg.secret_key, host=cfg.host)
        self._base = _project_base(cfg)
        self._host = cfg.host
        self._auth = base64.b64encode(
            f"{cfg.public_key}:{cfg.secret_key}".encode()).decode()

    # --- Datasets / Experiments ------------------------------------------------
    def ensure_dataset(self, items: list[dict]) -> None:
        """items: [{id, question, expected_output, metadata}]. Idempotent.

        expected_output carries the GOLDEN result set (the correctness code
        evaluator compares against it); metadata carries grading params
        (ordered, float_dp, golden_sql, tier, question)."""
        try:
            self._client.create_dataset(name=DATASET_NAME,
                                        description="AgentArena golden questions")
        except Exception:  # noqa: BLE001 - already exists
            pass
        for it in items:
            self._client.create_dataset_item(
                dataset_name=DATASET_NAME, id=it["id"],
                input={"question": it["question"]},
                expected_output=it["expected_output"],
                metadata=it["metadata"])

    def run_experiment(self, *, config_id: str, description: str, task,
                       max_concurrency: int = 1):
        """Run one model×prompt config as a Dataset Run (Experiment). `task` is
        `def task(*, item, **kwargs) -> dict` returning the agent result payload
        (becomes the observation output the evaluators read). Returns the SDK
        experiment result. Server-side evaluators (correctness + llm_judge)
        configured for Experiments on `arena-golden` run automatically."""
        ds = self._client.get_dataset(DATASET_NAME)
        return ds.run_experiment(name=config_id, run_name=config_id,
                                 description=description, task=task,
                                 max_concurrency=max_concurrency)

    def trace_url(self, trace_id: str) -> str:
        return f"{self._base}/traces/{trace_id}"

    def session_url(self, session_id: str) -> str:
        return f"{self._base}/sessions/{quote(session_id, safe='')}"

    # --- Score readiness -------------------------------------------------------
    def fetch_trace_scores(self, trace_id: str) -> list:
        """Read evaluator scores with the current public Scores API v3.
        Returns a list of {name, value, string, dataType} — the caller classifies
        by name/type, since evaluator score names are user-defined."""
        try:
            query = urlencode({"traceId": trace_id, "fields": "subject", "limit": 100})
            req = urllib.request.Request(
                f"{self._host}/api/public/v3/scores?{query}",
                headers={"Authorization": f"Basic {self._auth}"})
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read())
        except Exception:  # noqa: BLE001
            return []
        scores = []
        for score in data.get("data", []) or []:
            value = score.get("value")
            categorical = score.get("dataType") in {"CATEGORICAL", "TEXT", "CORRECTION"}
            scores.append({
                "name": score.get("name") or "",
                "value": None if categorical else value,
                "string": value if categorical else None,
                "dataType": score.get("dataType"),
            })
        return scores

    def flush(self) -> None:
        self._client.flush()
