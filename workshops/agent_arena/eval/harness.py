"""Grid runner: for each (model x prompt) config, run every golden question,
grade it with Langfuse evaluators, and keep Langfuse as the result store.

Usage: source .env && python -m eval.harness [--run-id RID]
"""
import argparse
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from arena.config import load_config
from agents.chclient import ROClickHouseClient
from agents.llm import OpenRouterClient, cost_usd
from agents.loop import run_agent
from agents.policy import load_policy, with_policy
from eval.golden import GoldenQuestion, load_golden, fewshot_examples
from eval.langfuse_adapter import LangfuseTracer, emit_agent_trace
from eval.serialize import result_payload, golden_payload


DEFAULT_REQUIRED_SCORE_NAMES = (
    "correctness",
    "agent-arena-llm-judge",
)


def parse_args(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--eval-timeout", type=int, default=180,
                    help="max seconds to wait for Langfuse evaluator scores")
    ap.add_argument("--eval-poll", type=float, default=3.0,
                    help="seconds between Langfuse score polls")
    ap.add_argument("--models", default="", help="CSV of model names to override the config grid")
    ap.add_argument("--prompts", default="", help="CSV of prompt names to override the config grid")
    ap.add_argument("--models-file", default="",
                    help="JSON file of model specs [{id,name,family,price_per_1m_in,price_per_1m_out}] "
                         "to run instead of config.yaml models (used by the web UI)")
    ap.add_argument("--policy-version", default="policy-v2")
    ap.add_argument("--wait-for-score", action="append", default=[])
    return ap.parse_args(argv)


def effective_run_id(run_id: str, policy_version: str) -> str:
    return f"{run_id}--{policy_version}"


def _dataset_item(q: GoldenQuestion, *, rows, cols, default_float_dp: int) -> dict:
    float_dp = q.float_dp if q.float_dp is not None else default_float_dp
    return {
        "id": q.id,
        "question": q.question,
        "expected_output": golden_payload(
            golden_sql=q.golden_sql,
            rows=rows,
            cols=cols,
            ordered=q.ordered,
        ),
        "metadata": {
            "tier": q.tier,
            "ordered": q.ordered,
            "float_dp": float_dp,
        },
    }


def main(argv=None) -> None:
    args = parse_args(argv)

    cfg = load_config()
    release = args.run_id or f"run-{uuid.uuid4().hex[:8]}"
    policy = load_policy(args.policy_version)
    run_id = effective_run_id(release, policy.version)

    # The web UI can hand the harness an arbitrary set of models (from the live
    # OpenRouter catalog) via a JSON file, overriding config.yaml's model list.
    if args.models_file:
        import json
        from arena.config import ModelCfg
        with open(args.models_file) as f:
            cfg.models = [ModelCfg(**m) for m in json.load(f)]

    tracer = LangfuseTracer(cfg.langfuse)

    # Refresh per-model prices from OpenRouter's live catalog so cost-per-correct
    # reflects current prices; fall back to config.yaml prices if unreachable.
    try:
        from agents.llm import fetch_openrouter_prices, apply_live_prices
        _prices = fetch_openrouter_prices(cfg.openrouter.base_url)
        cfg.models = apply_live_prices(cfg.models, _prices)
    except Exception as e:  # noqa: BLE001 - pricing refresh is best-effort
        print(f"[pricing] live refresh failed ({e}); using config.yaml prices")

    ro = ROClickHouseClient(cfg.clickhouse)
    llm = OpenRouterClient(cfg.openrouter.base_url, cfg.openrouter.api_key)

    with open("schema/schema_context.md") as f:
        schema_ctx = with_policy(f.read(), policy)

    all_questions = load_golden()
    # Hold out the few-shot example questions so P2 is not graded on its own
    # examples; every config is scored on the SAME evaluation set for fairness.
    eval_questions = [q for q in all_questions if not q.fewshot_holdout]
    questions = eval_questions

    # Snapshot golden results once per run for deterministic evaluator inputs.
    golden_cache = {}
    for q in questions:
        gr = ro.query(q.golden_sql)
        golden_cache[q.id] = (gr.rows, gr.cols)

    # Upload the golden set as a Langfuse Dataset (each config becomes a Run).
    tracer.ensure_dataset([
        _dataset_item(
            q,
            rows=golden_cache[q.id][0],
            cols=golden_cache[q.id][1],
            default_float_dp=cfg.eval.float_dp,
        )
        for q in questions
    ])

    model_names, prompt_names = cfg.resolved_grid()
    if args.models:
        model_names = [m for m in model_names if m in set(args.models.split(","))]
    if args.prompts:
        prompt_names = [p for p in prompt_names if p in set(args.prompts.split(","))]
    print(f"run_id={run_id} configs={len(model_names)}x{len(prompt_names)} "
          f"questions={len(questions)} grade=langfuse", flush=True)

    qmap = {q.id: q for q in questions}
    pending_trace_ids = []
    task_failures = []

    def run_one(mname, pname, mcfg, pcfg, examples, config_id, q):
        """Run the agent for one (config, question). Returns (payload, cost, latency, ar)."""
        t0 = time.time()
        ar = run_agent(q.question, mcfg, pcfg, schema_ctx, ro, llm,
                       dict(cfg.openrouter.inference), examples=examples,
                       max_retries=cfg.eval.default_max_retries)
        latency_ms = int((time.time() - t0) * 1000)
        c = cost_usd(ar.usage, mcfg.price_per_1m_in, mcfg.price_per_1m_out)
        payload = result_payload(sql=ar.sql, rows=ar.rows, cols=ar.cols,
                                 error=ar.error, outcome_hint=ar.outcome_hint,
                                 transcript=ar.transcript, cost_usd=c,
                                 latency_ms=latency_ms, retries=ar.attempts - 1,
                                 tier=q.tier)
        return payload, c, latency_ms, ar

    for mname in model_names:
        mcfg = cfg.model_by_name(mname)
        for pname in prompt_names:
            pcfg = cfg.prompt_by_name(pname)
            config_id = f"{mname}__{pname}"
            examples = fewshot_examples(all_questions, pcfg.k) if pcfg.k else None
            session_id = f"{run_id}__{config_id}"

            # Each config is a Dataset Run. The task output is the complete
            # leaderboard record; Langfuse evaluators attach correctness/outcome.
            def task(*, item, _m=mname, _p=pname, _mc=mcfg, _pc=pcfg,
                     _ex=examples, _cid=config_id, _sid=session_id, **kwargs):
                item_id = str(_item_field(item, "id") or "unknown")
                try:
                    q = _question_for_item(item, qmap)
                    payload, c, latency_ms, ar = run_one(_m, _p, _mc, _pc, _ex, _cid, q)
                    trace_id = emit_agent_trace(
                        trace_name="agent_run", session_id=_sid,
                        tags=[_cid, f"run:{run_id}", policy.version, _m, _p],
                        metadata={"config_id": _cid, "question_id": q.id,
                                  "model": _m, "prompt": _p, "run_id": run_id,
                                  "policy_version": policy.version,
                                  "release": release},
                        question=q.question, model=_m,
                        transcript=ar.transcript, sql=ar.sql,
                        output_payload=payload, usage=ar.usage)
                    if not trace_id:
                        raise RuntimeError(
                            "Langfuse trace creation failed; benchmark result was not stored")
                    pending_trace_ids.append(trace_id)
                    print(f"  {_cid} {q.id} pending {latency_ms}ms ${c:.5f}", flush=True)
                    return payload
                except Exception as exc:
                    task_failures.append(f"{_cid}/{item_id}: {exc}")
                    raise
            tracer.run_experiment(config_id=f"{run_id}__{config_id}",
                                  description=f"AgentArena run {run_id}; config {config_id}", task=task)
            if task_failures:
                raise RuntimeError("experiment item failures: " + "; ".join(task_failures))
    tracer.flush()

    if pending_trace_ids:
        _wait_for_scores(
            tracer,
            pending_trace_ids,
            args.eval_timeout,
            args.eval_poll,
            args.wait_for_score,
        )
    print(f"done. run_id={run_id}", flush=True)


def _classify_scores(scores):
    """Map a trace's evaluator scores → (correctness, judge, outcome), tolerant of
    user-chosen evaluator score names. A numeric score whose name hints judge
    (judge/llm) → judge; numeric hinting correctness (correct/corect) → correctness;
    any categorical/string score → outcome."""
    corr = judge = outcome = None
    for s in scores or []:
        nm = (s.get("name") or "").lower()
        val, sval = s.get("value"), s.get("string")
        if sval is not None and val is None:
            outcome = sval                      # categorical (the code evaluator's `outcome`)
        elif val is None:
            continue
        elif "judge" in nm or "llm" in nm:
            judge = val
        elif "correct" in nm or "corect" in nm:
            corr = val
    return corr, judge, outcome


def _item_field(item, name, default=None):
    return item.get(name, default) if isinstance(item, dict) else getattr(item, name, default)


def _question_for_item(item, qmap) -> GoldenQuestion:
    """Resolve local questions and learner-promoted Langfuse dataset items."""
    item_id = str(_item_field(item, "id") or "")
    if item_id in qmap:
        return qmap[item_id]
    inp = _item_field(item, "input", {}) or {}
    metadata = _item_field(item, "metadata", {}) or {}
    expected = _item_field(item, "expected_output", {}) or {}
    question = inp.get("question") if isinstance(inp, dict) else None
    golden_sql = (metadata.get("golden_sql") or expected.get("golden_sql"))
    if not item_id or not question or not golden_sql:
        raise ValueError(f"dataset item {item_id or '<unknown>'} lacks promoted-question metadata")
    return GoldenQuestion(id=item_id, tier=int(metadata.get("tier", 3)),
                          question=str(question), ordered=bool(metadata.get("ordered", False)),
                          golden_sql=str(golden_sql))


def _wait_for_scores(tracer, trace_ids, timeout, poll, required_names=None) -> None:
    """Compatibility wrapper for the two default asynchronous evaluators."""
    _wait_for_required_scores(
        tracer, trace_ids, timeout, poll, required_names=required_names
    )


def _wait_for_required_scores(
    tracer, trace_ids, timeout, poll, required_names=None
) -> None:
    """Wait for the default scores and every additional exact score name."""
    required = list(dict.fromkeys([
        *DEFAULT_REQUIRED_SCORE_NAMES,
        *(required_names or []),
    ]))
    remaining = set(trace_ids)
    print(f"grading via LangFuse evaluators — waiting on {len(remaining)} traces "
          f"(timeout {timeout}s)…", flush=True)
    deadline = time.time() + timeout
    last_missing = {tid: set(required) for tid in remaining}
    while remaining and time.time() < deadline:
        tids = list(remaining)
        with ThreadPoolExecutor(max_workers=min(8, len(tids))) as pool:
            score_lists = list(pool.map(tracer.fetch_trace_scores, tids))
        done = []
        for tid, scores in zip(tids, score_lists):
            available = {
                score.get("name")
                for score in scores or []
                if score.get("name")
                and (score.get("value") is not None or score.get("string") is not None)
            }
            missing = set(required) - available
            last_missing[tid] = missing
            if not missing:
                done.append(tid)
        for tid in done:
            remaining.discard(tid)
        if remaining:
            print(f"  {len(remaining)} traces still pending", flush=True)
            time.sleep(poll)
    if remaining:
        missing_names = sorted({name for tid in remaining for name in last_missing[tid]})
        raise RuntimeError(
            f"{len(remaining)} traces are missing {', '.join(missing_names)} after {timeout}s; "
            "configure the required Langfuse evaluators for Experiments on arena-golden. "
            "See eval/langfuse_evaluators/README.md."
        )
    print(f"Langfuse scored all {len(trace_ids)} traces; leaderboard ready", flush=True)


if __name__ == "__main__":
    main()
