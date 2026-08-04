"""Read-model helpers for AgentArena results stored in Langfuse experiments."""

from datetime import datetime, timedelta, timezone
from collections import defaultdict
from urllib.parse import quote


def _json_object(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        import json
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except (TypeError, ValueError):
            return {}
    return {}


def _score_values(scores):
    correctness = judge = None
    outcome = None
    for score in scores or []:
        name = str(score.get("name") or "").lower()
        value = score.get("value")
        if "judge" in name or "llm" in name:
            judge = value
        elif "correct" in name:
            correctness = value
        elif name == "outcome" and isinstance(value, str):
            outcome = value
    return correctness, judge, outcome


def normalize_experiment_item(item: dict, project_base: str) -> dict | None:
    """Convert one Langfuse Experiment Item into the dashboard's question row."""
    metadata = _json_object(item.get("metadata"))
    output = _json_object(item.get("output"))
    item_metadata = _json_object(item.get("experimentItemMetadata"))
    experiment_name = str(item.get("experimentName") or "")
    inferred_run, separator, inferred_config = experiment_name.partition("__")
    run_id = (metadata.get("runid") or metadata.get("run_id")
              or (inferred_run if separator else None))
    config_id = (metadata.get("configid") or metadata.get("config_id")
                 or (inferred_config if separator else experiment_name))
    question_id = (metadata.get("questionid") or metadata.get("question_id")
                   or item.get("experimentItemId") or metadata.get("dataset_item_id"))
    if not (run_id and config_id and question_id):
        return None

    model_name = metadata.get("model")
    prompt_name = metadata.get("prompt")
    if not model_name or not prompt_name:
        model_name, _, prompt_name = config_id.partition("__")

    correctness, judge, outcome = _score_values(item.get("scores"))
    correctness = int(round(float(correctness))) if correctness is not None else 0
    judge = float(judge) if judge is not None else 0.0
    trace_id = str(item.get("traceId") or "")
    session_id = f"{run_id}__{config_id}"
    project_base = project_base.rstrip("/")
    inp = _json_object(item.get("input"))

    latency_ms = output.get("latency_ms")
    if latency_ms is None and item.get("startTime") and item.get("endTime"):
        start = datetime.fromisoformat(item["startTime"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(item["endTime"].replace("Z", "+00:00"))
        latency_ms = int((end - start).total_seconds() * 1000)

    return {
        "run_id": str(run_id),
        "config_id": str(config_id),
        "model_name": str(model_name),
        "prompt_name": str(prompt_name),
        "question_id": str(question_id),
        "tier": int(output.get("tier", item_metadata.get("tier", 0))),
        "correctness": correctness,
        "judge_score": judge,
        "cost_usd": float(output.get("cost_usd", 0.0)),
        "latency_ms": int(latency_ms or 0),
        "retries": int(output.get("retries", 0)),
        "outcome": outcome or "pending",
        "sql": str(output.get("sql") or ""),
        "trace_id": trace_id,
        "trace_url": f"{project_base}/traces/{quote(trace_id, safe='')}",
        "session_id": session_id,
        "session_url": f"{project_base}/sessions/{quote(session_id, safe='_')}",
        "question": inp.get("question"),
        "transcript": output.get("transcript") if isinstance(output.get("transcript"), list) else [],
        "started_at": item.get("startTime"),
    }


def build_read_model(rows: list[dict]) -> dict:
    """Aggregate normalized question rows into the existing dashboard contract."""
    newest = {}
    for row in rows:
        key = (row["run_id"], row["config_id"], row["question_id"])
        prior = newest.get(key)
        if prior is None or (row.get("started_at") or "") > (prior.get("started_at") or ""):
            newest[key] = row
    rows = list(newest.values())

    run_started = {}
    by_config = defaultdict(list)
    by_tier = defaultdict(list)
    outcome_counts = defaultdict(int)
    questions = defaultdict(list)
    for row in rows:
        rid = row["run_id"]
        run_started[rid] = max(run_started.get(rid, ""), row.get("started_at") or "")
        by_config[(rid, row["config_id"])].append(row)
        by_tier[(rid, row["config_id"], row["tier"])].append(row)
        outcome_counts[(rid, row["config_id"], row["outcome"])] += 1
        questions[(rid, row["config_id"])].append(row)

    leaderboard = defaultdict(list)
    for (rid, config_id), group in by_config.items():
        n_questions = len(group)
        n_correct = sum(int(r["correctness"]) for r in group)
        total_cost = sum(float(r["cost_usd"]) for r in group)
        leaderboard[rid].append({
            "config_id": config_id,
            "model_name": group[0]["model_name"],
            "prompt_name": group[0]["prompt_name"],
            "n_questions": n_questions,
            "accuracy": round(n_correct / n_questions, 4),
            "n_correct": n_correct,
            "avg_judge_score": round(sum(float(r["judge_score"]) for r in group) / n_questions, 3),
            "total_cost_usd": round(total_cost, 6),
            "avg_latency_ms": round(sum(int(r["latency_ms"]) for r in group) / n_questions, 1),
            "cost_per_correct_answer": round(total_cost / n_correct, 6) if n_correct else None,
        })
    for board in leaderboard.values():
        board.sort(key=lambda r: (r["cost_per_correct_answer"] is None,
                                  r["cost_per_correct_answer"] or 0,
                                  -r["accuracy"]))

    tiers = defaultdict(list)
    for (rid, config_id, tier), group in by_tier.items():
        tiers[rid].append({
            "config_id": config_id,
            "tier": tier,
            "accuracy": round(sum(int(r["correctness"]) for r in group) / len(group), 3),
        })
    for values in tiers.values():
        values.sort(key=lambda r: (r["config_id"], r["tier"]))

    outcomes = defaultdict(list)
    for (rid, config_id, outcome), count in outcome_counts.items():
        outcomes[rid].append({"config_id": config_id, "outcome": outcome, "n": count})
    for values in outcomes.values():
        values.sort(key=lambda r: (r["config_id"], r["outcome"]))

    for values in questions.values():
        values.sort(key=lambda r: r["question_id"])

    return {
        "runs": sorted(run_started, key=run_started.get, reverse=True),
        "leaderboard": dict(leaderboard),
        "tiers": dict(tiers),
        "outcomes": dict(outcomes),
        "questions": dict(questions),
    }


def _recent_from_start(days: int = 30) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat().replace("+00:00", "Z")


def fetch_experiment_rows(get_page, project_base: str,
                          from_start: str | None = None) -> list[dict]:
    """Page through Langfuse Experiment Items and normalize AgentArena records."""
    params = {
        "fromStartTime": from_start or _recent_from_start(),
        "fields": "core,dataset,io,metadata,itemMetadata,experimentMetadata,scores",
        "limit": 100,
        "scoreLimit": 50,
    }
    rows = []
    while True:
        page = get_page(params)
        for item in page.get("data", []):
            row = normalize_experiment_item(item, project_base)
            if row is not None:
                rows.append(row)
        cursor = (page.get("meta") or {}).get("cursor")
        if not cursor:
            return rows
        params = {**params, "cursor": cursor}
