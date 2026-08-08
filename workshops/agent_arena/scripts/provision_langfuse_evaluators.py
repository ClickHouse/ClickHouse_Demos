"""Provision the AgentArena Langfuse evaluators through the public API.

Requires the normal learner `.env`, including a working OpenRouter inference key.
The operation is idempotent: the LLM connection is upserted, and evaluator/rule
resources are created only when their AgentArena names do not already exist.
"""
import os
import re

from dotenv import load_dotenv

from scripts.langfuse_admin import LangfuseAdmin

DATASET_NAME = "arena-golden"
CONNECTION = "agent-arena-openrouter"
EVALUATOR = "llm_judge"
RULE = "agent-arena-llm-judge"
JUDGE_MODEL = "openai/gpt-5.6-luna"


def judge_prompt(path="eval/langfuse_evaluators/llm_judge_prompt.md") -> str:
    with open(path) as handle:
        blocks = re.findall(r"```\n(.*?)\n```", handle.read(), re.DOTALL)
    if len(blocks) < 2:
        raise ValueError(f"expected system and user prompt code blocks in {path}")
    return blocks[0].strip() + "\n\n" + blocks[1].strip()


def ensure_golden_dataset() -> None:
    """Create/update arena-golden before rules need to filter on its ID."""
    from arena.config import load_config
    from agents.chclient import ROClickHouseClient
    from eval.golden import load_golden
    from eval.langfuse_adapter import LangfuseTracer
    from eval.serialize import golden_payload

    cfg = load_config()
    ro = ROClickHouseClient(cfg.clickhouse)
    questions = [question for question in load_golden() if not question.fewshot_holdout]
    items = []
    for question in questions:
        result = ro.query(question.golden_sql)
        items.append({
            "id": question.id, "question": question.question,
            "expected_output": golden_payload(
                golden_sql=question.golden_sql, rows=result.rows, cols=result.cols,
                ordered=question.ordered),
            "metadata": {"tier": question.tier, "ordered": question.ordered,
                         "float_dp": cfg.eval.float_dp},
        })
    tracer = LangfuseTracer(cfg.langfuse)
    tracer.ensure_dataset(items)
    tracer.flush()


def main() -> None:
    load_dotenv()
    required = ["LANGFUSE_BASE_URL", "LANGFUSE_PUBLIC_KEY", "LANGFUSE_SECRET_KEY",
                "OPENROUTER_API_KEY", "OPENROUTER_BASE_URL"]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise SystemExit("missing required environment variables: " + ", ".join(missing))
    api = LangfuseAdmin(os.environ["LANGFUSE_BASE_URL"], os.environ["LANGFUSE_PUBLIC_KEY"],
                        os.environ["LANGFUSE_SECRET_KEY"])
    ensure_golden_dataset()
    api.call("PUT", "/api/public/llm-connections", {
        "provider": CONNECTION, "adapter": "openai",
        "secretKey": os.environ["OPENROUTER_API_KEY"],
        "baseURL": os.environ["OPENROUTER_BASE_URL"],
        "customModels": [JUDGE_MODEL], "withDefaultModels": False,
    })

    evaluators = api.call("GET", "/api/public/unstable/evaluators").get("data", [])
    evaluator = next((row for row in evaluators
                      if row.get("name") == EVALUATOR and row.get("scope") == "project"), None)
    if evaluator is None:
        evaluator = api.call("POST", "/api/public/unstable/evaluators", {
            "type": "llm_as_judge", "name": EVALUATOR, "prompt": judge_prompt(),
            "outputDefinition": {
                "dataType": "NUMERIC",
                "score": {"description": "Numeric SQL quality score from 0 to 1."},
                "reasoning": {"description": "One concise reason for the score."},
            },
            "modelConfig": {"provider": CONNECTION, "model": JUDGE_MODEL},
        })

    datasets = api.call("GET", "/api/public/v2/datasets?limit=100").get("data", [])
    dataset = next((row for row in datasets if row.get("name") == DATASET_NAME), None)
    if dataset is None:
        raise SystemExit(f"dataset {DATASET_NAME!r} was not visible after provisioning")
    rules = api.call("GET", "/api/public/unstable/evaluation-rules").get("data", [])
    rule = next((row for row in rules if row.get("name") == RULE), None)
    body = {
        "name": RULE,
        "evaluator": {"name": EVALUATOR, "scope": "project", "type": "llm_as_judge"},
        "target": "experiment", "enabled": True, "sampling": 1,
        "filter": [{"column": "datasetId", "operator": "any of",
                    "value": [dataset["id"]], "type": "stringOptions"}],
        "mapping": [
            {"variable": "question", "source": "input", "jsonPath": "$.question"},
            {"variable": "generated_sql", "source": "output", "jsonPath": "$.sql"},
            {"variable": "golden_sql", "source": "expected_output",
             "jsonPath": "$.golden_sql"},
        ],
    }
    if rule is None:
        rule = api.call("POST", "/api/public/unstable/evaluation-rules", body)
    else:
        update = {key: value for key, value in body.items() if key != "evaluator"}
        rule = api.call("PATCH", f"/api/public/unstable/evaluation-rules/{rule['id']}", update)
    print(f"OK: {EVALUATOR} evaluator rule is {rule.get('status', 'configured')}")


if __name__ == "__main__":
    main()
