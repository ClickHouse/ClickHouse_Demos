from dataclasses import dataclass, field
import yaml


@dataclass
class GoldenQuestion:
    id: str
    tier: int
    question: str
    ordered: bool
    golden_sql: str
    tags: list = field(default_factory=list)
    notes: str = ""
    fewshot_holdout: bool = False


def load_golden(path: str = "golden/questions.yaml") -> list[GoldenQuestion]:
    with open(path) as f:
        raw = yaml.safe_load(f)
    return [GoldenQuestion(
        id=q["id"], tier=q["tier"], question=q["question"],
        ordered=q.get("ordered", False), golden_sql=q["golden_sql"],
        tags=q.get("tags", []), notes=q.get("notes", ""),
        fewshot_holdout=q.get("fewshot_holdout", False),
    ) for q in raw]


def fewshot_examples(questions: list[GoldenQuestion], k: int) -> list[dict]:
    held = [q for q in questions if q.fewshot_holdout][:k]
    return [{"question": q.question, "golden_sql": q.golden_sql} for q in held]
