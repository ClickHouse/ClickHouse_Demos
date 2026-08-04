"""LLM-as-a-judge: an OpenRouter-scored quality rating (0..1) for the generated SQL,
independent of execution accuracy. Pushed to LangFuse as the 'llm_judge' score so
it shows up as a metric that exists because LangFuse is the eval store.
"""
import re

JUDGE_MODEL = "openai/gpt-5.6-luna"  # reliable, cheap-ish judge
_SYS = (
    "You are a strict SQL reviewer for a ClickHouse analytics database. Given a "
    "natural-language question and a candidate SQL query, rate how well the SQL "
    "answers the question on an integer scale 0-10 (10 = perfect, correct and "
    "idiomatic; 0 = wrong or empty). Reply with ONLY the integer."
)


def judge_sql(llm, question: str, sql: str | None,
              model_id: str = JUDGE_MODEL) -> float:
    if not sql:
        return 0.0
    messages = [{"role": "user", "content": [{"text":
        f"Question: {question}\n\nSQL:\n{sql}\n\nScore (0-10):"}]}]
    try:
        res = llm.converse(model_id, _SYS, messages,
                           {"temperature": 0.0, "max_tokens": 4})
        m = re.search(r"\d+", res.text)
        return min(1.0, int(m.group()) / 10.0) if m else 0.0
    except Exception:  # noqa: BLE001
        return 0.0
