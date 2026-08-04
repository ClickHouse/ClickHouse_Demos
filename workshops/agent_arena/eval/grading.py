from decimal import Decimal


def normalize(rows, ordered: bool, dp: int = 4):
    out = []
    for r in rows:
        cells = []
        for v in r:
            if v is None:
                cells.append("∅")
            elif isinstance(v, bool):
                cells.append("1" if v else "0")
            elif isinstance(v, (float, Decimal)):
                cells.append(f"{float(v):.{dp}f}")
            else:
                cells.append(str(v))
        out.append(tuple(cells))
    return out if ordered else sorted(out)


def grade(agent, golden_rows, golden_cols, ordered: bool, dp: int = 4) -> int:
    if agent.error or agent.rows is None:
        return 0
    if not agent.rows and not golden_rows:
        return 1
    if agent.rows and golden_rows and len(agent.cols) != len(golden_cols):
        return 0
    return int(normalize(agent.rows, ordered, dp) == normalize(golden_rows, ordered, dp))


def classify_outcome(agent, golden_rows, score: int) -> str:
    if score == 1:
        return "correct"
    if agent.outcome_hint == "model_error":
        return "model_error"
    if agent.outcome_hint == "sql_policy_rejected":
        return "sql_policy_rejected"
    if agent.outcome_hint == "sql_exec_error" or agent.error:
        return "sql_exec_error"
    if agent.rows is not None and len(agent.rows) == 0 and golden_rows:
        return "empty_but_expected"
    return "wrong_result"
