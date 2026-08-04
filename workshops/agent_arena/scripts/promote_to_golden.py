"""Human-in-the-loop: promote human-approved production questions into the
`arena-golden` dataset so the next benchmark run measures them.

Workflow: a reviewer triages production chat traces in the LangFuse annotation
queue (see the workshop guide), then exports the approved ones as a JSON list of
{id, question, golden_sql, tier, ordered}. This script snapshots each query's
golden result set against ClickHouse and adds it as an arena-golden dataset item.

Usage: source .env && python -m scripts.promote_to_golden reviewed.json
"""
import json
import sys
from arena.config import load_config
from agents.chclient import ROClickHouseClient
from eval.langfuse_adapter import LangfuseTracer
from eval.serialize import golden_payload


def build_dataset_item(*, qid, question, golden_sql, tier, ordered, rows, cols,
                       float_dp) -> dict:
    """Assemble an arena-golden dataset item (same shape the harness uploads)."""
    return {
        "id": qid,
        "question": question,
        "expected_output": golden_payload(golden_sql=golden_sql, rows=rows,
                                           cols=cols, ordered=ordered),
        "metadata": {"ordered": bool(ordered), "float_dp": float_dp,
                     "golden_sql": golden_sql, "tier": tier, "question": question},
    }


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: python -m scripts.promote_to_golden reviewed.json")
        raise SystemExit(2)
    cfg = load_config()
    ro = ROClickHouseClient(cfg.clickhouse)
    tracer = LangfuseTracer(cfg.langfuse)
    with open(sys.argv[1]) as f:
        reviewed = json.load(f)

    items = []
    for rec in reviewed:
        qr = ro.query(rec["golden_sql"])   # snapshot the golden result set
        items.append(build_dataset_item(
            qid=rec["id"], question=rec["question"], golden_sql=rec["golden_sql"],
            tier=rec.get("tier", 3), ordered=rec.get("ordered", False),
            rows=qr.rows, cols=qr.cols, float_dp=cfg.eval.float_dp))
        print(f"  prepared {rec['id']}: {len(qr.rows)} golden row(s)")

    tracer.ensure_dataset(items)
    tracer.flush()
    print(f"promoted {len(items)} question(s) into the '{tracer and 'arena-golden'}' dataset — "
          f"re-run `python -m eval.harness` to measure them")


if __name__ == "__main__":
    main()
