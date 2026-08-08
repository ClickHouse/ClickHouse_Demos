"""Human-in-the-loop: promote human-approved production questions into the
`arena-golden` dataset so the next benchmark run measures them.

Workflow: a reviewer triages production chat traces in the Langfuse annotation
queue (see the workshop guide), then exports the approved ones as a JSON list of
{id, question, golden_sql, tier, ordered}. This script snapshots each query's
golden result set against ClickHouse and adds it as an arena-golden dataset item.

Usage: source .env && .venv/bin/python -m scripts.promote_to_golden reviewed.json
"""
import json
import sys
from arena.config import load_config
from agents.chclient import ROClickHouseClient
from eval.langfuse_adapter import LangfuseTracer
from eval.serialize import golden_payload


PROVENANCE_KEYS = (
    "source",
    "source_trace_id",
    "failure_category",
    "source_policy_version",
    "annotation_id",
)
PRODUCTION_PROVENANCE_KEYS = PROVENANCE_KEYS[:4]


def production_provenance(record: dict) -> dict[str, str]:
    """Return the allowlisted review provenance for a promoted record."""
    provenance = {key: record[key] for key in PROVENANCE_KEYS if key in record}
    if provenance.get("source") == "production-feedback":
        missing = [key for key in PRODUCTION_PROVENANCE_KEYS if not provenance.get(key)]
        if missing:
            raise ValueError(
                "production-feedback provenance requires " + ", ".join(missing)
            )
    return provenance


def build_dataset_item(*, qid, question, golden_sql, tier, ordered, rows, cols,
                       float_dp, provenance: dict | None = None) -> dict:
    """Assemble an arena-golden dataset item (same shape the harness uploads)."""
    metadata = {"ordered": bool(ordered), "float_dp": float_dp,
                "golden_sql": golden_sql, "tier": tier, "question": question}
    metadata.update(production_provenance(provenance or {}))
    return {
        "id": qid,
        "question": question,
        "expected_output": golden_payload(golden_sql=golden_sql, rows=rows,
                                           cols=cols, ordered=ordered),
        "metadata": metadata,
    }


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: .venv/bin/python -m scripts.promote_to_golden reviewed.json")
        raise SystemExit(2)
    cfg = load_config()
    ro = ROClickHouseClient(cfg.clickhouse)
    tracer = LangfuseTracer(cfg.langfuse)
    with open(sys.argv[1]) as f:
        reviewed = json.load(f)

    items = []
    for rec in reviewed:
        provenance = production_provenance(rec)
        qr = ro.query(rec["golden_sql"])   # snapshot the golden result set
        items.append(build_dataset_item(
            qid=rec["id"], question=rec["question"], golden_sql=rec["golden_sql"],
            tier=rec.get("tier", 3), ordered=rec.get("ordered", False),
            rows=qr.rows, cols=qr.cols, float_dp=cfg.eval.float_dp,
            provenance=provenance))
        print(f"  prepared {rec['id']}: {len(qr.rows)} golden row(s)")

    tracer.ensure_dataset(items)
    tracer.flush()
    print(f"promoted {len(items)} question(s) into the '{tracer and 'arena-golden'}' dataset — "
          f"re-run `.venv/bin/python -m eval.harness` to measure them")


if __name__ == "__main__":
    main()
