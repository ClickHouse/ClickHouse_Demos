"""Human-in-the-loop: promote human-approved production questions into the
`arena-golden` dataset so the next benchmark run measures them.

Workflow: a reviewer triages production chat traces in the Langfuse annotation
queue (see the workshop guide), then exports the approved ones as a JSON list of
{id, question, golden_sql, tier, ordered}. This script snapshots each query's
golden result set against ClickHouse and adds it as an arena-golden dataset item.

Usage: source .env && .venv/bin/python -m scripts.promote_to_golden reviewed.json
"""
import argparse
import json
from pathlib import Path
import re
import sys
from urllib.parse import urlencode

from arena.config import load_config
from agents.chclient import ROClickHouseClient
from agents.sqlguard import validate_select_only
from eval.langfuse_adapter import LangfuseTracer
from eval.serialize import golden_payload
from scripts.langfuse_admin import LangfuseAdmin, collect_numbered_pages


PROVENANCE_KEYS = (
    "source",
    "source_trace_id",
    "failure_category",
    "source_policy_version",
    "annotation_id",
)
PRODUCTION_PROVENANCE_KEYS = PROVENANCE_KEYS[:4]
DATASET_NAME = "arena-golden"
FIXTURE_PATH = (
    Path(__file__).resolve().parents[1]
    / "tests"
    / "fixtures"
    / "reviewed.production-example.json"
)


def production_provenance(record: dict) -> dict[str, str]:
    """Return the allowlisted review provenance for a promoted record."""
    provenance = {}
    for key in PROVENANCE_KEYS:
        if key not in record:
            continue
        value = record[key]
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"invalid {key}")
        provenance[key] = value.strip()
    if provenance.get("source") == "production-feedback":
        missing = [key for key in PRODUCTION_PROVENANCE_KEYS if key not in provenance]
        if missing:
            raise ValueError(
                "production-feedback provenance requires " + missing[0]
            )
    return provenance


def _safe_record_id(record: object, index: int) -> str:
    if isinstance(record, dict) and isinstance(record.get("id"), str):
        value = record["id"].strip()
        if value and re.fullmatch(r"[A-Za-z0-9._-]{1,80}", value):
            return value
    return f"item-{index}"


def validate_reviewed_records(reviewed: object) -> list[dict]:
    """Validate and normalize a complete review batch without side effects."""
    if not isinstance(reviewed, list) or not reviewed:
        raise ValueError("review batch is invalid")

    normalized = []
    seen_ids = set()
    for index, raw in enumerate(reviewed, start=1):
        record_id = _safe_record_id(raw, index)
        if not isinstance(raw, dict):
            raise ValueError(f"record {record_id}: invalid record")
        if raw.get("id") != record_id:
            raise ValueError(f"record {record_id}: invalid id")
        if record_id in seen_ids:
            raise ValueError(f"record {record_id}: duplicate id")
        seen_ids.add(record_id)

        record = dict(raw)
        for key in ("question", "golden_sql"):
            value = record.get(key)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"record {record_id}: invalid {key}")
            record[key] = value.strip()

        valid_sql, _reason = validate_select_only(record["golden_sql"])
        if not valid_sql:
            raise ValueError(f"record {record_id}: invalid golden_sql")

        try:
            provenance = production_provenance(record)
        except ValueError as exc:
            reason = str(exc)
            invalid_key = next(
                (
                    key
                    for key in sorted(PROVENANCE_KEYS, key=len, reverse=True)
                    if key in reason
                ),
                "provenance",
            )
            raise ValueError(
                f"record {record_id}: invalid {invalid_key}"
            ) from None
        for key in PROVENANCE_KEYS:
            record.pop(key, None)
        record.update(provenance)

        if "tier" in record and (
            type(record["tier"]) is not int or record["tier"] < 1
        ):
            raise ValueError(f"record {record_id}: invalid tier")
        if "ordered" in record and not isinstance(record["ordered"], bool):
            raise ValueError(f"record {record_id}: invalid ordered")
        normalized.append(record)
    return normalized


def _existing_item_metadata(api: LangfuseAdmin, item_ids: set[str]) -> dict[str, dict]:
    limit = 100

    def fetch(page: int) -> dict:
        query = urlencode({
            "datasetName": DATASET_NAME,
            "page": page,
            "limit": limit,
        })
        return api.call("GET", f"/api/public/dataset-items?{query}")

    try:
        rows = collect_numbered_pages(fetch, limit=limit)
    except Exception:  # noqa: BLE001 - never expose endpoint/client diagnostics
        raise RuntimeError("dataset provenance preflight failed") from None

    found = {}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("id"), str):
            raise RuntimeError("dataset provenance preflight failed")
        if row["id"] not in item_ids:
            continue
        item_id = row["id"]
        metadata = row.get("metadata")
        if item_id in found or not isinstance(metadata, dict):
            raise RuntimeError("dataset provenance preflight failed")
        found[item_id] = metadata
    return found


def preflight_provenance(api: LangfuseAdmin, records: list[dict]) -> None:
    """Refuse collisions that could replace genuine production provenance."""
    existing = _existing_item_metadata(api, {record["id"] for record in records})
    for record in records:
        metadata = existing.get(record["id"])
        if metadata is None:
            continue
        try:
            incoming = production_provenance(record)
            current = production_provenance(metadata)
        except ValueError:
            raise RuntimeError("dataset provenance preflight failed") from None
        if (
            current.get("source") == "production-feedback"
            or incoming.get("source") == "production-feedback"
        ) and current != incoming:
            raise RuntimeError(
                f"record {record['id']}: provenance collision"
            ) from None


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


def _review_path(argv: list[str] | None = None) -> Path:
    parser = argparse.ArgumentParser()
    parser.add_argument("reviewed", nargs="?")
    parser.add_argument("--synthetic-fixture", action="store_true")
    args = parser.parse_args(argv)
    if args.synthetic_fixture:
        if args.reviewed is not None:
            parser.error("synthetic fixture cannot be combined with a review file")
        return FIXTURE_PATH
    supplied = Path(args.reviewed or "reviewed.json")
    if supplied.resolve() == FIXTURE_PATH:
        parser.error("tracked fixture requires --synthetic-fixture")
    return supplied


def main(argv: list[str] | None = None) -> None:
    review_path = _review_path(argv)
    try:
        reviewed = json.loads(review_path.read_text())
    except Exception:  # noqa: BLE001 - keep file/parser details out of output
        raise ValueError("review file is invalid") from None
    records = validate_reviewed_records(reviewed)

    cfg = load_config()
    admin = LangfuseAdmin(
        cfg.langfuse.host,
        cfg.langfuse.public_key,
        cfg.langfuse.secret_key,
    )
    preflight_provenance(admin, records)
    ro = ROClickHouseClient(cfg.clickhouse)
    tracer = LangfuseTracer(cfg.langfuse)

    items = []
    for rec in records:
        provenance = production_provenance(rec)
        try:
            qr = ro.query(rec["golden_sql"])
        except Exception:  # noqa: BLE001 - never expose SQL/client diagnostics
            raise RuntimeError(
                f"record {rec['id']}: golden query failed"
            ) from None
        items.append(build_dataset_item(
            qid=rec["id"], question=rec["question"], golden_sql=rec["golden_sql"],
            tier=rec.get("tier", 3), ordered=rec.get("ordered", False),
            rows=qr.rows, cols=qr.cols, float_dp=cfg.eval.float_dp,
            provenance=provenance))
        print(f"  prepared {rec['id']}: {len(qr.rows)} golden row(s)")

    try:
        tracer.ensure_dataset(items)
        tracer.flush()
    except Exception:  # noqa: BLE001 - never expose endpoint/client diagnostics
        raise RuntimeError("dataset promotion failed") from None
    print(f"promoted {len(items)} question(s) into the '{DATASET_NAME}' dataset — "
          f"re-run `.venv/bin/python -m eval.harness` to measure them")


if __name__ == "__main__":
    main()
