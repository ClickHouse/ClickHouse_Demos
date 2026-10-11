"""Verify bytes and declared facts before any dataset becomes canonical."""
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
from pathlib import Path
from typing import Iterator
from uuid import UUID

import pyarrow.parquet as pq
import pyarrow.compute as pc

from pricing_lab.contracts import Scope, Subject


@dataclass(frozen=True)
class SaleFact:
    scope: Scope
    subject: Subject
    event_id: UUID
    event_type: str
    quantity: int
    receipts_minor: int
    sold_cost_minor: int
    offered_price_minor: int
    event_time: datetime
    calibration: bool


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def checked_path(root: Path, key: str) -> Path:
    path = root / key
    if Path(key).is_absolute() or ".." in Path(key).parts or not path.resolve().is_relative_to(root.resolve()):
        raise ValueError("Manifest path must stay inside its dataset directory.")
    return path


def read_manifest(path: Path) -> dict:
    manifest = json.loads(path.read_text())
    required = {"dataset_version", "tier", "seed", "history_cutover", "files", "companion_files", "currency", "source_namespace", "rows"}
    if not required <= manifest.keys() or manifest["dataset_version"] != "retail-v1" or manifest["currency"] != "SGD":
        raise ValueError("Unsupported dataset manifest schema or currency.")
    if manifest["source_namespace"] != "history-v1" or not manifest["files"]:
        raise ValueError("Historical namespace/files missing.")
    keys = set()
    for file in manifest["files"] + manifest["companion_files"]:
        key = file["key"]
        local = checked_path(path.parent, key)
        if key in keys:
            raise ValueError("Duplicate manifest path.")
        keys.add(key)
        if sha256_file(local) != file["sha256"]:
            raise ValueError(f"Dataset checksum mismatch: {key}")
        if local.stat().st_size != file["bytes"] or pq.ParquetFile(local).metadata.num_rows != file["rows"]:
            raise ValueError(f"Dataset count/size mismatch: {key}")
        if file in manifest["files"]:
            fields = ("quantity", "receipts_minor", "sold_cost_minor")
            totals = dict.fromkeys(fields, 0)
            for batch in pq.ParquetFile(local).iter_batches(batch_size=10000, columns=list(fields)):
                for field in fields:
                    totals[field] += pc.sum(batch[field]).as_py() or 0
            if any(totals[field] != file[field] for field in fields):
                raise ValueError(f"Dataset monetary/quantity totals mismatch: {key}")
    if sum(f["rows"] for f in manifest["files"]) != manifest["rows"]:
        raise ValueError("Manifest row totals do not reconcile.")
    return manifest


def iter_manifest_rows(path: Path) -> Iterator[SaleFact]:
    manifest = read_manifest(path)
    scope = Scope(owner_id="history-v1", scenario_id="grocery", generation=1)
    for file in manifest["files"]:
        for batch in pq.ParquetFile(path.parent / file["key"]).iter_batches(batch_size=10000):
            for row in batch.to_pylist():
                yield SaleFact(scope=scope, subject=Subject(sku=row["sku"], store_id=row["store_id"]),
                               event_id=UUID(row["event_id"]), event_type=row["event_type"],
                               quantity=row["quantity"], receipts_minor=row["receipts_minor"],
                               sold_cost_minor=row["sold_cost_minor"], offered_price_minor=row["offered_price_minor"],
                               event_time=row["event_time"], calibration=row["calibration"])
