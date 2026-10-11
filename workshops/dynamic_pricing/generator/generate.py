"""Stream deterministic synthetic history. Hidden demand parameters are not features."""
import argparse
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .manifest import sha256_file

VERSION = "retail-v1"
CUTOVER = datetime(2026, 1, 30, tzinfo=UTC)
START = datetime(2025, 11, 1, tzinfo=UTC)
SHARD_ROWS = 100000
FACT_SCHEMA = pa.schema([
    ("event_id", pa.string()), ("original_event_id", pa.string()),
    ("sku", pa.uint32()), ("store_id", pa.uint16()), ("channel", pa.string()),
    ("event_type", pa.string()), ("quantity", pa.int32()),
    ("offered_price_minor", pa.int64()), ("receipts_minor", pa.int64()),
    ("sold_cost_minor", pa.int64()), ("event_time", pa.timestamp("ms", tz="UTC")),
    ("calibration", pa.bool_()), ("currency", pa.string()),
])


def _event_id(seed: int, key: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"history-v1/{seed}/{key}"))


def _fact(event_id, sku, store, price, time, calibration, quantity=1, original=""):
    return {"event_id": event_id, "original_event_id": original, "sku": sku, "store_id": store,
            "channel": "store", "event_type": "sale" if quantity > 0 else "return",
            "quantity": quantity, "offered_price_minor": price,
            "receipts_minor": quantity * price, "sold_cost_minor": quantity * (400 if sku == 101 else 500),
            "event_time": time, "calibration": calibration, "currency": "SGD"}


def _write(output: Path, name: str, rows: list[dict], schema=None, facts=False) -> dict:
    path = output / name
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path, compression="zstd", row_group_size=10000)
    result = {"key": name, "sha256": sha256_file(path), "rows": table.num_rows, "bytes": path.stat().st_size}
    if facts:
        for field in ("quantity", "receipts_minor", "sold_cost_minor"):
            result[field] = sum(row[field] for row in rows)
    return result


def _calibration(seed: int, tier: str):
    rng = np.random.default_rng(seed)
    days, skus, stores = (3, [101], [1]) if tier == "test" else (90, [101, 102], [1, 2])
    intervals, events = [], []
    for day in range(days):
        for sku in skus:
            for store in stores:
                reference = 800 if sku == 101 else 1000
                for hour in range(10, 18):
                    time = START + timedelta(days=day, hours=hour)
                    lower = ((int(reference * .65) + 49) // 50) * 50
                    price = int(rng.choice(np.arange(lower, reference + 1, 50)))
                    # Known synthetic generator, independent of offline estimator code.
                    baseline = .25 if tier == "test" else (4 if sku == 101 else 6)
                    baseline *= (1 + (store - 1) * .1) * (1 + (hour - 13) * .025)
                    baseline *= 1.15 if time.weekday() >= 5 else 1
                    mean = baseline * (price / reference) ** -1.8
                    count = int(rng.poisson(mean))
                    intervals.append({"sku": sku, "store_id": store, "hour": time,
                                      "available_units": 10000, "open": True, "exposure_hours": 1.0,
                                      "offered_price_minor": price, "reference_price_minor": reference,
                                      "unit_cost_minor": reference // 2, "calibration": True})
                    for sale in range(count):
                        key = f"calibration/{day}/{sku}/{store}/{hour}/{sale}"
                        events.append(_fact(_event_id(seed, key), sku, store, price,
                                            time + timedelta(seconds=int(rng.integers(1, 3600))), True))
                        if len(events) == SHARD_ROWS:
                            yield "events", events
                            events = []
    if events:
        yield "events", events
    yield "intervals", intervals


def generate_dataset(output: Path, rows: int, seed: int, tier: str) -> Path:
    if tier not in ("test", "core", "showcase") or type(rows) is not int or rows <= 0:
        raise ValueError("Choose a known tier and positive integer row count.")
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        raise ValueError("Choose an empty output directory for a new immutable dataset.")
    files, companions, calibration_count = [], [], 0
    for kind, batch in _calibration(seed, tier):
        if kind == "events":
            files.append(_write(output, f"calibration-{len(files):05}.parquet", batch, FACT_SCHEMA, True))
            calibration_count += len(batch)
        else:
            companions.append(_write(output, "availability.parquet", batch))
    if rows < calibration_count:
        raise ValueError(f"Requested rows cannot contain the complete calibration cohort ({calibration_count}).")
    products = [{"sku": s, "category": "grocery", "reference_price_minor": p, "unit_cost_minor": p // 2,
                 "selling_cost_minor": 20, "disposal_cost_minor": 20, "currency": "SGD"}
                for s, p in [(101, 800), (102, 1000)]]
    companions.append(_write(output, "products.parquet", products))
    rng = np.random.default_rng(seed + 10000)
    buffer, previous = [], None
    for i in range(rows - calibration_count):
        sku, store = (101 if i % 2 == 0 else 102), (1 if i % 4 < 2 else 2)
        price = 800 if sku == 101 else 1000
        time = START + timedelta(seconds=int(rng.integers(0, 90 * 86400)))
        if i % 20 == 19 and previous:
            fact = _fact(_event_id(seed, f"operational/{i}"), previous["sku"], previous["store_id"],
                         previous["offered_price_minor"], previous["event_time"] + timedelta(milliseconds=1),
                         False, -1, previous["event_id"])
        else:
            fact = _fact(_event_id(seed, f"operational/{i}"), sku, store, price, time, False)
        previous = fact
        buffer.append(fact)
        if len(buffer) == SHARD_ROWS:
            files.append(_write(output, f"operational-{len(files):05}.parquet", buffer, FACT_SCHEMA, True))
            buffer = []
    if buffer:
        files.append(_write(output, f"operational-{len(files):05}.parquet", buffer, FACT_SCHEMA, True))
    manifest = {"dataset_version": VERSION, "generator_version": "1.0", "tier": tier, "seed": seed,
                "history_cutover": CUTOVER.isoformat(), "files": files, "companion_files": companions,
                "currency": "SGD", "source_namespace": "history-v1", "rows": rows,
                "calibration_rows": calibration_count,
                "schema": str(FACT_SCHEMA), "synthetic": True}
    path = output / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return path


def main():
    parser = argparse.ArgumentParser(description="Generate manifested synthetic retail Parquet.")
    parser.add_argument("--tier", choices=["test", "core", "showcase"], required=True)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(generate_dataset(args.output, args.rows, args.seed, args.tier))


if __name__ == "__main__":
    main()
