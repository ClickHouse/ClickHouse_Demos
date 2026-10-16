"""Generate bounded Parquet shards for a synthetic convenience-store chain."""
import argparse
from datetime import timedelta
import hashlib
import json
from pathlib import Path
from uuid import UUID

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .manifest import sha256_file
from .retail_catalog import (START, CUTOVER, DAYS, TRADING_HOURS, retail_catalog,
                             trading_slots, calendar_factor, reference_rate)

VERSION = 'retail-chain-v2'
SHARD_ROWS = 100000
FACT_SCHEMA = pa.schema([
    ('event_id', pa.string()), ('transaction_id', pa.string()), ('line_number', pa.uint16()),
    ('store_id', pa.uint16()), ('district_id', pa.uint16()), ('sku', pa.uint32()),
    ('quantity', pa.int32()), ('price_minor', pa.int64()), ('receipts_minor', pa.int64()),
    ('sold_cost_minor', pa.int64()), ('event_time', pa.timestamp('ms', tz='UTC')),
    ('original_event_id', pa.string()),
])


def _id(seed: int, kind: str, index: int) -> str:
    prefix = int.from_bytes(hashlib.sha256(f'{VERSION}/{seed}/{kind}'.encode()).digest()[:8], 'big')
    return str(UUID(int=(prefix << 64) | index))


def _write(root, name, rows, schema=None, facts=False):
    path = root / name
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path, compression='zstd', row_group_size=10000)
    result = {'key': name, 'sha256': sha256_file(path), 'rows': len(rows), 'bytes': path.stat().st_size}
    if facts:
        result.update({field: sum(r[field] for r in rows)
                       for field in ('quantity', 'receipts_minor', 'sold_cost_minor')})
        result['transactions'] = len({r['transaction_id'] for r in rows})
    return result


def _companions(root: Path, seed: int, stores: list[dict], products: list[dict]):
    files = [_write(root, 'stores.parquet', stores), _write(root, 'products.parquet', products)]
    # Calendar exposure is shared across every store/product; a compact table preserves
    # zero-sale open intervals without materializing their 22.4M-row Cartesian product.
    availability = [{'hour': at, 'open': True, 'exposure_hours': 1.0,
                     'ample_stock': True, 'calendar_factor': calendar_factor(at)}
                    for at in trading_slots()]
    files.append(_write(root, 'availability.parquet', availability))
    inventory = []
    for store in stores:
        for product in products:
            inventory.append({'store_id': store['store_id'], 'sku': product['sku'],
                              'stock_units': 20 + (store['store_id'] + product['sku']) % 41,
                              'expires_at': CUTOVER + timedelta(hours=8),
                              'current_price_minor': product['reference_price_minor'],
                              'inventory_version': 1, 'price_version': 1, 'rule_version': 1})
    files.append(_write(root, 'inventory.parquet', inventory))
    rng = np.random.default_rng(seed + 100000)
    calibration = []
    for product in products:
        for group in range(4):
            for at in trading_slots():
                price = int(rng.choice(product['permitted_prices_minor']))
                ratio = price / product['reference_price_minor']
                rate = reference_rate(product['sku'], group) * calendar_factor(at)
                calibration.append({'sku': product['sku'], 'store_group': group, 'hour': at,
                                    'offered_price_minor': price,
                                    'reference_price_minor': product['reference_price_minor'],
                                    'units': int(rng.poisson(rate * ratio ** -2)),
                                    'exposure_hours': 1.0, 'calendar_factor': calendar_factor(at),
                                    'available_units': 10000, 'open': True})
    files.append(_write(root, 'calibration.parquet', calibration))
    return files


def generate_retail(output: Path, rows: int, seed: int = 17) -> Path:
    if type(rows) is not int or not 0 < rows < 2**63 or type(seed) is not int or not 0 <= seed < 2**32:
        raise ValueError('Use a positive integer row count and a nonnegative 32-bit seed.')
    output = Path(output)
    if output.exists() and any(output.iterdir()):
        raise ValueError('Choose an empty directory for an immutable retail dataset.')
    output.mkdir(parents=True, exist_ok=True)
    stores, products = retail_catalog()
    companions = _companions(output, seed, stores, products)
    rng = np.random.default_rng(seed)
    store_weights = np.array([0.8 + .15 * s['store_group'] for s in stores])
    product_weights = np.array([reference_rate(p['sku'], 0) for p in products])
    slot_times = list(trading_slots())
    slot_weights = np.array([calendar_factor(at) for at in slot_times])
    files = []
    for start in range(0, rows, SHARD_ROWS):
        count = min(SHARD_ROWS, rows - start)
        baskets = (count + 1) // 2
        store_indices = rng.choice(len(stores), baskets, p=store_weights / store_weights.sum())
        slot_indices = rng.choice(len(slot_times), baskets, p=slot_weights / slot_weights.sum())
        seconds = rng.integers(0, 3599, baskets)
        sku_indices = rng.choice(len(products), count, p=product_weights / product_weights.sum())
        batch = []
        for i in range(count):
            index = start + i
            store, product = stores[int(store_indices[i // 2])], products[int(sku_indices[i])]
            at = slot_times[int(slot_indices[i // 2])] + timedelta(seconds=int(seconds[i // 2]))
            price = product['reference_price_minor']
            row = {'event_id': _id(seed, 'line', index),
                   'transaction_id': _id(seed, 'basket', index // 2), 'line_number': i % 2 + 1,
                   'store_id': store['store_id'], 'district_id': store['district_id'],
                   'sku': product['sku'], 'quantity': 1, 'price_minor': price,
                   'receipts_minor': price, 'sold_cost_minor': product['unit_cost_minor'],
                   'event_time': at, 'original_event_id': str(UUID(int=0))}
            if index % 100 == 99:
                original = batch[-1]
                row = {**original, 'event_id': row['event_id'],
                       'transaction_id': _id(seed, 'return', index), 'line_number': 1,
                       'quantity': -1, 'receipts_minor': -original['receipts_minor'],
                       'sold_cost_minor': -original['sold_cost_minor'],
                       'original_event_id': original['event_id'],
                       'event_time': original['event_time'] + timedelta(milliseconds=1)}
            batch.append(row)
        files.append(_write(output, f'sales-{len(files):05}.parquet', batch, FACT_SCHEMA, True))
        print(f'Generated {start + count:,}/{rows:,} retail sales lines', flush=True)
    expected = sum(reference_rate(p['sku'], s['store_group']) for p in products for s in stores)
    expected *= sum(calendar_factor(at) for at in slot_times)
    manifest = {'dataset_version': VERSION, 'generator_version': '2.0', 'tier': 'showcase',
                'seed': seed, 'source_namespace': VERSION, 'synthetic': True, 'currency': 'SGD',
                'history_start': START.isoformat(), 'history_cutover': CUTOVER.isoformat(),
                'calendar_timezone': 'Asia/Singapore', 'trading_hours': TRADING_HOURS,
                'days': DAYS, 'rows': rows, 'transactions': sum(f['transactions'] for f in files),
                'history_rate_scale': (rows - rows // 100) / expected,
                'exposure_scope': 'availability.parquet applies to every store/product; ample stock',
                'calibration_scope': 'independent randomized ample-stock exposure; excluded from sales',
                'files': files, 'companion_files': companions}
    path = output / 'manifest.json'
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, required=True)
    parser.add_argument('--seed', type=int, default=17)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print(generate_retail(args.output, args.rows, args.seed))


if __name__ == '__main__':
    main()
