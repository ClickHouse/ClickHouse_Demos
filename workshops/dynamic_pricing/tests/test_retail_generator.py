"""Exercise the actual Parquet artifacts, not generator implementation details."""
import json
from datetime import UTC
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from generator.manifest import sha256_file, read_manifest


def test_catalog_covers_chain_and_price_policies():
    from generator.retail_catalog import retail_catalog
    stores, products = retail_catalog()
    assert {s['store_id'] for s in stores} == set(range(1, 5001))
    assert len({s['district_id'] for s in stores}) == 20
    assert len(products) == len({p['sku'] for p in products}) == 20
    for product in products:
        assert product['currency'] == 'SGD'
        assert product['reference_price_minor'] in product['permitted_prices_minor']
        assert all(0 < p <= product['reference_price_minor'] for p in product['permitted_prices_minor'])


def test_generated_history_is_reproducible_and_reconciles(tmp_path):
    from generator.retail import generate_retail
    paths = [generate_retail(tmp_path / name, rows=20000, seed=17) for name in ('a', 'b')]
    manifests = [json.loads(path.read_text()) for path in paths]
    assert manifests[0] == manifests[1]
    manifest = manifests[0]
    assert manifest['rows'] == 20000
    facts = []
    for file in manifest['files']:
        path = paths[0].parent / file['key']
        assert sha256_file(path) == file['sha256']
        rows = pq.read_table(path).to_pylist()
        assert len(rows) == file['rows']
        assert sum(r['receipts_minor'] for r in rows) == file['receipts_minor']
        facts.extend(rows)
    assert len(facts) == len({r['event_id'] for r in facts}) == 20000
    assert len({r['transaction_id'] for r in facts}) == manifest['transactions']
    assert len({r['district_id'] for r in facts}) == 20
    assert len({r['event_time'].date() for r in facts}) == 28
    assert all(2 <= r['event_time'].astimezone(UTC).hour < 10 for r in facts)
    sales = {r['event_id']: r for r in facts if r['quantity'] > 0}
    returned = {}
    for row in facts:
        if row['quantity'] < 0:
            original = sales[row['original_event_id']]
            assert row['event_time'] >= original['event_time']
            assert row['price_minor'] == original['price_minor']
            returned[original['event_id']] = returned.get(original['event_id'], 0) - row['quantity']
            assert returned[original['event_id']] <= original['quantity']
    assert returned
    for file in manifest['companion_files']:
        assert sha256_file(paths[0].parent / file['key']) == file['sha256']
    inventory_file = next(f for f in manifest['companion_files'] if f['key'] == 'inventory.parquet')
    assert inventory_file['rows'] == 100000
    assert all('calibration' not in f['key'] for f in manifest['files'])
    assert read_manifest(paths[0]) == manifest


def test_rejects_invalid_counts_and_existing_output(tmp_path):
    from generator.retail import generate_retail
    for count in (0, -1, True, 1.2):
        with pytest.raises(ValueError):
            generate_retail(tmp_path/'bad', rows=count)
    occupied = tmp_path/'occupied'
    occupied.mkdir()
    (occupied/'keep').write_text('preserve')
    with pytest.raises(ValueError):
        generate_retail(occupied, rows=10)
    assert (occupied/'keep').read_text() == 'preserve'
