"""Deterministic synthetic transactions priced from committed operational state."""
from datetime import timedelta
from uuid import UUID, uuid5
import numpy as np


def generate_batch(run_id, sequence, state, transactions, logical_end, seed, window_seconds=2.0):
    if type(transactions) is not int or transactions < 0 or sequence < 1 or logical_end.tzinfo is None:
        raise ValueError('Use nonnegative transactions, a positive sequence, and UTC-aware time.')
    if not state or transactions == 0:
        return []
    rng = np.random.default_rng(np.random.SeedSequence([seed, sequence]))
    available = [dict(s) for s in state if s['stock_units'] > 0]
    from generator.retail_catalog import reference_rate
    weights = np.array([reference_rate(s['sku'], (s['store_id']-1)%4) *
                        float(s.get('demand_multiplier', 1)) *
                        (s['current_price_minor'] / (800+(s['sku']-101)%5*100))**-2 for s in available])
    if not len(weights) or weights.sum() == 0:
        return []
    if not np.isfinite(weights).all() or (weights < 0).any():
        raise ValueError('Use finite nonnegative demand multipliers.')
    selections = rng.choice(len(available), transactions, p=weights/weights.sum())
    rows = []
    for i, index in enumerate(selections):
        if not available:
            break
        item = available[index]
        if item['stock_units'] <= 0:
            continue
        transaction = uuid5(run_id, f'{sequence}/transaction/{i}')
        rows.append(dict(event_id=uuid5(run_id,f'{sequence}/line/{i}'), transaction_id=transaction,
                         line_number=1, branch='applied', receipt_id=UUID(int=0), store_id=item['store_id'], district_id=item['district_id'], sku=item['sku'],
                         quantity=1, price_minor=item['current_price_minor'], receipts_minor=item['current_price_minor'],
                         sold_cost_minor=item['unit_cost_minor'], original_event_id=UUID(int=0),
                         event_time=logical_end - timedelta(milliseconds=1 + int(rng.integers(max(1,int(window_seconds*1000)-1)))),
                         price_version=item['price_version'], inventory_version=item['inventory_version']))
        item['stock_units'] -= 1
    return rows
