from datetime import UTC, datetime
from uuid import UUID
import pytest
from pricing_lab.retail_stream import generate_batch


def test_stream_is_repeatable_and_uses_acknowledged_price():
    state = [dict(store_id=1, district_id=1, sku=101, stock_units=20, current_price_minor=700,
                  unit_cost_minor=400, price_version=2, inventory_version=3)]
    args = (UUID(int=1), 1, state, 10, datetime(2026,1,30,2,0,2,tzinfo=UTC), 19)
    a, b = generate_batch(*args), generate_batch(*args)
    assert a == b
    assert len({r['transaction_id'] for r in a}) == 10
    assert len({r['event_id'] for r in a}) == len(a)
    assert all(r['price_minor'] == 700 and r['price_version'] == 2 for r in a)
    assert sum(r['quantity'] for r in a) <= 20
    assert all(r['event_time'] < args[4] for r in a)


def test_stream_rejects_bad_count_and_stops_at_stock():
    with pytest.raises(ValueError):
        generate_batch(UUID(int=1), 1, [], -1, datetime.now(UTC), 17)
    assert generate_batch(UUID(int=1), 1, [], 20, datetime.now(UTC), 17) == []


def test_district_demand_control_changes_transaction_mix():
    base=dict(sku=101,stock_units=2000,current_price_minor=800,unit_cost_minor=400,price_version=1,inventory_version=1)
    state=[dict(base,store_id=1,district_id=1,demand_multiplier=0),dict(base,store_id=251,district_id=2,demand_multiplier=2)]
    rows=generate_batch(UUID(int=1),1,state,100,datetime(2026,1,30,2,0,2,tzinfo=UTC),17)
    assert len(rows)==100
    assert {r['district_id'] for r in rows}=={2}
