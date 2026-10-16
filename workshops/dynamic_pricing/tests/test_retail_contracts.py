"""Reject inputs that would make SQL forecasts or delivery evidence misleading."""
from decimal import Decimal
from uuid import uuid4

import pytest


def test_chain_boundary_and_fractional_demand(retail_fixture):
    from pricing_lab.retail_contracts import PriceInput
    value = PriceInput.model_validate(retail_fixture['input'] | {'base_demand_units': '16.25'})
    assert value.subject.store_id == 5000
    assert value.base_demand_units == Decimal('16.25')
    assert value.cutoff.utcoffset().total_seconds() == 0


@pytest.mark.parametrize('change', [
    {'subject': {'store_id': 5001, 'sku': 101}},
    {'subject': {'store_id': True, 'sku': 101}},
    {'subject': {'store_id': 1, 'sku': 0}},
    {'stock_units': -1}, {'stock_units': 1.5},
    {'base_demand_units': '-1'}, {'base_demand_units': 'Infinity'},
    {'elasticity': 'NaN'}, {'elasticity': '0.1'},
    {'permitted_prices_minor': [0, 800]},
    {'permitted_prices_minor': [700]},
    {'permitted_prices_minor': [700, 800, 800]},
    {'permitted_prices_minor': [True, 800]},
    {'permitted_prices_minor': [700, 800, 900]},
    {'scenario_weights': ['0.2']}, {'scenario_weights': []},
    {'scenario_weights': ['-1', '2'], 'demand_multipliers': ['1', '2']},
    {'scenario_weights': ['0.5', '0.5']},
    {'demand_multipliers': ['-1']}, {'demand_multipliers': ['NaN']},
    {'expires_at': '2026-01-30T08:00:00Z'},
    {'cutoff': '2026-01-30T08:00:00'}, {'model_version': ''},
])
def test_invalid_forecast_inputs_rejected(retail_fixture, change):
    from pricing_lab.retail_contracts import PriceInput
    with pytest.raises(ValueError):
        PriceInput.model_validate(retail_fixture['input'] | change)


def test_no_stock_and_zero_demand_are_valid_nonopportunities(retail_fixture):
    from pricing_lab.retail_contracts import PriceInput
    value = PriceInput.model_validate(retail_fixture['input'] | {
        'stock_units': 0, 'base_demand_units': '0', 'elasticity': '0'})
    assert value.stock_units == 0


def test_batch_counts_and_hash_are_validated():
    from pricing_lab.retail_contracts import RetailBatch
    batch = dict(run_id=uuid4(), batch_id=uuid4(), sequence=1,
                 payload_sha256='a' * 64, event_count=3, transaction_count=2,
                 logical_end='2026-01-30T08:00:00Z', created_at='2026-10-16T00:00:00Z')
    assert RetailBatch.model_validate(batch).transaction_count == 2
    for change in ({'payload_sha256': 'bad'}, {'transaction_count': 4},
                   {'sequence': 0}, {'event_count': 0}, {'transaction_count': True}):
        with pytest.raises(ValueError):
            RetailBatch.model_validate(batch | change)


def test_result_preserves_signed_profit_and_fractional_units():
    from pricing_lab.retail_contracts import PriceResult
    result = PriceResult(price_minor=600, units='1.25', receipts_minor='750',
                         waste_units='18.75', net_recovery_minor='-212.5',
                         contribution_minor='-8212.5', incremental_profit_minor='-100',
                         recommended=False)
    assert result.units == Decimal('1.25')
    assert result.incremental_profit_minor == -100
    with pytest.raises(ValueError):
        PriceResult.model_validate(result.model_dump() | {'units': '-1'})
