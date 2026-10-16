from decimal import Decimal
import pytest
from pydantic import ValidationError
from pricing_lab.retail_contracts import PriceInput
from pricing.retail_reference import evaluate_reference


def test_reference_profits_and_keep_price(retail_fixture):
    data = retail_fixture['input']
    rows = evaluate_reference(PriceInput.model_validate(data))
    chosen = next(r for r in rows if r.recommended)
    assert chosen.price_minor == 700
    assert chosen.net_recovery_minor == Decimal('13600')
    assert chosen.incremental_profit_minor == Decimal('1320')
    assert chosen.contribution_minor == Decimal('5600')
    assert next(r for r in rows if r.price_minor == 800).net_recovery_minor == Decimal('12280')
    for stock, demand in [(4, '16'), (0, '16'), (20, '0')]:
        results = evaluate_reference(PriceInput.model_validate({**data, 'stock_units': stock,
                                                              'base_demand_units': demand}))
        assert next(r.price_minor for r in results if r.recommended) == 800
        assert all(r.units <= stock for r in results)


def test_scenarios_are_capped_before_weighting(retail_fixture):
    data = {**retail_fixture['input'], 'demand_multipliers': ['0.5', '1', '1.5'],
            'scenario_weights': ['0.25', '0.5', '0.25']}
    result = next(r for r in evaluate_reference(PriceInput.model_validate(data)) if r.price_minor == 800)
    assert result.units == Decimal('15')
    assert result.waste_units == Decimal('5')


def test_unsupported_price_rejected(retail_fixture):
    with pytest.raises(ValidationError):
        PriceInput.model_validate({**retail_fixture['input'], 'permitted_prices_minor': [900, 800]})
