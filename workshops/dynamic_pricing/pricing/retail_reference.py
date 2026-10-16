"""Independent decimal oracle for the SQL price comparison, not a serving model."""
from decimal import Decimal, localcontext
from pricing_lab.retail_contracts import PriceInput, PriceResult


def evaluate_reference(inputs: PriceInput) -> list[PriceResult]:
    with localcontext() as ctx:
        ctx.prec = 40
        rows = []
        for price in inputs.permitted_prices_minor:
            response = (Decimal(price) / inputs.current_price_minor) ** inputs.elasticity
            units = sum((min(Decimal(inputs.stock_units), inputs.base_demand_units * m * response) * w
                         for m, w in zip(inputs.demand_multipliers, inputs.scenario_weights)), Decimal(0))
            waste = inputs.stock_units - units
            recovery = units * (price - inputs.selling_cost_minor) - waste * inputs.disposal_cost_minor
            rows.append(dict(price_minor=price, units=units, receipts_minor=units * price,
                             waste_units=waste, net_recovery_minor=recovery,
                             contribution_minor=recovery - inputs.stock_units * inputs.unit_cost_minor))
        baseline = next(r['net_recovery_minor'] for r in rows if r['price_minor'] == inputs.current_price_minor)
        best = max(rows, key=lambda r: (r['net_recovery_minor'],
                                       r['price_minor'] == inputs.current_price_minor, r['price_minor']))
        return [PriceResult(**r, incremental_profit_minor=r['net_recovery_minor'] - baseline,
                            recommended=r is best) for r in rows]
