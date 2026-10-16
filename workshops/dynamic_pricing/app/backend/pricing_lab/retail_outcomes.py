"""Common-random-number synthetic outcomes, distinct from price forecasts."""
import numpy as np
from scipy.stats import poisson


def paired_demand(seed,sequence,inputs,price_minor):
    if price_minor not in inputs.permitted_prices_minor:
        raise ValueError('Use a permitted comparison price.')
    draw=np.random.default_rng(np.random.SeedSequence([seed,sequence])).uniform()
    unchanged=float(inputs.base_demand_units)
    applied=unchanged*(price_minor/inputs.current_price_minor)**float(inputs.elasticity)
    return int(poisson.ppf(draw,applied)),int(poisson.ppf(draw,unchanged))


def pair_rows(source,run_id,sequence,logical_end):
    from datetime import timedelta
    from decimal import Decimal
    from uuid import UUID,uuid5
    from .retail_contracts import PriceInput
    rows=[]
    for comparison in source.comparisons(run_id):
        inputs=PriceInput.model_validate(comparison['inputs'])
        end=min(logical_end,inputs.expires_at)
        elapsed=(end-comparison['logical_end']).total_seconds()
        if elapsed<=0:
            continue
        fraction=Decimal(str(elapsed))/Decimal(str((inputs.expires_at-inputs.cutoff).total_seconds()))
        # Evaluation uses the synthetic environment's known response, with a separate seed.
        step=inputs.model_copy(update={'base_demand_units':inputs.base_demand_units*fraction,'elasticity':Decimal('-2')})
        receipt=comparison['receipt_id']
        proposed,unchanged=paired_demand(comparison['seed']+(receipt.int%2**32),sequence,step,comparison['price_minor'])
        for branch,units,price,version in [
            ('applied',min(comparison['applied_stock'],proposed),comparison['price_minor'],comparison['price_version']),
            ('comparison',min(comparison['comparison_stock'],unchanged),inputs.current_price_minor,inputs.price_version)]:
            for i in range(units):
                event=uuid5(receipt,f'{sequence}/{branch}/{i}')
                rows.append(dict(event_id=event,transaction_id=event,line_number=1,
                    store_id=inputs.subject.store_id,district_id=(inputs.subject.store_id-1)//250+1,sku=inputs.subject.sku,
                    quantity=1,price_minor=price,receipts_minor=price,sold_cost_minor=inputs.unit_cost_minor,
                    original_event_id=UUID(int=0),
                    event_time=end-timedelta(milliseconds=1),price_version=version,
                    inventory_version=comparison['inventory_version'],branch=branch,receipt_id=receipt))
    return rows


class OutcomeService:
    def __init__(self,analytics):
        self.analytics=analytics

    def series(self,run_id,receipt_id):
        from .analytics import identifier
        db=identifier(self.analytics.database)
        rows=self.analytics.query(f"SELECT * FROM {db}.retail_outcome_series(run={{run:UUID}},receipt={{receipt:UUID}}) ORDER BY minute,branch LIMIT 1000",{'run':run_id,'receipt':receipt_id})
        return [dict(r,expired=bool(r['expired'])) for r in rows]


def install_retail_outcomes(analytics):
    from .settings import LAB_ROOT
    from .analytics import identifier
    db=identifier(analytics.database)
    sql=(LAB_ROOT/'sql/retail/outcomes.sql').read_text().replace('__DATABASE__',db)
    analytics.command(f'CREATE OR REPLACE VIEW {db}.retail_outcome_series AS {sql}')
