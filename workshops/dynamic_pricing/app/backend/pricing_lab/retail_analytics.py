"""Bounded clients for the same analytical SQL used by learners and managed agents."""
from .settings import LAB_ROOT
from .retail_contracts import PriceInput, PriceResult, RetailSubject


def price_sql(input_sql):
    return (LAB_ROOT/'sql/retail/compare_prices.sql').read_text().replace('__INPUT__',input_sql)


class RetailAnalytics:
    def __init__(self,analytics):
        self.analytics=analytics
        self.db=analytics.database

    def compare(self,inputs: PriceInput,*,measured=False):
        columns={'stock_units':'UInt64','current_price_minor':'Int64','unit_cost_minor':'Int64',
                 'selling_cost_minor':'Int64','disposal_cost_minor':'Int64','base_demand_units':'Float64',
                 'elasticity':'Float64','permitted_prices_minor':'Array(Int64)',
                 'demand_multipliers':'Array(Float64)','scenario_weights':'Array(Float64)'}
        select='SELECT '+','.join('{'+name+':'+kind+'} AS '+name for name,kind in columns.items())
        params={k:getattr(inputs,k) for k in columns}
        params.update(base_demand_units=float(inputs.base_demand_units),elasticity=float(inputs.elasticity),
                      demand_multipliers=list(map(float,inputs.demand_multipliers)),scenario_weights=list(map(float,inputs.scenario_weights)),
                      permitted_prices_minor=list(inputs.permitted_prices_minor))
        if measured:
            result=self.analytics.measured_query(price_sql(select),params)
            return dict(result,rows=[PriceResult.model_validate(r) for r in result['rows']])
        return [PriceResult.model_validate(r) for r in self.analytics.query(price_sql(select),params)]

    def opportunities(self,run_id,district_id,cutoff,limit=20):
        if not 1<=district_id<=20 or not 1<=limit<=100 or cutoff.tzinfo is None or cutoff.second or cutoff.microsecond:
            raise ValueError('Choose a district, 1–100 results, and a closed UTC minute.')
        return self.analytics.query(f'''SELECT * FROM {self.db}.retail_opportunities(
          run={{run:UUID}}, district={{district:UInt16}}, cutoff={{cutoff:DateTime64(3,'UTC')}})
          ORDER BY excess_units DESC, store_id, sku LIMIT {{limit:UInt16}}''',
          dict(run=run_id,district=district_id,cutoff=cutoff,limit=limit))

    def price_inputs(self,run_id,subject: RetailSubject,cutoff):
        rows=self.analytics.query(f'''SELECT * FROM {self.db}.retail_opportunities(
          run={{run:UUID}}, district={{district:UInt16}}, cutoff={{cutoff:DateTime64(3,'UTC')}})
          WHERE store_id={{store:UInt16}} AND sku={{sku:UInt32}} LIMIT 1''',
          dict(run=run_id,district=(subject.store_id-1)//250+1,store=subject.store_id,sku=subject.sku,cutoff=cutoff))
        if not rows or rows[0]['eligibility']!='ready':
            raise ValueError('This product is not ready for a price comparison.')
        row=rows[0]
        return PriceInput.model_validate(dict(subject=subject,cutoff=cutoff,
            **{k:row[k] for k in PriceInput.model_fields if k not in ('subject','cutoff')}))

    def model_version(self):
        return self.analytics.query(f'SELECT min(model_version) version FROM {self.db}.retail_model_parameters')[0]['version']
