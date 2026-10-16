"""SQL shared by the console exercises and automated retail setup."""
from .analytics import identifier
from .settings import LAB_ROOT


def install_retail_schema(analytics):
    db = identifier(analytics.database)
    for name in ('006_retail_sales.sql', '008_retail_dimensions.sql'):
        text = (LAB_ROOT / 'sql/clickhouse' / name).read_text().replace('__DATABASE__', db)
        for statement in text.split(';'):
            if statement.strip():
                analytics.command(statement)
    analytics.command(f"CREATE TABLE IF NOT EXISTS {db}.retail_sales_console AS {db}.retail_sales")


def rollup_select(database, store=True):
    db = identifier(database)
    keys = 'district_id, ' + ('store_id, ' if store else '') + 'sku, run_id, branch, source'
    return f'''SELECT {keys}, toStartOfMinute(event_time) AS minute,
      sumState(toInt64(quantity)) AS units_state, sumState(toInt64(greatest(quantity,0))) AS sold_units_state, sumState(receipts_minor) AS receipts_state,
      sumState(sold_cost_minor) AS cost_state, countState() AS rows_state,
      uniqExactState(transaction_id) AS transactions_state, maxState(sequence) AS sequence_state
      FROM {db}.retail_sales''', keys + ', minute'


def create_rollup_target(analytics, name, store=True):
    text = (LAB_ROOT / 'sql/clickhouse/007_retail_rollup.sql').read_text()
    for old, new in [('__DATABASE__', identifier(analytics.database)), ('__TARGET__', identifier(name)),
                     ('__STORE_COLUMN__', 'store_id UInt16,' if store else ''),
                     ('__STORE_KEY__', 'store_id,' if store else '')]:
        text = text.replace(old, new)
    analytics.command(text)


def install_retail_analysis(analytics):
    from .retail_analytics import price_sql
    db=identifier(analytics.database)
    discovery=(LAB_ROOT/'sql/retail/discover.sql').read_text().replace('__DATABASE__',db)
    analytics.command(f'CREATE OR REPLACE VIEW {db}.retail_opportunities AS {discovery}')
    inputs=f'''SELECT stock_units,current_price_minor,unit_cost_minor,selling_cost_minor,disposal_cost_minor,
      base_demand_units,elasticity,permitted_prices_minor,demand_multipliers,scenario_weights
      FROM {db}.retail_opportunities(run={{run:UUID}},district=toUInt16(intDiv({{store:UInt16}}-1,250)+1),
      cutoff={{cutoff:DateTime64(3,'UTC')}})
      WHERE store_id={{store:UInt16}} AND sku={{sku:UInt32}} AND model_version={{model:String}} AND eligibility='ready' '''
    analytics.command(f'CREATE OR REPLACE VIEW {db}.retail_price_comparison AS '+price_sql(inputs))
    analytics.command((LAB_ROOT/'sql/clickhouse/010_retail_analysis.sql').read_text().replace('__DATABASE__',db))


def install_retail_agent_access(analytics):
    db=identifier(analytics.database)
    text=(LAB_ROOT/'sql/clickhouse/011_retail_agent_access.sql').read_text()
    for old,new in [('__DATABASE__',db),('__ROLE__',identifier(db+'_retail_reader')),('__PROFILE__',identifier(db+'_retail_reader_limits'))]:
        text=text.replace(old,new)
    for statement in text.split(';'):
        if statement.strip():analytics.command(statement)
    return db+'_retail_reader'
