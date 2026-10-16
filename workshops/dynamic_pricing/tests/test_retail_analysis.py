from decimal import Decimal
import pytest
from pricing_lab.retail_analytics import RetailAnalytics
from pricing_lab.retail_contracts import PriceInput
from pricing.retail_reference import evaluate_reference

pytestmark=pytest.mark.integration


@pytest.mark.parametrize('case',['markdown','keep','uncertainty','zero_stock','zero_demand'])
def test_sql_prices_match_independent_arithmetic(analytics,retail_fixture,case):
    data=retail_fixture['input'].copy()
    if case=='keep': data['stock_units']=4
    if case=='zero_stock': data['stock_units']=0
    if case=='zero_demand': data['base_demand_units']='0'
    if case=='uncertainty':
        data.update(demand_multipliers=['0.5','1','1.5'],scenario_weights=['0.25','0.5','0.25'])
    inputs=PriceInput.model_validate(data)
    actual=RetailAnalytics(analytics).compare(inputs)
    expected=evaluate_reference(inputs)
    assert {r.price_minor for r in actual}=={600,700,800}
    for want in expected:
        got=next(r for r in actual if r.price_minor==want.price_minor)
        for field in ['units','receipts_minor','waste_units','net_recovery_minor','contribution_minor','incremental_profit_minor']:
            assert abs(getattr(got,field)-getattr(want,field)) < Decimal('.01')
        assert got.recommended==want.recommended


@pytest.fixture
def opportunity_case(analytics):
    from datetime import UTC,datetime,timedelta
    from uuid import uuid4
    from pricing_lab.retail_schema import install_retail_schema,install_retail_analysis
    db=analytics.database
    install_retail_schema(analytics)
    schemas={
      'retail_inventory_history':"run_id UUID,store_id Int32,sku Int32,version Int64,stock_units Int32,expires_at DateTime64(3,'UTC'),effective_sequence Int64",
      'retail_price_history':'run_id UUID,store_id Int32,sku Int32,version Int64,price_minor Int64,effective_sequence Int64',
      'retail_policy_history':'run_id UUID,store_id Int32,sku Int32,version Int64,permitted_prices_minor Array(Int64),unit_cost_minor Int64,selling_cost_minor Int64,disposal_cost_minor Int64,effective_sequence Int64',
      'retail_batches':"run_id UUID,batch_id UUID,sequence Int64,event_count Int64,status String,logical_end DateTime64(3,'UTC'),acknowledged_at DateTime64(3,'UTC')",
      'retail_runs':"run_id UUID,status String,created_at DateTime64(3,'UTC')"}
    for name,columns in schemas.items():
        key='run_id,batch_id' if name=='retail_batches' else 'run_id' if name=='retail_runs' else 'run_id,store_id,sku,version'
        analytics.command(f'CREATE TABLE {db}.{name}_cdc ({columns},_peerdb_version UInt64,_peerdb_is_deleted UInt8) ENGINE=ReplacingMergeTree(_peerdb_version) ORDER BY ({key})')
    run,batch=uuid4(),uuid4()
    cutoff=datetime(2026,1,30,8,tzinfo=UTC)
    analytics.command(f"INSERT INTO {db}.retail_products VALUES (101,'Sandwich','Food',800,400,20,50,[600,700,800],'SGD')")
    analytics.command(f"INSERT INTO {db}.retail_stores VALUES (1,'Store 1',1,'District 1',0,'Asia/Singapore')")
    analytics.command(f"INSERT INTO {db}.retail_model_parameters VALUES ('fixture',101,0,-2,8,0.75,1)")
    analytics.command(f"INSERT INTO {db}.retail_demand_scenarios VALUES ('fixture','typical',1,1)")
    for day in [2,9,16,23]:
        for hour in range(2,10):
            at=datetime(2026,1,day,hour,tzinfo=UTC)
            analytics.client.insert(f'{db}.retail_availability',[[at,True,1.,True,1.]],column_names=['hour','open','exposure_hours','ample_stock','calendar_factor'])
            analytics.client.insert(f'{db}.retail_sales',[[uuid4(),uuid4(),1,1,101,8,800,6400,3200,at]],column_names=['event_id','transaction_id','store_id','district_id','sku','quantity','price_minor','receipts_minor','sold_cost_minor','event_time'])
    analytics.command(f"INSERT INTO {db}.retail_inventory_history_cdc VALUES ('{run}',1,101,1,20,'2026-01-30 10:00:00',1,1,0)")
    analytics.command(f"INSERT INTO {db}.retail_price_history_cdc VALUES ('{run}',1,101,1,800,0,1,0)")
    analytics.command(f"INSERT INTO {db}.retail_policy_history_cdc VALUES ('{run}',1,101,1,[600,700,800],400,20,50,0,1,0)")
    analytics.command(f"INSERT INTO {db}.retail_runs_cdc VALUES ('{run}','stopped',now64(3),1,0)")
    analytics.command(f"INSERT INTO {db}.retail_batches_cdc VALUES ('{run}','{batch}',1,2,'acknowledged','2026-01-30 08:00:00',now64(3),1,0)")
    for _ in range(2):
        analytics.client.insert(f'{db}.retail_sales',[[uuid4(),uuid4(),1,1,101,1,800,800,400,cutoff-timedelta(minutes=5),run,batch,1,'live']],column_names=['event_id','transaction_id','store_id','district_id','sku','quantity','price_minor','receipts_minor','sold_cost_minor','event_time','run_id','batch_id','sequence','source'])
    from pricing_lab.retail_schema import create_rollup_target,rollup_select
    create_rollup_target(analytics,'retail_sales_minute')
    select,keys=rollup_select(db)
    analytics.command(f'INSERT INTO {db}.retail_sales_minute {select} GROUP BY {keys}')
    analytics.command(f'CREATE MATERIALIZED VIEW {db}.retail_sales_minute_mv TO {db}.retail_sales_minute AS {select} GROUP BY {keys}')
    install_retail_analysis(analytics)
    return RetailAnalytics(analytics),run,cutoff


def test_opportunity_uses_exposure_and_price_view(opportunity_case,analytics):
    client,run,cutoff=opportunity_case
    row=client.opportunities(run,1,cutoff)[0]
    assert row['eligibility']=='ready'
    assert row['base_demand_units']==pytest.approx(16)
    assert row['excess_units']==pytest.approx(4)
    from pricing_lab.retail_contracts import RetailSubject
    assert client.price_inputs(run,RetailSubject(store_id=1,sku=101),cutoff).base_demand_units == 16
    rows=analytics.query(f"SELECT * FROM {analytics.database}.retail_price_comparison(run={{run:UUID}},store=1,sku=101,cutoff={{cutoff:DateTime64(3,'UTC')}},model='fixture')",{'run':run,'cutoff':cutoff})
    assert next(r for r in rows if r['recommended'])['price_minor']==700


@pytest.mark.parametrize('defect,expected',[('expired','expired'),('empty','no_stock'),('missing_model','unsupported_model'),('missing_exposure','missing_history'),('stockout','stockout'),('future','waiting_for_cdc')])
def test_ineligible_inputs_never_silently_pass(opportunity_case,analytics,defect,expected):
    client,run,cutoff=opportunity_case
    db=analytics.database
    if defect=='expired':
        analytics.command(f"INSERT INTO {db}.retail_inventory_history_cdc VALUES ('{run}',1,101,2,20,'2026-01-30 07:00:00',1,2,0)")
    elif defect in ('empty','stockout'):
        analytics.command(f"INSERT INTO {db}.retail_inventory_history_cdc VALUES ('{run}',1,101,2,0,'2026-01-30 10:00:00',1,2,0)")
        if defect=='stockout':
            analytics.command(f"INSERT INTO {db}.retail_inventory_history_cdc VALUES ('{run}',1,101,3,20,'2026-01-30 10:00:00',1,3,0)")
    elif defect=='missing_model': analytics.command(f'TRUNCATE TABLE {db}.retail_model_parameters')
    elif defect=='missing_exposure': analytics.command(f'TRUNCATE TABLE {db}.retail_availability')
    else:
        from datetime import timedelta
        cutoff+=timedelta(minutes=1)
    assert client.opportunities(run,1,cutoff)[0]['eligibility']==expected


def test_future_inventory_is_excluded_and_slowdown_raises_risk(opportunity_case,analytics):
    from uuid import uuid4
    from datetime import timedelta
    client,run,cutoff=opportunity_case
    db=analytics.database
    before=client.opportunities(run,1,cutoff)[0]
    analytics.command(f"INSERT INTO {db}.retail_inventory_history_cdc VALUES ('{run}',1,101,100,999,'2026-01-30 10:00:00',3,100,0)")
    assert client.opportunities(run,1,cutoff)[0]['stock_units']==20
    batch=uuid4(); end=cutoff+timedelta(minutes=15)
    analytics.client.insert(f'{db}.retail_sales',[[uuid4(),uuid4(),1,1,101,1,800,800,400,end-timedelta(minutes=1),run,batch,2,'live']],column_names=['event_id','transaction_id','store_id','district_id','sku','quantity','price_minor','receipts_minor','sold_cost_minor','event_time','run_id','batch_id','sequence','source'])
    analytics.command(f"INSERT INTO {db}.retail_batches_cdc VALUES ('{run}','{batch}',2,1,'acknowledged','2026-01-30 08:15:00',now64(3),2,0)")
    assert client.opportunities(run,1,end)[0]['eligibility']=='waiting_for_cdc'
    analytics.command(f"INSERT INTO {db}.retail_inventory_history_cdc VALUES ('{run}',1,101,2,20,'2026-01-30 10:00:00',2,2,0)")
    after=client.opportunities(run,1,end)[0]
    assert after['eligibility']=='ready'
    assert after['recent_demand_factor']==.5
    assert after['base_demand_units']==pytest.approx(7)
    assert after['excess_units']>before['excess_units']


def test_deleted_inventory_is_held(opportunity_case,analytics):
    client,run,cutoff=opportunity_case
    analytics.command(f"INSERT INTO {analytics.database}.retail_inventory_history_cdc VALUES ('{run}',1,101,1,20,'2026-01-30 10:00:00',1,2,1)")
    assert client.opportunities(run,1,cutoff)[0]['eligibility']=='waiting_for_cdc'


def test_returns_do_not_become_negative_demand(opportunity_case,analytics):
    from uuid import uuid4
    client,run,cutoff=opportunity_case
    db=analytics.database
    analytics.command(f"INSERT INTO {db}.retail_sales(event_id,transaction_id,store_id,district_id,sku,quantity,price_minor,receipts_minor,sold_cost_minor,event_time) VALUES ('{uuid4()}','{uuid4()}',1,1,101,-5,800,-4000,-2000,'2026-01-23 08:00:00')")
    assert client.opportunities(run,1,cutoff)[0]['base_demand_units']==pytest.approx(16)


def test_censored_history_has_no_eligible_exposure(opportunity_case,analytics):
    client,run,cutoff=opportunity_case
    analytics.command(f'ALTER TABLE {analytics.database}.retail_availability UPDATE ample_stock=false WHERE 1 SETTINGS mutations_sync=2')
    assert client.opportunities(run,1,cutoff)[0]['eligibility']=='missing_history'


def test_held_run_cannot_recommend(opportunity_case,analytics):
    client,run,cutoff=opportunity_case
    analytics.command(f"INSERT INTO {analytics.database}.retail_runs_cdc VALUES ('{run}','held',now64(3),2,0)")
    assert client.opportunities(run,1,cutoff)[0]['eligibility']=='waiting_for_cdc'


@pytest.mark.parametrize('lag',['inventory_outside_window','price'])
def test_operational_convergence_is_independent_of_recent_demand(opportunity_case,analytics,lag):
    from uuid import uuid4
    from datetime import timedelta
    client,run,cutoff=opportunity_case;db=analytics.database
    if lag=='price':
        analytics.command(f"ALTER TABLE {db}.retail_sales UPDATE price_version=2,price_minor=700,receipts_minor=700 WHERE source='live' SETTINGS mutations_sync=2")
    else:
        analytics.command(f"INSERT INTO {db}.retail_inventory_history_cdc VALUES ('{run}',1,101,1,20,'2026-01-30 10:00:00',0,2,0)")
        end=cutoff+timedelta(minutes=20);batch=uuid4()
        analytics.client.insert(f'{db}.retail_sales',[[uuid4(),uuid4(),2,1,101,1,800,800,400,end-timedelta(seconds=1),run,batch,2,'live']],column_names=['event_id','transaction_id','store_id','district_id','sku','quantity','price_minor','receipts_minor','sold_cost_minor','event_time','run_id','batch_id','sequence','source'])
        analytics.command(f"INSERT INTO {db}.retail_batches_cdc VALUES ('{run}','{batch}',2,1,'acknowledged','2026-01-30 08:20:00',now64(3),2,0)")
        cutoff=end
    assert client.opportunities(run,1,cutoff)[0]['eligibility']=='waiting_for_cdc'


@pytest.mark.parametrize('slot',[7,9])
def test_missing_exposure_in_required_slot_is_not_zero_demand(opportunity_case,analytics,slot):
    client,run,cutoff=opportunity_case
    analytics.command(f'ALTER TABLE {analytics.database}.retail_availability UPDATE ample_stock=false WHERE toHour(hour)={slot} SETTINGS mutations_sync=2')
    assert client.opportunities(run,1,cutoff)[0]['eligibility']=='missing_history'
