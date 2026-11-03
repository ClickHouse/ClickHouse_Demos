from datetime import datetime, UTC
from uuid import uuid4
import pytest
from generator.retail import generate_retail
from pricing_lab.retail_history import RetailHistory

pytestmark = pytest.mark.integration


def test_retail_raw_rollup_parity(analytics, ledger, tmp_path):
    path = generate_retail(tmp_path / 'data', 1000)
    history = RetailHistory(analytics, ledger)
    history.load(path.as_uri())
    assert history.create_rollup()['ready']
    assert history.create_rollup()['ready']
    db = analytics.database
    # One basket spans two SKUs. Exact transaction states must merge across products.
    at = datetime(2026, 1, 30, 2, tzinfo=UTC)
    columns = ['event_id','transaction_id','store_id','district_id','sku','quantity',
               'price_minor','receipts_minor','sold_cost_minor','event_time','branch','source']
    transaction = uuid4()
    rows = [[uuid4(),transaction,1,1,101,2,800,1600,800,at,'applied','live'],
            [uuid4(),transaction,1,1,102,2,800,1600,800,at,'applied','live'],
            [uuid4(),uuid4(),1,1,101,-1,800,-800,-400,at,'applied','live'],
            [uuid4(),uuid4(),1,1,101,1,800,800,400,at,'comparison','live']]
    analytics.client.insert(f'{db}.retail_sales', rows, column_names=columns)
    raw = analytics.query(f"SELECT sum(quantity) units, sum(receipts_minor) receipts, count() rows, uniqExact(transaction_id) transactions FROM {db}.retail_sales WHERE source='live' AND branch='applied'")[0]
    assert raw == dict(units=3, receipts=2400, rows=3, transactions=2)
    for target in ['retail_sales_minute','retail_district_minute']:
        aggregate = analytics.query(f"SELECT sumMerge(units_state) units, sumMerge(receipts_state) receipts, countMerge(rows_state) rows, uniqExactMerge(transactions_state) transactions FROM {db}.{target} WHERE source='live' AND branch='applied'")[0]
        assert aggregate == raw
        assert analytics.query(f"SELECT countMerge(rows_state) n FROM {db}.{target} WHERE minute < {{cutoff:DateTime}} AND source='live'", {'cutoff': at})[0]['n'] == 0

def test_manual_aggregates_register_without_backfill(analytics,ledger,tmp_path):
    from generator.retail import generate_retail
    from pricing_lab.retail_history import RetailHistory
    from pricing_lab.retail_schema import create_rollup_target,rollup_select
    history=RetailHistory(analytics,ledger)
    history.load(generate_retail(tmp_path/'manual',100).as_uri())
    for name,store in [('retail_sales_minute',True),('retail_district_minute',False)]:
        create_rollup_target(analytics,name,store)
        select,keys=rollup_select(analytics.database,store)
        analytics.command(f'INSERT INTO {analytics.database}.{name} {select} GROUP BY {keys}')
    assert history.register_rollup()['ready']
    assert history.register_rollup()['ready']
    assert analytics.query(f'SELECT countMerge(rows_state) n FROM {analytics.database}.retail_district_minute')[0]['n']==100


def test_manual_aggregate_wrong_group_is_rejected(analytics,ledger,tmp_path):
    from pricing_lab.retail_schema import create_rollup_target,rollup_select
    history=RetailHistory(analytics,ledger)
    history.load(generate_retail(tmp_path/'wrong',100).as_uri())
    for name,store in [('retail_sales_minute',True),('retail_district_minute',False)]:
        create_rollup_target(analytics,name,store)
        select,keys=rollup_select(analytics.database,store)
        if not store:select=select.replace('SELECT district_id,','SELECT toUInt16(1) AS district_id,')
        analytics.command(f'INSERT INTO {analytics.database}.{name} {select} GROUP BY {keys}')
    with pytest.raises(ValueError,match='grouped facts'):
        history.register_rollup(full_check=True)


def test_quick_registration_does_not_scan_sales_or_aggregate_data(analytics,ledger,tmp_path,monkeypatch):
    from pricing_lab.retail_schema import create_rollup_target,rollup_select
    history=RetailHistory(analytics,ledger)
    history.load(generate_retail(tmp_path/'quick',100).as_uri())
    for name,store in [('retail_sales_minute',True),('retail_district_minute',False)]:
        create_rollup_target(analytics,name,store)
        select,keys=rollup_select(analytics.database,store)
        analytics.command(f'INSERT INTO {analytics.database}.{name} {select} GROUP BY {keys}')
    query=analytics.query
    def metadata_only(sql,*args,**kwargs):
        if not sql.lstrip().startswith('DESCRIBE'):
            assert 'system.' in sql, 'Quick registration must only read metadata, not workshop data.'
        return query(sql,*args,**kwargs)
    monkeypatch.setattr(analytics,'query',metadata_only)
    assert history.register_rollup()['validation']=='quick'


def test_quick_registration_rejects_empty_aggregate(analytics,ledger,tmp_path):
    from pricing_lab.retail_schema import create_rollup_target,rollup_select
    history=RetailHistory(analytics,ledger)
    history.load(generate_retail(tmp_path/'empty',100).as_uri())
    for name,store in [('retail_sales_minute',True),('retail_district_minute',False)]:
        create_rollup_target(analytics,name,store)
        if store:
            select,keys=rollup_select(analytics.database,store)
            analytics.command(f'INSERT INTO {analytics.database}.{name} {select} GROUP BY {keys}')
    with pytest.raises(ValueError,match='empty'):
        history.register_rollup()
    with ledger.connect() as conn:
        assert not conn.execute('SELECT data FROM pricing.retail_history_state WHERE id').fetchone()['data'].get('rollup')


def test_full_check_can_verify_an_already_quick_registered_rollup(analytics,ledger,tmp_path,monkeypatch):
    from pricing_lab.retail_schema import create_rollup_target,rollup_select
    history=RetailHistory(analytics,ledger)
    history.load(generate_retail(tmp_path/'upgrade',100).as_uri())
    for name,store in [('retail_sales_minute',True),('retail_district_minute',False)]:
        create_rollup_target(analytics,name,store)
        select,keys=rollup_select(analytics.database,store)
        analytics.command(f'INSERT INTO {analytics.database}.{name} {select} GROUP BY {keys}')
    assert history.register_rollup()['validation']=='quick'
    assert history.register_rollup(full_check=True)['validation']=='full'
    with ledger.connect() as conn:
        assert conn.execute('SELECT data FROM pricing.retail_history_state WHERE id').fetchone()['data']['rollup_validation']=='full'


def test_registration_refuses_an_active_writer_without_scanning_data(analytics,ledger,tmp_path):
    history=RetailHistory(analytics,ledger)
    history.load(generate_retail(tmp_path/'writer',100).as_uri())
    with ledger.connect() as writer:
        assert writer.execute('SELECT pg_try_advisory_lock(84927642) locked').fetchone()['locked']
        with pytest.raises(ValueError,match='Stop the sales writer'):
            history.register_rollup()


def test_failed_full_check_preserves_previous_quick_registration(analytics,ledger,tmp_path):
    from pricing_lab.retail_schema import create_rollup_target,rollup_select
    history=RetailHistory(analytics,ledger)
    history.load(generate_retail(tmp_path/'limited',100).as_uri())
    for name,store in [('retail_sales_minute',True),('retail_district_minute',False)]:
        create_rollup_target(analytics,name,store)
        select,keys=rollup_select(analytics.database,store)
        if not store:select=select.replace('SELECT district_id,','SELECT toUInt16(1) AS district_id,')
        analytics.command(f'INSERT INTO {analytics.database}.{name} {select} GROUP BY {keys}')
    # Metadata checks deliberately do not claim grouped-data parity.
    assert history.register_rollup()['validation']=='quick'
    with pytest.raises(ValueError,match='grouped facts'):
        history.register_rollup(full_check=True)
    with ledger.connect() as conn:
        assert conn.execute('SELECT data FROM pricing.retail_history_state WHERE id').fetchone()['data']['rollup_validation']=='quick'
