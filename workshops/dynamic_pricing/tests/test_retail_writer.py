"""Exercise the real writer loop, source locks, delivery and terminal SQL."""
from datetime import timedelta
from types import SimpleNamespace
import json
import pytest
from psycopg.types.json import Jsonb
from test_retail_approval import approval_case
from pricing_lab.retail_source import RetailSource
from pricing_lab.retail_schema import install_retail_schema,create_rollup_target,rollup_select
from pricing_lab.retail_outcomes import OutcomeService,install_retail_outcomes
from app.loadgen import retail_writer

pytestmark=pytest.mark.integration


def prepare_writer(c,analytics,tmp_path,monkeypatch):
    install_retail_schema(analytics);db=analytics.database
    for name,store in [('retail_sales_minute',True),('retail_district_minute',False)]:
        create_rollup_target(analytics,name,store);select,keys=rollup_select(db,store)
        analytics.command(f'CREATE MATERIALIZED VIEW {db}.{name}_mv TO {db}.{name} AS {select} GROUP BY {keys}')
    for name in ['retail_inventory_current','retail_price_current','retail_policy_current','retail_opportunities']:
        analytics.command(f'CREATE VIEW {db}.{name} AS SELECT 1')
    with c.ledger.connect() as conn:
        conn.execute('INSERT INTO pricing.retail_history_state(data) VALUES (%s)',(Jsonb({'rollup':True}),))
        conn.execute('UPDATE pricing.retail_runs SET logical_end=%s',(c.inputs.expires_at-timedelta(seconds=2),))
    source=RetailSource(c.ledger,tmp_path/'spool')
    settings=SimpleNamespace(postgres_dsn=SimpleNamespace(get_secret_value=lambda:'unused'),retail_spool=tmp_path/'spool')
    monkeypatch.setattr(retail_writer.Settings,'from_env',lambda **kw:settings)
    monkeypatch.setattr(retail_writer,'Ledger',lambda _:c.ledger)
    monkeypatch.setattr(retail_writer,'RetailSource',lambda *args:source)
    from pricing_lab.analytics import AnalyticsClient
    monkeypatch.setattr(retail_writer,'AnalyticsClient',lambda _:AnalyticsClient(analytics.settings))
    return source


@pytest.mark.parametrize('empty',[False,True])
def test_writer_commits_expiry_and_final_waste(approval_case,analytics,tmp_path,monkeypatch,empty):
    import pricing_lab.retail_outcomes as outcomes
    c=approval_case;source=prepare_writer(c,analytics,tmp_path,monkeypatch)
    receipt=c.service.apply(c.request,'learner-1')
    monkeypatch.setattr(outcomes,'paired_demand',lambda *a,**kw:(0,0) if empty else (1,0))
    retail_writer.main(['--transactions-per-minute','30','--minutes','1','--speed','20'])
    assert source.run(c.run)['logical_end']==c.inputs.expires_at
    with c.ledger.connect() as conn:
        batch=conn.execute('SELECT * FROM pricing.retail_batches').fetchone()
        stored=conn.execute('SELECT data FROM pricing.retail_receipts').fetchone()['data']
    assert batch['status']=='acknowledged' and batch['event_count']==(0 if empty else 1)
    db=analytics.database
    analytics.command(f'CREATE TABLE {db}.retail_receipts_cdc(run_id UUID,receipt_id UUID,data String,_peerdb_version UInt64,_peerdb_is_deleted UInt8) ENGINE=ReplacingMergeTree(_peerdb_version) ORDER BY receipt_id')
    analytics.command(f"CREATE TABLE {db}.retail_batches_cdc(run_id UUID,batch_id UUID,event_count UInt64,logical_end DateTime64(3,'UTC'),status String,_peerdb_version UInt64,_peerdb_is_deleted UInt8) ENGINE=ReplacingMergeTree(_peerdb_version) ORDER BY batch_id")
    analytics.client.insert(f'{db}.retail_receipts_cdc',[[c.run,receipt.receipt_id,json.dumps(stored),1,0]],column_names=['run_id','receipt_id','data','_peerdb_version','_peerdb_is_deleted'])
    analytics.client.insert(f'{db}.retail_batches_cdc',[[c.run,batch['batch_id'],batch['event_count'],batch['logical_end'],'acknowledged',1,0]],column_names=['run_id','batch_id','event_count','logical_end','status','_peerdb_version','_peerdb_is_deleted'])
    install_retail_outcomes(analytics)
    final=[r for r in OutcomeService(analytics).series(c.run,receipt.receipt_id) if r['branch']=='applied'][-1]
    assert final['expired'] and final['waste_units']==(20 if empty else 19)


def test_writer_regenerates_after_concurrent_approval(approval_case,analytics,tmp_path,monkeypatch):
    c=approval_case;source=prepare_writer(c,analytics,tmp_path,monkeypatch)
    stage=source.stage;attempts=[]
    def interleaved(batch,rows):
        attempts.append(batch.sequence)
        if len(attempts)==1:c.service.apply(c.request,'learner-1')
        return stage(batch,rows)
    monkeypatch.setattr(source,'stage',interleaved)
    retail_writer.main(['--transactions-per-minute','30','--minutes','1','--speed','20'])
    assert attempts==[1,1]
    assert source.run(c.run)['logical_end']==c.inputs.expires_at
    with c.ledger.connect() as conn:
        assert conn.execute("SELECT count(*) n FROM pricing.retail_batches WHERE status='acknowledged'").fetchone()['n']==1


def test_writer_final_short_interval_does_not_backdate_sales(approval_case,analytics,tmp_path,monkeypatch):
    c=approval_case;prepare_writer(c,analytics,tmp_path,monkeypatch)
    start=c.inputs.expires_at-timedelta(milliseconds=1)
    with c.ledger.connect() as conn:conn.execute('UPDATE pricing.retail_runs SET logical_end=%s',(start,))
    retail_writer.main(['--transactions-per-minute','300','--minutes','1','--speed','20'])
    row=analytics.query(f"SELECT min(event_time) first,max(event_time) last,count() n FROM {analytics.database}.retail_sales WHERE source='live'")[0]
    assert row['n']>0 and row['first']>=start and row['last']<c.inputs.expires_at


def test_writer_reaches_complete_minutes_after_rate_change(approval_case,analytics,tmp_path,monkeypatch):
    c=approval_case;prepare_writer(c,analytics,tmp_path,monkeypatch)
    boundary=c.inputs.expires_at-timedelta(minutes=1)
    with c.ledger.connect() as conn:conn.execute('UPDATE pricing.retail_runs SET logical_end=%s',(boundary-timedelta(milliseconds=1),))
    retail_writer.main(['--transactions-per-minute','30','--minutes','1','--speed','20'])
    with c.ledger.connect() as conn:
        first=conn.execute('SELECT logical_end FROM pricing.retail_batches ORDER BY sequence LIMIT 1').fetchone()
    assert first['logical_end']==boundary
    with c.ledger.connect() as conn:
        final=conn.execute('SELECT logical_end FROM pricing.retail_runs').fetchone()['logical_end']
    assert final==boundary+timedelta(minutes=1)-timedelta(milliseconds=1)


def test_accelerated_writer_batches_a_full_synthetic_minute(approval_case,analytics,tmp_path,monkeypatch):
    c=approval_case;source=prepare_writer(c,analytics,tmp_path,monkeypatch)
    start=c.inputs.expires_at-timedelta(minutes=2)
    with c.ledger.connect() as conn:conn.execute('UPDATE pricing.retail_runs SET logical_end=%s',(start,))
    retail_writer.main(['--transactions-per-minute','10000','--minutes','1','--speed','20'])
    run=source.run(c.run)
    assert run['logical_end']==start+timedelta(minutes=1)
    assert run['sequence']==1
