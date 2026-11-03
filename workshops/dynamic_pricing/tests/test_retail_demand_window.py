from datetime import UTC,datetime,timedelta
from types import SimpleNamespace
from uuid import uuid4
import pytest
from pricing_lab.retail_api import RetailWorkbench,StreamRequest
from app.loadgen import retail_writer

START=datetime(2026,1,30,2,20,10,tzinfo=UTC)


def test_temporary_acceleration_stops_at_the_requested_boundary():
    target=START+timedelta(minutes=15)
    control={'fast_forward_until':target.isoformat()}
    assert retail_writer.pacing(control,START,1)==(20,target)
    assert retail_writer.pacing(control,target,1)==(1,None)
    assert retail_writer.pacing(control,target+timedelta(seconds=1),3)==(3,None)
    assert retail_writer.pacing({},START,2)==(2,None)


@pytest.mark.integration
def test_slowdown_records_a_durable_window_and_reports_completed_evidence(ledger):
    run=uuid4();now={'cutoff':START.replace(second=0),'age':2,'visible':True}
    with ledger.connect() as conn:
        conn.execute('INSERT INTO pricing.retail_runs(run_id,manifest_digest,logical_end,seed) VALUES (%s,%s,%s,200017)',(run,'test',START))
    def query(sql,*args):
        if 'system.tables' in sql:return [{'name':name} for name in ('retail_analysis_context','retail_opportunities')]
        return [{'cutoff':now['cutoff'],'model_version':'test','delivery_age_seconds':now['age']}] if now['visible'] else []
    workbench=RetailWorkbench(ledger,SimpleNamespace(database='pricing',query=query),None,None,None,None)
    response=workbench.control(StreamRequest(action='slow_district',district=1))
    assert response['demand_window']['started_at']==START.isoformat()
    target=datetime(2026,1,30,2,36,tzinfo=UTC)
    assert response['demand_window']['ready_at']==target.isoformat()
    assert response['demand_window']['speed']==20
    initial=workbench.context()['demand_windows']['1']
    assert initial['covered_seconds']==0 and not initial['ready']
    now['cutoff']=target-timedelta(minutes=5)
    progress=workbench.context()['demand_windows']['1']
    assert progress['covered_seconds']==600 and progress['remaining_seconds']==300
    assert not progress['ready']
    with ledger.connect() as conn:conn.execute('UPDATE pricing.retail_runs SET logical_end=%s WHERE run_id=%s',(now['cutoff'],run))
    assert workbench.control(StreamRequest(action='slow_district',district=1))['demand_window']==response['demand_window']
    now['cutoff']=target
    assert workbench.context()['demand_windows']['1']['ready']
    now['age']=121
    assert not workbench.context()['demand_windows']['1']['ready']
    now['visible']=False
    missing=workbench.context()['demand_windows']['1']
    assert missing['covered_seconds']==0 and not missing['ready']
    with ledger.connect() as conn:
        control=conn.execute('SELECT control FROM pricing.retail_runs WHERE run_id=%s',(run,)).fetchone()['control']
    assert control['demand_revision']==1
    assert control['district_demand']['1']==.5
    assert control['fast_forward_until']==target.isoformat()


@pytest.mark.integration
def test_inflight_old_demand_batch_is_rejected_after_a_slowdown(ledger,tmp_path):
    from psycopg.types.json import Jsonb
    from pricing_lab.retail_source import RetailSource,make_batch,UncommittedBatchConflict
    run=uuid4()
    with ledger.connect() as conn:
        conn.execute('INSERT INTO pricing.retail_runs(run_id,manifest_digest,logical_end,seed,control) VALUES (%s,%s,%s,200017,%s)',(run,'test',START,Jsonb({'demand_revision':1})))
    source=RetailSource(ledger,tmp_path/'spool');batch=make_batch(run,1,[],START+timedelta(seconds=10))
    with pytest.raises(UncommittedBatchConflict,match='Demand changed'):
        source.stage(batch,[],expected_demand_revision=0)
    assert source.run(run)['logical_end']==START
    assert source.stage(batch,[],expected_demand_revision=1).sequence==1
