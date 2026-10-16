from datetime import UTC,datetime
from uuid import UUID
import pytest
from pricing_lab.retail_benchmark import run_retail_benchmark,BenchmarkMismatch

RUN_ID=UUID(int=1)
CUTOFF=datetime(2026,1,30,8,tzinfo=UTC)

class Client:
    database='pricing'
    rollup_units=2
    missing=False
    def __init__(self): self.ids=[]
    def measured_query(self,sql,params):
        query_id=str(len(self.ids));self.ids.append(query_id)
        return dict(rows=[dict(district_id=1,sku=101,units=self.rollup_units if 'retail_district_minute' in sql else 2,receipts_minor=1600,transactions=2)],query_id=query_id,client_seconds=.123,read_rows=2,read_bytes=64)
    def query(self,sql,params=None):
        if 'system.query_log' in sql:
            return [dict(query_id=q,query_duration_ms=(i+1)*10,read_rows=2,read_bytes=64,memory_usage=128) for i,q in enumerate(params['ids']) if not self.missing or i!=0]
        if 'retail_analysis_context' in sql:return [dict(cutoff=CUTOFF)]
        if 'system.parts' in sql:return [dict(active_parts=3,bytes_on_disk=100)]
        if 'version()' in sql:return [dict(version='test')]
        if 'EXPLAIN' in sql:return [dict(explain='district pruning')]
        return []

def test_mismatch_prevents_speedup_report():
    client=Client();client.rollup_units=999
    with pytest.raises(BenchmarkMismatch):run_retail_benchmark(client,RUN_ID,1,CUTOFF,20,log_attempts=1)

def test_server_milliseconds_and_missing_measurements():
    result=run_retail_benchmark(Client(),RUN_ID,1,CUTOFF,20,log_attempts=1)
    assert [r['server_seconds'] for r in result['samples'][:4]]==[.01,.02,.03,.04]
    assert result['samples'][0]['client_seconds']==.123
    assert result['equal'] and result['performance_complete']
    client=Client();client.missing=True
    result=run_retail_benchmark(client,RUN_ID,1,CUTOFF,20,log_attempts=1)
    assert result['samples'][0]['server_seconds'] is None
    assert result['summary'][result['samples'][0]['query']]['server_p50_seconds'] is None
    assert not result['performance_complete']

@pytest.mark.parametrize('repetitions,cutoff',[(19,CUTOFF),(20,CUTOFF.replace(second=1)),(20,CUTOFF.replace(tzinfo=None))])
def test_reject_incomplete_benchmark_contract(repetitions,cutoff):
    with pytest.raises(ValueError):run_retail_benchmark(Client(),RUN_ID,1,cutoff,repetitions)

from test_retail_analysis import opportunity_case

@pytest.mark.integration
@pytest.mark.parametrize('running',[False,True])
def test_real_raw_rollup_equal_at_cutoff_during_later_ingestion(analytics,opportunity_case,running):
    from pricing_lab.retail_schema import create_rollup_target,rollup_select
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event
    from uuid import uuid4
    _,run,cutoff=opportunity_case
    db=analytics.database
    create_rollup_target(analytics,'retail_district_minute',store=False)
    select,keys=rollup_select(db,store=False)
    analytics.command(f'INSERT INTO {db}.retail_district_minute {select} GROUP BY {keys}')
    analytics.command(f'CREATE MATERIALIZED VIEW {db}.retail_district_minute_mv TO {db}.retail_district_minute AS {select} GROUP BY {keys}')
    stop=Event()
    def ingest():
        # Separate client: clickhouse-connect sessions forbid concurrent queries.
        from pricing_lab.analytics import AnalyticsClient
        writer=AnalyticsClient(analytics.settings)
        try:
            for _ in range(10):
                writer.client.insert(f'{db}.retail_sales',[[uuid4(),uuid4(),1,1,101,1,800,800,400,cutoff,run,'live']],column_names=['event_id','transaction_id','store_id','district_id','sku','quantity','price_minor','receipts_minor','sold_cost_minor','event_time','run_id','source'])
                if stop.wait(.05):break
        finally:writer.close()
    with ThreadPoolExecutor() as pool:
        future=pool.submit(ingest) if running else None
        try:
            result=run_retail_benchmark(analytics,run,1,cutoff,20,log_attempts=1)
            assert result['equal'] and len(result['samples'])==40
            assert result['facts']==[dict(district_id=1,sku=101,units=258,receipts_minor=206400,transactions=34)]
            assert result['active_parts'] and result['explain']['raw']
        finally:
            stop.set()
            if future:future.result()

def test_boundary_must_cover_requested_cutoff():
    from datetime import timedelta
    with pytest.raises(ValueError,match='boundary'):
        run_retail_benchmark(Client(),RUN_ID,1,CUTOFF+timedelta(minutes=1),20)

def test_unavailable_query_log_does_not_invent_server_performance():
    class Restricted(Client):
        def query(self,sql,params=None):
            if 'system.query_log' in sql:raise RuntimeError('permission denied')
            return super().query(sql,params)
    report=run_retail_benchmark(Restricted(),RUN_ID,1,CUTOFF,20,log_attempts=1)
    assert not report['performance_complete']
    assert all(s['server_seconds'] is None for s in report['samples'])


def test_cloud_collects_query_logs_from_every_service_node():
    class Cloud(Client):
        def query(self,sql,params=None):
            if 'system.clusters' in sql:return [{'n':1}]
            if 'system.query_log' in sql:
                rows=super().query(sql,params)
                return rows if 'clusterAllReplicas' in sql else rows[::2]
            return super().query(sql,params)
    report=run_retail_benchmark(Cloud(),RUN_ID,1,CUTOFF,20,log_attempts=1)
    assert report['performance_complete']


def test_cloud_query_log_flush_can_take_more_than_ten_polls(monkeypatch):
    import pricing_lab.retail_benchmark as module
    monkeypatch.setattr(module.time,'sleep',lambda _:None)
    class Delayed(Client):
        attempts=0
        def query(self,sql,params=None):
            if 'system.query_log' in sql:
                self.attempts+=1
                if self.attempts<=10:return []
            return super().query(sql,params)
    assert run_retail_benchmark(Delayed(),RUN_ID,1,CUTOFF)['performance_complete']
