from datetime import UTC,datetime
import pytest
from pricing_lab.benchmark import BenchmarkRunner,BenchmarkMismatch,elapsed_seconds,summarize

class QueryAdapter:
    def __init__(self):
        self.rollup_result={'canonical_rows':3,'net_units':4,'receipts_minor':2900,'sold_cost_minor':1600}
        self.calls=[]
    def measured_query(self,sql,parameters):
        self.calls.append((sql,parameters))
        rows=self.rollup_result if 'hist_hour' in sql else {'canonical_rows':3,'net_units':4,'receipts_minor':2900,'sold_cost_minor':1600}
        return {'rows':[rows],'query_id':'fake-query-'+str(len(self.calls)),'client_seconds':.01,'read_rows':3,'read_bytes':100}

@pytest.fixture
def benchmark_runner():
    adapter=QueryAdapter()
    return BenchmarkRunner(adapter,'pricing','hist_raw_test','hist_hour_test',{'sandbox':'learner-1/grocery','sku':101,'store':1,'cutoff':datetime(2026,1,30,tzinfo=UTC)})

def test_latency_comparison_refuses_different_facts(benchmark_runner):
    benchmark_runner.analytics.rollup_result={**benchmark_runner.analytics.rollup_result,'net_units':99}
    with pytest.raises(BenchmarkMismatch): benchmark_runner.run(3)

def test_low_sample_count_has_no_tail_percentile_claim(benchmark_runner):
    result=benchmark_runner.run(3)
    assert result['raw']['summary']['p95_seconds'] is None
    assert result['raw']['summary']['tail_label']=='insufficient_samples'
    assert result['facts']['net_units']==4
    assert result['environment']['hardware'] is None
    assert result['cache']['use_query_cache']==0

def test_negative_or_mixed_clock_durations_are_rejected():
    with pytest.raises(ValueError): elapsed_seconds(20,10)
    with pytest.raises(ValueError): elapsed_seconds(datetime(2026,1,1,tzinfo=UTC),10)
    assert elapsed_seconds(10,12)==2

def test_import_and_query_timings_remain_separate(benchmark_runner):
    result=benchmark_runner.run(3,import_measurement={'load_seconds':2.,'rows':1000})
    assert result['import']['rows_per_second']==500
    assert result['raw']['summary']['median_seconds']==.01

def test_every_sample_must_match_fixed_facts(benchmark_runner):
    original=benchmark_runner.analytics.measured_query
    def changed(sql,params):
        row=original(sql,params)
        if len(benchmark_runner.analytics.calls)>4: row['rows']=[{'canonical_rows':3,'net_units':0,'receipts_minor':2900,'sold_cost_minor':1600}]
        return row
    benchmark_runner.analytics.measured_query=changed
    with pytest.raises(BenchmarkMismatch): benchmark_runner.run(3)

@pytest.mark.integration
def test_actual_golden_benchmark_records_query_ids_and_same_totals(features,analytics,ledger,scope,manifest_uri):
    from pricing_lab.benchmark import run_benchmark
    result=run_benchmark(manifest_uri,analytics,3,ledger=ledger)
    assert result['facts']['net_units']==4 and result['facts']['receipts_minor']==2900
    assert result['environment']['clickhouse_version']
    assert result['import']['load_seconds']>0
    assert result['raw']['samples'][0]['query_id']
    assert result['raw']['samples'][0]['read_rows']>=3
    assert result['query_log']['status'] in ('available','not_visible','unavailable')

@pytest.mark.integration
def test_freshness_probe_measures_one_actual_source_write(features,source,ledger,scope,subject,model):
    from pricing_lab.benchmark import run_freshness_probe
    from cdc_utils import replay_all
    class RecordedConnector:
        def create_sale(self,scope,request):
            sale=source.create_sale(scope,request)
            replay_all(features.analytics,ledger,version=2)
            return sale
    before=ledger.current_state(scope,subject)
    result=run_freshness_probe(scope,subject,RecordedConnector(),ledger,features,model)
    assert result['applied'] is False and result['candidates']>0
    assert result['emission_to_candidate_seconds']>0
    assert result['source_transaction_seconds']>=0
    assert result['post_commit_visibility_seconds']>=0
    after=ledger.current_state(scope,subject)
    assert after['stock_units']==before['stock_units']-1
    assert after['current_price_minor']==before['current_price_minor']


def test_measured_query_works_when_cloud_log_queries_is_readonly():
    from pricing_lab.analytics import AnalyticsClient
    from pricing_lab.settings import Settings
    class Result:
        summary={'read_rows':'1','read_bytes':'8'}
        query_id='cloud-query'
        def named_results(self): return [{'count':1}]
    class CloudClient:
        def query(self,sql,parameters,settings):
            if 'log_queries' in settings:
                raise RuntimeError('Setting log_queries is unknown or readonly')
            assert settings['use_query_cache']==0
            return Result()
    analytics=AnalyticsClient(Settings())
    analytics._client=CloudClient()
    result=analytics.measured_query('SELECT count() AS count',{})
    assert result['rows']==[{'count':1}] and result['query_id']=='cloud-query'
