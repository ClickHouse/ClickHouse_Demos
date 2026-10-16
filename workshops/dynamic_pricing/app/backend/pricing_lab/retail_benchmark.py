"""Equal-population retail measurements; absent server evidence stays absent."""
from datetime import UTC,datetime
import math
import time
from .settings import LAB_ROOT

class BenchmarkMismatch(ValueError):pass


def percentile(values,q):
    ordered=sorted(values)
    return ordered[max(0,math.ceil(len(ordered)*q)-1)] if ordered else None


def run_retail_benchmark(analytics,run_id,district_id,cutoff,repetitions=20,*,log_attempts=60):
    if repetitions<20 or repetitions>100 or not 1<=district_id<=20 or cutoff.tzinfo is None or cutoff.second or cutoff.microsecond:
        raise ValueError('Use 20–100 samples, a district, and an exclusive whole UTC minute.')
    db=analytics.database
    context=analytics.query(f'SELECT * FROM {db}.retail_analysis_context WHERE run_id={{run:UUID}}',{'run':run_id})
    if not context or context[0]['cutoff']<cutoff:
        raise ValueError('Wait for a complete acknowledged source boundary covering the cutoff.')
    params=dict(run=run_id,district=district_id,cutoff=cutoff)
    queries={name:(LAB_ROOT/f'benchmarks/queries/retail_{name}.sql').read_text().replace('__DATABASE__',db) for name in ('raw','rollup')}
    warmup=[];samples=[];facts=None
    for iteration in range(repetitions+1):
        for name in (('raw','rollup') if iteration%2==0 else ('rollup','raw')):
            result=analytics.measured_query(queries[name],params)
            if facts is None:facts=result['rows']
            if result['rows']!=facts:raise BenchmarkMismatch('Raw and aggregate results differ at the fixed cutoff; no performance comparison was produced.')
            sample={k:v for k,v in result.items() if k!='rows'}
            sample.update(query=name,server_seconds=None,memory_usage=None)
            (warmup if iteration==0 else samples).append(sample)
    ids=[s['query_id'] for s in samples];logs={};log_error=None
    log_table='system.query_log'
    try:
        clusters=analytics.query("SELECT count() n FROM system.clusters WHERE cluster='default'")
        if clusters and clusters[0]['n']:
            log_table="clusterAllReplicas('default', system.query_log)"
    except Exception:
        pass  # A restricted account can still expose its node-local query log.
    for attempt in range(log_attempts):
        try:
            rows=analytics.query(f'''SELECT query_id,query_duration_ms,read_rows,read_bytes,memory_usage
              FROM {log_table} WHERE type='QueryFinish' AND query_id IN {{ids:Array(String)}}''',{'ids':ids})
            logs.update({r['query_id']:r for r in rows})
        except Exception:
            log_error='Query-log metrics unavailable for this account.';break
        if len(logs)==len(ids):break
        if attempt+1<log_attempts:time.sleep(1)
    for sample in samples:
        log=logs.get(sample['query_id'])
        if log:
            sample.update(server_seconds=log['query_duration_ms']/1000,memory_usage=log['memory_usage'],read_rows=log['read_rows'],read_bytes=log['read_bytes'])
    summary={}
    for name in queries:
        subset=[s for s in samples if s['query']==name]
        server=[s['server_seconds'] for s in subset if s['server_seconds'] is not None]
        complete=len(server)==repetitions
        summary[name]={f'{kind}_p{p}_seconds':percentile(values,p/100) for kind,values in [('client',[s['client_seconds'] for s in subset]),('server',server if complete else [])] for p in (50,95)}
    return dict(schema_version=2,captured_at=datetime.now(UTC).isoformat(),run_id=str(run_id),district_id=district_id,cutoff=cutoff.isoformat(),
        repetitions=repetitions,equal=True,facts=facts,samples=samples,warmup=warmup,summary=summary,
        performance_complete=len(logs)==len(ids),query_log_note=log_error,query_log_scope=log_table,
        query_settings={'use_query_cache':0,'max_execution_time':60},cache={'result_cache':'disabled','storage_cache':'not flushed; one warmup per query'},
        queries=queries,explain={name:analytics.query('EXPLAIN indexes=1 '+sql,params) for name,sql in queries.items()},
        environment={'version':analytics.query('SELECT version() AS version'),'service_sizing':None,'sizing_verified':False},
        active_parts=analytics.query('SELECT table,count() active_parts,sum(bytes_on_disk) bytes_on_disk FROM system.parts WHERE database={db:String} AND active GROUP BY table',{'db':db}),
        dataset=analytics.query(f"SELECT count() rows FROM {db}.retail_sales WHERE source='history'"),
        live=analytics.query(f"SELECT uniqExact(transaction_id)/60. transactions_per_second FROM {db}.retail_sales WHERE run_id={{run:UUID}} AND source='live' AND branch='applied' AND emitted_at>=now()-INTERVAL 60 SECOND",{'run':run_id}))
