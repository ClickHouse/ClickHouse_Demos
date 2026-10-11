"""Compare immutable history at one fixed barrier; never fabricate measurements."""
from datetime import UTC,datetime
import hashlib
import json
import math
from pathlib import Path
import platform
import statistics
import time
from uuid import uuid4
from .analytics import identifier
from .contracts import Subject,SaleRequest,WorkshopError
from .history import HistoryLoader,resolve_manifest
from .ledger import Ledger
from .settings import LAB_ROOT


class BenchmarkMismatch(WorkshopError):
    def __init__(self): super().__init__('benchmark_mismatch','Raw and rollup facts differ at the fixed barrier. Reconcile the import before comparing latency.')


def elapsed_seconds(start,end):
    if not isinstance(start,(int,float)) or not isinstance(end,(int,float)) or not math.isfinite(start) or not math.isfinite(end) or end<start:
        raise ValueError('Use nondecreasing readings from the same monotonic clock.')
    return end-start


def summarize(values):
    ordered=sorted(values)
    return {'samples':len(values),'median_seconds':statistics.median(values),
            'minimum_seconds':min(values),'maximum_seconds':max(values),
            'p95_seconds':ordered[math.ceil(.95*len(values))-1] if len(values)>=30 else None,
            'tail_label':'observed_nearest_rank' if len(values)>=30 else 'insufficient_samples'}


def fingerprint(rows):
    return hashlib.sha256(json.dumps(rows,sort_keys=True,default=str,separators=(',',':')).encode()).hexdigest()


class BenchmarkRunner:
    def __init__(self,analytics,database,raw_table,rollup_table,parameters):
        self.analytics=analytics;self.parameters=parameters
        db=identifier(database)
        common="WHERE sandbox_id={sandbox:String} AND store_id={store:Int32} AND sku={sku:Int32}"
        self.queries={
            'raw':f"SELECT count() AS canonical_rows,sum(quantity) AS net_units,sum(receipts_minor) AS receipts_minor,sum(sold_cost_minor) AS sold_cost_minor FROM {db}.{identifier(raw_table)} {common} AND event_time<{{cutoff:DateTime64(3,'UTC')}}",
            'rollup':f"SELECT sum(row_count) AS canonical_rows,sum(net_units) AS net_units,sum(receipts_minor) AS receipts_minor,sum(sold_cost_minor) AS sold_cost_minor FROM {db}.{identifier(rollup_table)} {common} AND hour<{{cutoff:DateTime64(3,'UTC')}}"}

    def run(self,repetitions,*,import_measurement=None):
        if type(repetitions) is not int or not 3<=repetitions<=100: raise ValueError('Use between 3 and 100 samples per query.')
        results={'raw':[],'rollup':[]};reference=None;facts=None
        # Two warmups per query. Alternate order to reduce a fixed order bias.
        for iteration in range(repetitions+2):
            for kind in (('raw','rollup') if iteration%2==0 else ('rollup','raw')):
                sample=self.analytics.measured_query(self.queries[kind],self.parameters)
                current=fingerprint(sample['rows'])
                if reference is None: reference=current;facts=sample['rows'][0]
                if current!=reference: raise BenchmarkMismatch()
                elapsed_seconds(0,sample['client_seconds'])
                if iteration>=2: results[kind].append({k:v for k,v in sample.items() if k!='rows'})
        measurement=dict(import_measurement) if import_measurement else None
        if measurement:
            elapsed_seconds(0,measurement['load_seconds'])
            measurement['rows_per_second']=measurement['rows']/measurement['load_seconds'] if measurement['load_seconds'] else None
        return {'schema_version':1,'captured_at':datetime.now(UTC).isoformat(),'facts':facts,'facts_fingerprint':reference,
                'fixed_parameters':self.parameters,'queries':self.queries,
                'raw':{'samples':results['raw'],'summary':summarize([s['client_seconds'] for s in results['raw']])},
                'rollup':{'samples':results['rollup'],'summary':summarize([s['client_seconds'] for s in results['rollup']])},
                'cache':{'use_query_cache':0,'warmups_per_query':2,'storage_caches':'not_flushed','query_condition_cache':'server_default'},
                'concurrency':1,'environment':{'hardware':None,'cloud_sizing':None,'sizing_verified':False},
                'import':measurement,'freshness':None}


def query_log(analytics,query_ids,timeout_seconds=10):
    started=time.monotonic();rows=[]
    table="clusterAllReplicas('default',system.query_log)" if analytics.settings.ingestion_mode=='clickpipes' else 'system.query_log'
    while time.monotonic()-started<timeout_seconds:
        try:
            rows=analytics.query(f"SELECT query_id,query_duration_ms,read_rows,read_bytes,memory_usage FROM {table} WHERE type='QueryFinish' AND event_time>=now()-INTERVAL 1 HOUR AND query_id IN {{ids:Array(String)}} LIMIT 200",{'ids':query_ids})
        except Exception: return {'status':'unavailable','rows':[],'action':'Check read access to query logs across the replicas. Client durations and HTTP read counters remain available.'}
        if set(r['query_id'] for r in rows)==set(query_ids): return {'status':'available','rows':rows,'table':table}
        time.sleep(.25)
    return {'status':'not_visible','rows':rows,'table':table,'action':'Query-log rows were not all visible within the bounded wait. Do not fill missing metrics with zeros.'}


def run_benchmark(manifest_path,analytics,repetitions,*,ledger=None):
    settings=analytics.settings;ledger=ledger or Ledger(settings.postgres_dsn.get_secret_value())
    scope,cutoff=ledger.current_scope(settings.owner_id,settings.scenario_id)
    loader=HistoryLoader(analytics,ledger);active=loader.active(scope)
    uri=str(manifest_path)
    if not uri.startswith(('file:','https:','s3:')): uri=Path(uri).resolve().as_uri()
    manifest,_=resolve_manifest(uri)
    if fingerprint(manifest)!=fingerprint(active['manifest']): raise BenchmarkMismatch()
    loader.verify(active['run_id'])
    complete_cutoff=cutoff.replace(minute=0,second=0,microsecond=0)
    result=BenchmarkRunner(analytics,analytics.database,active['raw_table'],active['rollup_table'],
                           {'sandbox':scope.sandbox_id,'sku':101,'store':1,'cutoff':complete_cutoff}).run(repetitions,import_measurement=active.get('measurement'))
    result['barrier']={'import_run_id':str(active['run_id']),'scope':scope.model_dump(),'scenario_cutoff':cutoff.isoformat(),
                       'manifest_sha256':fingerprint(manifest),'dataset_rows':manifest['rows'],'tier':manifest['tier']}
    version=analytics.query('SELECT version() AS version')[0]['version']
    result['environment'].update({'configured_ingestion_mode':settings.ingestion_mode,'cloud_rehearsal_verified':False,
                                  'clickhouse_version':version,'python_version':platform.python_version(),'host_platform':platform.platform(),
                                  'dependency_lock_sha256':hashlib.sha256((LAB_ROOT/'versions.lock').read_bytes()).hexdigest()})
    ids=[sample['query_id'] for kind in ('raw','rollup') for sample in result[kind]['samples']]
    result['query_log']=query_log(analytics,ids)
    return result


def run_freshness_probe(scope,subject,source,ledger,features,model):
    """An explicit synthetic write; all elapsed readings use this process clock."""
    from .simulator import simulate
    state=ledger.current_state(scope,subject)
    request=SaleRequest(event_id=uuid4(),subject=subject,quantity=1,occurred_at=state['scenario_cutoff'],expected_price_version=state['applied_price_version'])
    emission=time.monotonic();sale=source.create_sale(scope,request);commit=time.monotonic()
    visible=features.await_revisions(scope,{'sale':str(sale['event_id']),'inventory':str(sale['inventory_revision_id'])},features.timeout_seconds)
    convergence=time.monotonic();snapshot=features.capture(scope,subject);captured=time.monotonic()
    prices=sorted(set([550,600,650,700,750,800,snapshot.current_price_minor]))
    candidates=simulate(snapshot,model,prices,17,1024);recommended=time.monotonic()
    return {'event_id':str(sale['event_id']),'inventory_revision_id':str(sale['inventory_revision_id']),'snapshot_id':str(snapshot.snapshot_id),
            'source_emitted_at':sale['emitted_at'].isoformat(),'commit_acknowledged_at':sale['commit_acknowledged_at'].isoformat(),'visible_at':visible['visible_at'].isoformat(),
            'source_transaction_seconds':elapsed_seconds(emission,commit),'post_commit_visibility_seconds':elapsed_seconds(commit,convergence),
            'feature_capture_seconds':elapsed_seconds(convergence,captured),'recommendation_seconds':elapsed_seconds(captured,recommended),
            'emission_to_candidate_seconds':elapsed_seconds(emission,recommended),'candidates':len(candidates),'applied':False,
            'ingestion_mode':features.analytics.settings.ingestion_mode}
