"""Instance-owned workshop operations with explicit recorded mode."""
import argparse
from datetime import UTC,datetime
import json
from pathlib import Path
import shutil
import subprocess
import re
import sys
from uuid import UUID,uuid4
from .analytics import AnalyticsClient
from .clock import Clock
from .contracts import Scope,WorkshopError
from .features import FeatureService
from .history import HistoryLoader,resolve_manifest
from .ledger import Ledger
from .reports import export_report,EXPORT_ROOT
from .scenarios import ScenarioService,CHECKPOINTS,EVENTS
from .settings import Settings,LAB_ROOT
from .source import SourceStore


def tool_version(argv,major,*,minimum=None,exact=None):
    output=subprocess.run(argv,capture_output=True,text=True,check=True,timeout=10).stdout
    match=re.search(r'(?<![0-9])(\d+)\.(\d+)\.(\d+)',output)
    if not match: return False
    version=tuple(map(int,match.groups()))
    return version[0]==major and (minimum is None or version>=minimum) and (exact is None or version==exact)


def build_preflight(settings,module,check_toolchain=True):
    if module not in ('setup','baseline','cdc','diagnostics'): raise ValueError('Unknown module.')
    checks=[]
    def check(name,fn,action):
        try: passed=bool(fn())
        except Exception: passed=False
        checks.append({'name':name,'passed':passed,'action':None if passed else action})
    check('python',lambda:sys.version_info[:2]==(3,12),'Install Python 3.12 and recreate the lab environment.')
    if check_toolchain:
        check('docker',lambda:shutil.which('docker'),'Install Docker and restart the terminal.')
        check('node',lambda:tool_version(['node','--version'],22,minimum=(22,12,0)), 'Install Node.js 22.12 or newer within major version 22.')
        check('clickhousectl',lambda:tool_version(['clickhousectl','--version'],0,exact=(0,4,2)), 'Install clickhousectl 0.4.2.')
    check('postgres_credentials',lambda:bool(settings.postgres_dsn.get_secret_value()),'Set PRICING_POSTGRES_DSN in app/.env.workshop.')
    check('clickhouse_credentials',lambda:bool(settings.clickhouse_password.get_secret_value()),'Set the ClickHouse service credentials in app/.env.workshop.')
    check('clickhouse_tls',lambda:settings.clickhouse_secure or settings.clickhouse_host in ('localhost','127.0.0.1') or settings.ingestion_mode=='local_replay','Enable TLS for the ClickHouse Cloud endpoint.')
    ledger=Ledger(settings.postgres_dsn.get_secret_value());analytics=AnalyticsClient(settings,request_timeout_seconds=10)
    def pg_schema():
        with ledger.connect() as conn:
            return conn.execute("SELECT to_regclass('pricing.subject_state') AS name").fetchone()['name'] is not None
    if settings.postgres_dsn.get_secret_value(): check('source_schema',pg_schema,'Initialize the pricing source schema with the setup command.')
    if settings.clickhouse_password.get_secret_value(): check('clickhouse_connection',lambda:analytics.query('SELECT 1 AS ok')[0]['ok']==1,'Check the service endpoint, IP access list, TLS, and credentials.')
    scope=Scope(owner_id=settings.owner_id,scenario_id=settings.scenario_id,generation=1)
    if module!='setup':
        check('dataset_manifest',lambda:resolve_manifest(settings.dataset_manifest),'Configure a verified manifest and its read-only S3 access.')
        check('history_import',lambda:HistoryLoader(analytics,ledger).active(scope),'Load and reconcile the historical dataset.')
    if module in ('cdc','diagnostics'):
        features=FeatureService(analytics,ledger,Clock(datetime(2026,1,30,12,tzinfo=UTC)))
        def cdc(): features.configure_views();return True
        check('cdc_canonical_views',cdc,'Configure the Postgres ClickPipe and wait for its source-table snapshot.')
    if module=='diagnostics':
        check('clickstack_tables',lambda:analytics.query("SELECT count() AS n FROM system.tables WHERE database={db:String} AND name={table:String}",{'db':settings.clickstack_database,'table':settings.clickstack_trace_table})[0]['n']>0,'Configure ClickStack and deliver a trace before this module.')
        check('clickstack_delivery',lambda:analytics.query(f"SELECT count() AS n FROM {settings.clickstack_database}.{settings.clickstack_trace_table} WHERE Timestamp>=now()-INTERVAL 30 MINUTE AND SpanAttributes['pricing.owner_id']={{owner:String}}",{'owner':settings.owner_id})[0]['n']>0,'Send an application trace and verify this owner ID in the configured ClickStack source.')
        check('agent_access',lambda:settings.diagnostics_agent_mode=='replay' and (LAB_ROOT/'agent/episodes/near_expiry.json').is_file() or settings.agent_provider=='openai' and bool(settings.agent_model) and bool(settings.agent_api_key.get_secret_value()),'Configure an available agent model and key, or select replay mode.')
        checks.append({'name':'agent_mode','passed':True,'action':None,'mode':settings.diagnostics_agent_mode,'verified_live_access':False})
    analytics.close()
    return {'module':module,'toolchain_scope':'host' if check_toolchain else 'application','mode':'live','ready':all(c['passed'] for c in checks),'checks':checks}


def current_scope(ledger,settings):
    with ledger.connect() as conn:
        row=conn.execute('SELECT * FROM pricing.scenarios WHERE owner_id=%s AND scenario_id=%s',(settings.owner_id,settings.scenario_id)).fetchone()
    if not row: raise WorkshopError('scenario_missing','Initialize this learner instance before running the lab.')
    return Scope(owner_id=settings.owner_id,scenario_id=settings.scenario_id,generation=row['generation']),row['scenario_cutoff']


def main(argv=None):
    parser=argparse.ArgumentParser(prog='workshop');sub=parser.add_subparsers(dest='command',required=True)
    for command in ('initialize','preflight','load-history','register-history','scenario','reset','export-report','benchmark'):
        item=sub.add_parser(command);item.add_argument('--participant');item.add_argument('--offline',action='store_true')
        if command=='preflight': item.add_argument('--module',choices=['setup','baseline','cdc','diagnostics'],default='setup')
        if command=='benchmark': item.add_argument('--repetitions',type=int,default=10);item.add_argument('--probe-freshness',action='store_true')
        if command in ('load-history','register-history','benchmark'): item.add_argument('--manifest',help='Dataset metadata URI; defaults to PRICING_DATASET_MANIFEST.')
        if command=='scenario': item.add_argument('--event',choices=EVENTS,required=True);item.add_argument('--event-id',type=UUID,required=True)
        if command=='reset': item.add_argument('--checkpoint',choices=CHECKPOINTS,required=True);item.add_argument('--request-id',type=UUID)
        if command=='export-report': item.add_argument('--decision',type=UUID,required=True)
    args=parser.parse_args(argv);analytics=None
    try:
        settings=Settings.from_env()
        if args.participant and args.participant!=settings.owner_id:
            raise WorkshopError('participant_mismatch','Use the owner configured for this local instance.')
        if args.offline:
            if args.command=='preflight': result={'mode':'recorded','module':args.module,'ready':True,'label':'Recorded exercise; Cloud readiness is not verified'}
            elif args.command=='reset': result={'mode':'recorded','checkpoint':json.loads((LAB_ROOT/'fixtures/checkpoints'/f'{args.checkpoint}.json').read_text()),'label':'Recorded checkpoint; no cloud state changed'}
            elif args.command=='scenario':
                if args.event!='near_expiry': raise WorkshopError('offline_event_unavailable','Use live mode for this event or select the recorded near_expiry episode.')
                result={'mode':'recorded','event':args.event,'episode':json.loads((LAB_ROOT/'agent/episodes/near_expiry.json').read_text())}
            else: raise WorkshopError('offline_command_unavailable','Use live mode for imports and persisted decision reports.')
        elif args.command=='initialize':
            AnalyticsClient(settings).bootstrap_database()
            ledger=Ledger(settings.administrator_dsn());ledger.migrate()
            try:
                scope,cutoff=current_scope(ledger,settings)
                result={'mode':'live','scope':scope.model_dump(mode='json'),'initialized':False,'preserved_existing_state':True}
            except WorkshopError as error:
                if error.code!='scenario_missing': raise
                from .contracts import Subject
                fixture=json.loads((LAB_ROOT/'fixtures/checkpoints/lab1_baseline.json').read_text())
                scope=Scope(owner_id=settings.owner_id,scenario_id=settings.scenario_id,generation=1)
                ledger.initialize(scope,Subject.model_validate(fixture['subject']),fixture['state'],fixture['rules'],datetime.fromisoformat(fixture['scenario_cutoff']))
                result={'mode':'live','scope':scope.model_dump(mode='json'),'initialized':True}
        elif args.command=='preflight': result=build_preflight(settings,args.module)
        else:
            if args.command in ('load-history','register-history','benchmark'):
                args.manifest=args.manifest or settings.dataset_manifest
                if not args.manifest:
                    raise WorkshopError('dataset_manifest_missing','Set PRICING_DATASET_MANIFEST in app/.env.workshop or pass --manifest.')
            ledger=Ledger(settings.postgres_dsn.get_secret_value());scope,cutoff=current_scope(ledger,settings)
            analytics=AnalyticsClient(settings);clock=Clock(cutoff);features=FeatureService(analytics,ledger,clock)
            if args.command=='load-history': result=HistoryLoader(analytics,ledger).load(args.manifest,scope)
            elif args.command=='register-history': result=HistoryLoader(analytics,ledger).register_console(args.manifest,scope)
            elif args.command=='export-report': result={'path':str(export_report(scope,args.decision,EXPORT_ROOT/f'{args.decision}.json',ledger))}
            elif args.command=='benchmark':
                from .benchmark import run_benchmark,run_freshness_probe
                result=run_benchmark(args.manifest,analytics,args.repetitions,ledger=ledger)
                if args.probe_freshness:
                    from .contracts import Subject
                    from .model import DemandModel
                    features.configure_views()
                    result['freshness']=run_freshness_probe(scope,Subject(sku=101,store_id=1),SourceStore(settings.postgres_dsn.get_secret_value()),ledger,features,DemandModel.from_artifact(settings.model_path))
                output=LAB_ROOT/'benchmarks/results/private'/f'{settings.owner_id}-{uuid4()}.json'
                output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(result,default=str,indent=2));result['saved_path']=str(output)
            else:
                scenarios=ScenarioService(SourceStore(settings.postgres_dsn.get_secret_value()),ledger,features,clock)
                if args.command=='scenario': features.configure_views();result=scenarios.trigger(scope,args.event,args.event_id)
                else:
                    key_path=LAB_ROOT/'.run'/f'reset-{settings.owner_id}-{args.checkpoint}.json'
                    key_path.parent.mkdir(parents=True,exist_ok=True)
                    if args.request_id: key=args.request_id;origin=scope.generation
                    elif key_path.exists():
                        saved=json.loads(key_path.read_text());key=UUID(saved['key']);origin=saved['generation']
                    else: key=uuid4();origin=scope.generation
                    key_path.write_text(json.dumps({'key':str(key),'generation':origin}))
                    print(f'reset_request_id: {key} (reuse this ID for a failed retry)',file=sys.stderr)
                    if args.checkpoint!='lab1_baseline': features.configure_views()
                    restored=scenarios.reset(scope.model_copy(update={'generation':origin}),args.checkpoint,key)
                    result={'scope':restored.model_dump(mode='json'),'reset_request_id':str(key)}
        print(json.dumps(result,default=str,indent=2))
        return 0 if result.get('ready',True) else 1
    except WorkshopError as error:
        print(str(error),file=sys.stderr);return 2
    except Exception:
        print('operation_failed: Check the module preflight and retry. Credentials and raw database errors are omitted.',file=sys.stderr);return 2
    finally:
        if analytics: analytics.close()


if __name__=='__main__': raise SystemExit(main())
