#!/usr/bin/env python3
"""Run retail stages only against an explicitly separate rehearsal inventory."""
import argparse,json,time
from datetime import UTC,datetime
from pathlib import Path
from uuid import uuid4
from pricing_lab.settings import Settings
from pricing_lab.analytics import AnalyticsClient
from pricing_lab.ledger import Ledger
from pricing_lab.clock import Clock
from pricing_lab.retail_history import RetailHistory
from pricing_lab.retail_source import RetailSource
from pricing_lab.retail_initialize import install_retail_model
from pricing_lab.retail_schema import install_retail_analysis
from pricing_lab.retail_analytics import RetailAnalytics
from pricing_lab.retail_approval import RetailApprovalService
from pricing_lab.retail_outcomes import OutcomeService
from pricing_lab.retail_contracts import RetailSubject,RetailApproval
from pricing_lab.retail_benchmark import run_retail_benchmark
from pricing_lab.retail_rehearsal import summarize,stage_status

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('stage',choices=['load','initialize','benchmark','decision','outcomes'])
parser.add_argument('--manifest');parser.add_argument('--model',type=Path)
parser.add_argument('--district',type=int,default=1)
parser.add_argument('--receipt');parser.add_argument('--label',choices=['running','paused'],default='running')
parser.add_argument('--output',type=Path,default=Path('reports/retail-rehearsal.json'))
args=parser.parse_args();settings=Settings.from_env()
if not settings.owner_id.startswith('retail-rehearsal-') or 'retail-rehearsal' not in str(settings.resource_inventory_path):
    raise SystemExit('Use a separate retail-rehearsal inventory and private environment; learner resources are excluded.')
report=json.loads(args.output.read_text()) if args.output.exists() else {'checks':{}}
for name in ['automated_checks','dataset_100m','s3_publication','cloud_load','cloud_initialize','cloud_benchmark_running','cloud_benchmark_paused','cloud_decision','cloud_outcomes','managed_agent','managed_clickstack','windows_wsl2','timed_workshop','cleanup']:
    report['checks'].setdefault(name,{'status':'not_run'})
key='cloud_'+args.stage+('_'+args.label if args.stage=='benchmark' else '');started=time.monotonic();a=AnalyticsClient(settings);ledger=Ledger(settings.postgres_dsn.get_secret_value())
try:
    history=RetailHistory(a,ledger)
    if args.stage=='load':
        if not args.manifest or not args.manifest.startswith('https://s3.ap-southeast-1.amazonaws.com/partner-workshop/dynamic-pricing/retail-chain-v2/'):
            raise ValueError('Use the verified immutable retail S3 manifest.')
        manifest,_,_,_=history._manifest(args.manifest)
        count=a.query(f'SELECT count() n FROM {a.database}.retail_sales_console')[0]['n']
        if count==0:
            url=args.manifest.rsplit('/',1)[0]+'/sales-*.parquet'
            columns='event_id,transaction_id,line_number,store_id,district_id,sku,quantity,price_minor,receipts_minor,sold_cost_minor,event_time,original_event_id'
            a.command(f'INSERT INTO {a.database}.retail_sales_console ({columns}) SELECT {columns} FROM s3({{url:String}}, NOSIGN, \'Parquet\')',{'url':url})
        result=history.register_console(args.manifest)
    elif args.stage=='initialize':
        if not args.model or not args.manifest:raise ValueError('Supply verified model and manifest.')
        result={'model':install_retail_model(a,args.model),'run_id':str(RetailSource(ledger,settings.retail_spool).initialize(args.manifest)),'rollup':history.create_rollup()}
        install_retail_analysis(a)
    else:
        context=a.query(f'SELECT * FROM {a.database}.retail_analysis_context LIMIT 1')
        if not context:raise ValueError('No complete sales boundary is visible.')
        ctx=context[0];run=ctx['run_id'];analysis=RetailAnalytics(a)
        if args.stage=='benchmark':result=run_retail_benchmark(a,run,args.district,ctx['cutoff'],20)
        elif args.stage=='decision':
            ready=[r for r in analysis.opportunities(run,args.district,ctx['cutoff'],100) if r['eligibility']=='ready']
            if not ready:raise ValueError('No eligible pricing opportunity at this cutoff.')
            row=ready[0];inputs=analysis.price_inputs(run,RetailSubject(store_id=row['store_id'],sku=row['sku']),ctx['cutoff'])
            selected=next(r for r in analysis.compare(inputs) if r.recommended)
            service=RetailApprovalService(ledger,analysis,Clock(ctx['cutoff']))
            receipt=service.apply(RetailApproval(proposal_id=service.propose(inputs,selected.price_minor),idempotency_key=uuid4()),settings.owner_id)
            result={'inputs':inputs.model_dump(mode='json'),'selected':selected.model_dump(mode='json'),'receipt':receipt.model_dump(mode='json')}
        else:
            if not args.receipt:raise ValueError('Supply the acknowledged receipt UUID.')
            result=OutcomeService(a).series(run,args.receipt)
            if not result:raise ValueError('Observed receipt events are not visible yet.')
    report['checks'][key]={'status':stage_status(args.stage,result),'duration_seconds':time.monotonic()-started,'evidence':result}
except Exception as error:
    report['checks'][key]={'status':'failed','duration_seconds':time.monotonic()-started,'error_type':type(error).__name__}
    print(f'{key} failed: {type(error).__name__}; inspect private diagnostics without publishing credentials.')
finally:
    a.close();report['captured_at']=datetime.now(UTC).isoformat();report['status']=summarize(report['checks'])
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,default=str,indent=2)+'\n')
print(f'{key}: '+report['checks'][key]['status'])
raise SystemExit(0 if report['checks'][key]['status']=='passed' else 1)
