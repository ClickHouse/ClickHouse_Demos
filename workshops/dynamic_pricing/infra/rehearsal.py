"""Exercise already provisioned, dedicated Cloud resources through the learner API.

Creation/console wiring, browser inspection, restart, sizing, and pacing remain
separate evidence. This harness must not label a local replay as Cloud delivery.
"""
from datetime import UTC,datetime
import json
import platform
import time
from urllib.parse import urlsplit,parse_qs
from uuid import UUID,uuid4
import httpx
from pricing_lab.analytics import AnalyticsClient
from pricing_lab.clock import Clock
from pricing_lab.contracts import Scope,Subject,WorkshopError
from pricing_lab.features import FeatureService
from pricing_lab.ledger import Ledger,scope_key
from pricing_lab.scenarios import CHECKPOINTS
from .cleanup import cleanup
from .provision import CliRunner,command,check_inventory,owned,verify_pipe_name


def check_rehearsal_settings(settings,inventory):
    check_inventory(inventory,settings)
    if (settings.ingestion_mode!='clickpipes' or not settings.owner_id.startswith('rehearsal-')
        or settings.clickhouse_host in ('localhost','127.0.0.1','::1') or not settings.clickhouse_secure
        or urlsplit(settings.postgres_dsn.get_secret_value()).hostname in (None,'localhost','127.0.0.1','::1')
        or not settings.dataset_manifest.startswith('https://')
        or inventory.pending_creation or not all((inventory.service_id,inventory.postgres_id,inventory.clickpipe_id))):
        raise WorkshopError('rehearsal_scope_invalid','Use dedicated rehearsal-* owned Cloud resources, actual ClickPipes, and a published HTTPS manifest. No replay or incomplete inventory is accepted.')


def check_admin_dsn(settings,dsn):
    source=urlsplit(settings.postgres_dsn.get_secret_value());admin=urlsplit(dsn)
    if (admin.scheme not in ('postgres','postgresql')
        or (admin.hostname,admin.port or 5432,admin.path)!=(source.hostname,source.port or 5432,source.path)
        or not admin.username or admin.username==source.username
        or parse_qs(admin.query).get('sslmode')!=['verify-full']):
        raise WorkshopError('rehearsal_admin_invalid','Use a separate administrator role on the same dedicated source with verified TLS.')


def runtime_platform():
    system=platform.system();release=platform.release().lower()
    if system=='Darwin': return 'macos'
    if system=='Linux' and 'microsoft' in release and 'wsl2' in release: return 'windows-wsl2'
    raise WorkshopError('rehearsal_platform_invalid','Run the host harness from macOS Terminal or Ubuntu on actual Windows WSL 2.')


def rehearse_cloud(settings,inventory,*,admin_dsn,runner=None):
    check_rehearsal_settings(settings,inventory);check_admin_dsn(settings,admin_dsn);host_platform=runtime_platform();runner=runner or CliRunner()
    source_host=urlsplit(settings.postgres_dsn.get_secret_value()).hostname
    for kind,resource in [('service',inventory.service_id),('postgres',inventory.postgres_id)]:
        row=runner.run(command(settings,kind,'get',resource))
        if not owned(row,settings.owner_id,kind): raise WorkshopError('owner_mismatch','Verify dedicated owner tags before rehearsal writes.')
        matches=any(e.get('host')==settings.clickhouse_host for e in row.get('endpoints',[])) if kind=='service' else row.get('hostname')==source_host
        if not matches: raise WorkshopError('rehearsal_endpoint_mismatch','Match the configured source and analytical hosts to the exact saved Cloud resources.')
    pipe=runner.run(command(settings,'clickpipe','get',inventory.service_id,inventory.clickpipe_id))
    verify_pipe_name(pipe,settings.owner_id)
    if str(pipe.get('id'))!=str(inventory.clickpipe_id) or str(pipe.get('state','')).lower()!='running':
        raise WorkshopError('rehearsal_pipe_invalid','Verify the exact owned Running ClickPipe before rehearsal.')
    admin_ledger=Ledger(admin_dsn)
    with admin_ledger.connect() as conn:
        grants=conn.execute("SELECT has_table_privilege(current_user,'pricing.sales_transactions','UPDATE') AND has_table_privilege(current_user,'pricing.sales_transactions','DELETE') AS allowed").fetchone()
        if not grants['allowed']:
            raise WorkshopError('rehearsal_admin_privileges','Use the dedicated source administrator for the controlled correction and deletion checks.')
    ledger=Ledger(settings.postgres_dsn.get_secret_value());analytics=AnalyticsClient(settings)
    scope,cutoff=ledger.current_scope(settings.owner_id,settings.scenario_id)
    features=FeatureService(analytics,ledger,Clock(cutoff));features.configure_views();subject=Subject(sku=101,store_id=1)
    started=time.monotonic();checks=[]
    def passed(name,**evidence): checks.append({'name':name,'passed':True,**evidence})
    try:
        with httpx.Client(base_url=settings.allowed_origin,timeout=settings.convergence_timeout_seconds+15) as client:
            headers={'Origin':settings.allowed_origin}
            def call(method,path,body=None):
                response=client.request(method,path,json=body,headers={**headers,'X-Pricing-Generation':str(scope.generation)})
                if response.status_code!=200: raise WorkshopError('rehearsal_api_failed','Inspect the learner API action and dependency readiness. The Cloud rehearsal has not passed.')
                return response.json()
            session=call('POST','/api/session',{})
            if session['data']['owner_id']!=settings.owner_id or session['data']['ingestion_mode']!='clickpipes':
                raise WorkshopError('rehearsal_api_scope','Use the matching installation owner and live ClickPipes mode.')
            actual=call('GET','/api/freshness')
            trusted=ledger.current_state(scope,subject)
            if actual['scope']!=scope.model_dump(mode='json') or actual['data']['source_versions']!=trusted['source_versions']:
                raise WorkshopError('rehearsal_api_scope','Match the local API to the configured managed Postgres instance before writes.')
            def sale():
                state=ledger.current_state(scope,subject)
                body={'event_id':str(uuid4()),'subject':subject.model_dump(),'quantity':1,'occurred_at':state['scenario_cutoff'].isoformat(),'expected_price_version':state['applied_price_version']}
                row=call('POST','/api/sales',body)['data']
                retry=call('POST','/api/sales',body)['data']
                if row['event_id']!=retry['event_id'] or row['unit_price_minor']!=retry['unit_price_minor']: raise WorkshopError('rehearsal_retry','Source retry did not preserve the committed sale.')
                features.await_revisions(scope,{'sale':row['event_id'],'inventory':row['inventory_revision_id']},features.timeout_seconds)
                return row
            initial=call('POST','/api/candidates',{'subject':subject.model_dump()})
            stale=next((c['candidate'] for c in initial['candidates'] if c['policy']['status'] in ('eligible','requires_review')),None)
            if stale is None: raise WorkshopError('rehearsal_candidate_held','Inspect the initial candidate policy before exercising stale-source rejection.')
            transport=sale();features.capture(scope,subject)
            unchanged=ledger.current_state(scope,subject)['current_price_minor']
            failure=client.post('/api/candidates/'+stale['candidate_id']+'/approve',json={'idempotency_key':str(uuid4())},headers={**headers,'X-Pricing-Generation':str(scope.generation)})
            if failure.status_code!=409 or ledger.current_state(scope,subject)['current_price_minor']!=unchanged:
                raise WorkshopError('rehearsal_stale_application','A changed-source proposal must fail without changing the acknowledged price.')
            passed('changed_source_application_held',candidate_id=stale['candidate_id'])
            passed('source_insert_retry',event_id=transport['event_id'],inventory_revision_id=transport['inventory_revision_id'])
            # Controlled correction/delete only in the dedicated transport generation.
            with admin_ledger.connect() as conn:
                conn.execute('UPDATE pricing.sales_transactions SET unit_price_minor=unit_price_minor-50,receipts_minor=quantity*(unit_price_minor-50) WHERE owner_id=%s AND scenario_id=%s AND generation=%s AND event_id=%s',(*scope_key(scope),UUID(transport['event_id'])))
            features.capture(scope,subject) # full business values must converge, not just UUID visibility
            passed('source_correction_full_value_convergence',event_id=transport['event_id'])
            with admin_ledger.connect() as conn:
                conn.execute('DELETE FROM pricing.sales_transactions WHERE owner_id=%s AND scenario_id=%s AND generation=%s AND event_id=%s',(*scope_key(scope),UUID(transport['event_id'])))
            features.capture(scope,subject)
            passed('source_delete_canonical_filter',event_id=transport['event_id'])
            # Reset discards the transport experiment's business state while retaining evidence.
            reset=call('POST','/api/reset',{'checkpoint':'lab5_review','idempotency_key':str(uuid4())})
            scope=Scope.model_validate(reset['scope']);features.clock.restore_scenario(ledger.current_scope(settings.owner_id,settings.scenario_id)[1])
            proposal=call('POST','/api/candidates',{'subject':subject.model_dump()})
            item=next((c for c in proposal['candidates'] if c['candidate']['price_minor']!=c['candidate']['snapshot']['current_price_minor'] and c['policy']['status'] in ('eligible','requires_review')),None)
            if item is None: raise WorkshopError('rehearsal_candidate_held','Inspect model/support/policy evidence. A held candidate is not a successful demo.')
            candidate=item['candidate'];key=str(uuid4());path='/api/candidates/'+candidate['candidate_id']+'/approve'
            receipt=call('POST',path,{'idempotency_key':key})['receipt']
            duplicate=call('POST',path,{'idempotency_key':key})['receipt']
            if receipt['receipt_id']!=duplicate['receipt_id']: raise WorkshopError('rehearsal_application_retry','Application retry did not preserve its receipt.')
            next_sale=sale();features.capture(scope,subject)
            if (next_sale['unit_price_minor'],next_sale['applied_price_version'])!=(receipt['price_minor'],receipt['applied_price_version']):
                raise WorkshopError('rehearsal_price_mismatch','The subsequent sale does not match the acknowledged receipt.')
            passed('learner_receipt_next_sale',receipt_id=receipt['receipt_id'],decision_id=receipt['decision_id'],price_minor=receipt['price_minor'],event_id=next_sale['event_id'])
            decision=call('GET','/api/decisions/'+receipt['decision_id']);passed('decision_report',decision_id=receipt['decision_id'])
            replay=call('POST','/api/agent/episodes',{'mode':'replay','subject':subject.model_dump()})
            if replay['data']['mode']!='recorded': raise WorkshopError('rehearsal_replay_label','Recorded teaching evidence must stay labeled.')
            passed('recorded_provider_fallback_label',mode='recorded',live_provider_verified=False)
            trace_started=time.monotonic();traces=[]
            while time.monotonic()-trace_started<settings.convergence_timeout_seconds:
                traces=analytics.query(f"SELECT TraceId,SpanId,SpanName FROM {settings.clickstack_database}.{settings.clickstack_trace_table} WHERE Timestamp>=now()-INTERVAL 30 MINUTE AND SpanAttributes['pricing.owner_id']={{owner:String}} AND SpanAttributes['pricing.snapshot_id']={{snapshot:String}} LIMIT 20",{'owner':settings.owner_id,'snapshot':candidate['snapshot']['snapshot_id']})
                if traces: break
                time.sleep(.5)
            if not traces: raise WorkshopError('rehearsal_trace_missing','Verify the actual snapshot ID in ClickStack. Local OTLP smoke evidence cannot replace this gate.')
            passed('clickstack_snapshot_correlation',snapshot_id=candidate['snapshot']['snapshot_id'],trace_ids=sorted({r['TraceId'] for r in traces}))
            resets=[]
            for checkpoint in CHECKPOINTS:
                restored=call('POST','/api/reset',{'checkpoint':checkpoint,'idempotency_key':str(uuid4())})
                scope=Scope.model_validate(restored['scope']);resets.append({'checkpoint':checkpoint,'generation':scope.generation})
            passed('five_generation_resets',resets=resets)
            return {'status':'automated_cloud_checks_passed','captured_at':datetime.now(UTC).isoformat(),'platform':host_platform,
                    'owner_id':settings.owner_id,'resource_ids':{'service':str(inventory.service_id),'postgres':str(inventory.postgres_id),'clickpipe':str(inventory.clickpipe_id)},
                    'checks':checks,'elapsed_seconds':time.monotonic()-started,'decision':decision,'cleanup_preview':cleanup(inventory,runner,settings,False),
                    'pacing_verified':False,'provisioning_duration_seconds':None,'remaining_manual_gates':['fresh-service provisioning timing','browser timeline and keyboard inspection','local edge restart','service sizing and cost','180-minute complete pacing']}
    finally: analytics.close()
