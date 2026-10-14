"""Typed local API. Installation configuration determines owner and endpoints."""
from typing import Any,Literal
from uuid import UUID
import json
from fastapi import FastAPI,Request,Depends,Query,HTTPException
from fastapi.responses import JSONResponse,FileResponse
from pydantic import Field
from .observability import start_span
from .auth import COOKIE,BodyLimit,check_origin,require_session,issue_session
from .contracts import Contract,Scope,Subject,Snapshot,Candidate,PolicyResult,Receipt,WorkshopError,StaleGeneration,SaleRequest
from .cli import build_preflight
from .policy import validate_candidate
from .reports import export_report,EXPORT_ROOT
from .scenarios import CHECKPOINTS,EVENTS
from .simulator import simulate


class EpisodeAction(Contract):
    mode:Literal['live','replay']
    subject:Subject=Subject(sku=101,store_id=1)

class SubjectAction(Contract): subject:Subject=Subject(sku=101,store_id=1)
class Approval(Contract): idempotency_key:UUID
class Rejection(Contract): note:str=Field(max_length=500)
class Reset(Contract): checkpoint:str=Field(max_length=32);idempotency_key:UUID
class ScenarioAction(Contract): event:str=Field(max_length=32);event_id:UUID
class SessionAction(Contract): pass
class Envelope(Contract):
    scope:Scope|None
    mode:Literal['live','recorded']
    data:dict[str,Any]
class AssessedCandidate(Contract): candidate:Candidate;policy:PolicyResult
class CandidateEnvelope(Contract):
    scope:Scope
    mode:Literal['live','recorded']
    snapshot:Snapshot|None
    candidates:list[AssessedCandidate]
class ApplicationEnvelope(Contract): scope:Scope;mode:Literal['live','recorded'];receipt:Receipt


def create_app(settings,services):
    import asyncio
    from contextlib import asynccontextmanager,suppress
    @asynccontextmanager
    async def lifespan(app):
        worker=services.get('outbox')
        async def deliver_loop():
            while True:
                try:
                    scope,_=await asyncio.to_thread(services['ledger'].current_scope,settings.owner_id,settings.scenario_id)
                    await asyncio.to_thread(worker.drain,scope,5)
                except Exception:
                    pass # The module readiness and outbox counters expose pending delivery.
                await asyncio.sleep(2)
        task=asyncio.create_task(deliver_loop()) if worker and settings.postgres_dsn.get_secret_value() else None
        try: yield
        finally:
            if task:
                task.cancel()
                with suppress(asyncio.CancelledError): await task
            if services.get('shutdown'): services['shutdown']()
    app=FastAPI(title='Dynamic pricing workshop',docs_url=None,redoc_url=None,lifespan=lifespan)
    app.add_middleware(BodyLimit)
    app.state.services=services;app.state.settings=settings
    @app.middleware('http')
    async def safe_dependency_boundary(request,call_next):
        try:
            with start_span('pricing.http',{'pricing.owner_id':settings.owner_id,'pricing.operation':request.method},parent_headers=dict(request.headers)) as span:
                response=await call_next(request)
                span.set_attribute('pricing.http_status',response.status_code)
                return response
        except Exception as error:
            import logging
            logging.getLogger(__name__).warning('Pricing dependency request failed',extra={'error_type':type(error).__name__})
            return JSONResponse({'error':{'code':'dependency_unavailable','action':'Run the module preflight, check the dependency connection, and retry.'}},status_code=503)
    mode=services.get('mode','live')
    def protected(request:Request):
        require_session(request,settings)
        if request.method not in ('GET','HEAD'): check_origin(request,settings)
    def scope_for(request,mutation=False):
        scope,cutoff=services['ledger'].current_scope(settings.owner_id,settings.scenario_id)
        if mutation:
            header=request.headers.get('x-pricing-generation')
            if header!=str(scope.generation): raise StaleGeneration()
        features=services.get('features')
        if hasattr(features,'clock'): features.clock.restore_scenario(cutoff)
        return scope
    def envelope(scope,data): return Envelope(scope=scope,mode=mode,data=data)
    def trusted(scope,subject,candidates):
        state=services['ledger'].current_state(scope,subject)
        return {**state,'subject':subject,'candidate_ids':[c.candidate_id for c in candidates],'model_versions':[services['model'].version]}
    def assess(scope,subject,candidates):
        state=trusted(scope,subject,candidates)
        return [AssessedCandidate(candidate=c,policy=validate_candidate(c,state,state['rules'],services['features'].clock.wall_now())) for c in candidates]

    @app.exception_handler(WorkshopError)
    async def workshop_error(request,error):
        status=503 if error.code in ('application_failed','snapshot_held') else 403 if error.code=='reviewer_forbidden' else 404 if error.code.endswith('_missing') else 409
        return JSONResponse({'error':{'code':error.code,'action':error.action,'reason_ids':getattr(error,'reason_ids',[])}},status_code=status)
    @app.exception_handler(HTTPException)
    async def http_error(request,error):
        detail=error.detail if isinstance(error.detail,dict) else {'code':'request_rejected','action':str(error.detail)}
        return JSONResponse({'error':detail},status_code=error.status_code)
    @app.exception_handler(Exception)
    async def dependency_error(request,error):
        return JSONResponse({'error':{'code':'dependency_unavailable','action':'Run the module preflight, check the dependency connection, and retry.'}},status_code=503)

    @app.get('/api/health')
    def health(): return {'status':'ok'}
    @app.post('/api/session',response_model=Envelope)
    def session(body:SessionAction,request:Request):
        check_origin(request,settings)
        response=JSONResponse(envelope(None,{'owner_id':settings.owner_id,'scenario_id':settings.scenario_id,'model_version':getattr(services.get('model'),'version',None),'model_distribution':getattr(services.get('model'),'artifact',{}).get('distribution','development'),'ingestion_mode':settings.ingestion_mode}).model_dump(mode='json'))
        response.set_cookie(COOKIE,issue_session(settings),httponly=True,samesite='strict',max_age=8*3600,secure=settings.allowed_origin.startswith('https://'),path='/api')
        return response
    @app.get('/api/readiness',response_model=Envelope,dependencies=[Depends(protected)])
    def readiness(module:Literal['setup','baseline','cdc','telemetry','diagnostics']='setup'):
        return envelope(None,build_preflight(settings,module,check_toolchain=False))
    @app.get('/api/baseline',response_model=Envelope,dependencies=[Depends(protected)])
    def baseline(request:Request,sku:int=Query(101,ge=1,le=500),store_id:int=Query(1,ge=1,le=20)):
        scope=scope_for(request);subject=Subject(sku=sku,store_id=store_id)
        return envelope(scope,{'reconciliation':services['features'].reconcile(scope,subject,include_live=False),'label':'Historical reconciliation; excludes live CDC facts'})
    @app.get('/api/freshness',response_model=Envelope,dependencies=[Depends(protected)])
    def freshness(request:Request,sku:int=Query(101,ge=1,le=500),store_id:int=Query(1,ge=1,le=20)):
        scope=scope_for(request);subject=Subject(sku=sku,store_id=store_id);state=services['ledger'].current_state(scope,subject)
        data={k:state[k] for k in ('stock_units','inventory_expires_at','current_price_minor','applied_price_version','source_versions','scenario_cutoff')}
        selected={kind:services['features']._effective(view,scope,subject,state['scenario_cutoff']) for kind,view in [('inventory','inventory_live_current'),('price','price_live_current'),('cost','cost_live_current')]}
        data['source_ages_seconds']={k:max(0.,(state['scenario_cutoff']-r['effective_at']).total_seconds()) if r else None for k,r in selected.items()}
        data['selected_versions']={k:str(r['revision_id']) if k=='inventory' and r else str(r['source_version']) if r else None for k,r in selected.items()}
        return envelope(scope,data)
    @app.post('/api/candidates',response_model=CandidateEnvelope,dependencies=[Depends(protected)])
    def propose(body:SubjectAction,request:Request):
        scope=scope_for(request,True);snapshot=services['features'].capture(scope,body.subject)
        state=services['ledger'].current_state(scope,body.subject);step=state['rules']['price_step_minor']
        if [body.subject.sku,body.subject.store_id] not in services['model'].artifact['subjects']:
            from .model import ModelUnsupported
            raise ModelUnsupported()
        low,high=services['model'].artifact['supported_price_ratios'][str(body.subject.sku)]
        floor=max(state['rules']['floor_minor'],int(__import__('math').ceil(low*snapshot.reference_price_minor/step))*step)
        prices=list(range(floor,min(snapshot.current_price_minor,int(high*snapshot.reference_price_minor))+1,step))
        candidates=simulate(snapshot,services['model'],prices,17,4096)
        services['ledger'].store_candidates(candidates)
        return CandidateEnvelope(scope=scope,mode=mode,snapshot=snapshot,candidates=assess(scope,body.subject,candidates))
    @app.get('/api/candidates',response_model=CandidateEnvelope,dependencies=[Depends(protected)])
    def candidates(request:Request,sku:int=Query(101,ge=1,le=500),store_id:int=Query(1,ge=1,le=20)):
        scope=scope_for(request);subject=Subject(sku=sku,store_id=store_id);items=services['ledger'].list_candidates(scope,subject)
        return CandidateEnvelope(scope=scope,mode=mode,snapshot=items[0].snapshot if items else None,candidates=assess(scope,subject,items))
    @app.post('/api/candidates/{candidate_id}/approve',response_model=ApplicationEnvelope,dependencies=[Depends(protected)])
    def approve(candidate_id:UUID,body:Approval,request:Request):
        scope=scope_for(request,True)
        receipt=services['publisher'].approve_and_apply(scope,candidate_id,settings.owner_id,body.idempotency_key)
        return ApplicationEnvelope(scope=scope,mode=mode,receipt=receipt)
    @app.post('/api/candidates/{candidate_id}/reject',response_model=Envelope,dependencies=[Depends(protected)])
    def reject(candidate_id:UUID,body:Rejection,request:Request):
        scope=scope_for(request,True)
        return envelope(scope,services['publisher'].reject(scope,candidate_id,settings.owner_id,body.note))
    @app.post('/api/reset',response_model=Envelope,dependencies=[Depends(protected)])
    def reset(body:Reset,request:Request):
        scope=scope_for(request)
        header=request.headers.get('x-pricing-generation')
        if header!=str(scope.generation):
            previous=services['ledger'].reset_request_origin(settings.owner_id,settings.scenario_id,body.idempotency_key)
            if not previous or header!=str(previous['from_generation']) or scope.generation!=previous['to_generation'] or body.checkpoint!=previous['checkpoint']: raise StaleGeneration()
            scope=scope.model_copy(update={'generation':previous['from_generation']})
        if body.checkpoint not in CHECKPOINTS: raise WorkshopError('checkpoint_unknown','Choose a checkpoint listed in this workshop.')
        restored=services['scenarios'].reset(scope,body.checkpoint,body.idempotency_key)
        return envelope(restored,{'checkpoint':body.checkpoint,'reset_request_id':str(body.idempotency_key)})
    @app.post('/api/scenarios',response_model=Envelope,dependencies=[Depends(protected)])
    def scenario(body:ScenarioAction,request:Request):
        scope=scope_for(request,True)
        return envelope(scope,services['scenarios'].trigger(scope,body.event,body.event_id))
    @app.post('/api/sales',response_model=Envelope,dependencies=[Depends(protected)])
    def sale(body:SaleRequest,request:Request):
        scope=scope_for(request,True);row=services['source'].create_sale(scope,body)
        return envelope(scope,{k:row[k] for k in ('event_id','inventory_revision_id','quantity','unit_price_minor','applied_price_version','commit_acknowledged_at')})
    @app.get('/api/timeline',response_model=Envelope,dependencies=[Depends(protected)])
    def timeline(request:Request,sku:int=Query(101,ge=1,le=500),store_id:int=Query(1,ge=1,le=20)):
        scope=scope_for(request);subject=Subject(sku=sku,store_id=store_id)
        events=services['ledger'].timeline(scope,subject);cdc_status='visible'
        analytics=services['features'].analytics
        try:
            rows=analytics.query(f"SELECT event_id,unit_price_minor,applied_price_version,_peerdb_synced_at FROM {analytics.database}.sales_live_current WHERE owner_id={{owner:String}} AND scenario_id={{scenario:String}} AND generation={{generation:Int32}} AND sku={{sku:Int32}} AND store_id={{store:Int32}} AND channel='store' ORDER BY _peerdb_synced_at DESC LIMIT 50",{'owner':scope.owner_id,'scenario':scope.scenario_id,'generation':scope.generation,'sku':sku,'store':store_id})
            for row in rows:
                events.append({'kind':'cdc','evidence_id':str(row['event_id']),'at':row['_peerdb_synced_at'],'price_minor':row['unit_price_minor'],'applied_price_version':row['applied_price_version']})
        except Exception: cdc_status='unavailable'
        return envelope(scope,{'events':sorted(events,key=lambda e:e['at']),'cdc_status':cdc_status,'ingestion_mode':settings.ingestion_mode})
    @app.get('/api/decisions/{decision_id}',response_model=Envelope,dependencies=[Depends(protected)])
    def decision(decision_id:UUID,request:Request):
        scope=scope_for(request);data=services['ledger'].get_decision(scope,decision_id)
        receipt=services['ledger'].get_receipt(scope,decision_id)
        return envelope(scope,{**data,'receipt':receipt.model_dump(mode='json') if receipt else None})
    @app.get('/api/reports/{decision_id}',dependencies=[Depends(protected)])
    def report(decision_id:UUID,request:Request,format:Literal['json','markdown']='json'):
        scope=scope_for(request);path=export_report(scope,decision_id,EXPORT_ROOT/f'{decision_id}.json',services['ledger'])
        return FileResponse(path if format=='json' else path.with_suffix('.md'),filename=f'{decision_id}.{ "json" if format=="json" else "md"}')
    @app.get('/api/queries/{name}',response_model=Envelope,dependencies=[Depends(protected)])
    def starter_query(name:str,request:Request):
        if name not in ('history_reconciliation','live_sales'): raise HTTPException(404,detail={'code':'query_unknown','action':'Choose a named starter query from the workshop.'})
        scope=scope_for(request);subject=Subject(sku=101,store_id=1)
        result=services['features'].reconcile(scope,subject) if name=='history_reconciliation' else services['features'].analytics.live_current(scope,subject)
        return envelope(scope,{'query_name':name,'result':result})
    @app.post('/api/agent/episodes',response_model=Envelope,dependencies=[Depends(protected)])
    def episode(body:EpisodeAction,request:Request):
        scope=scope_for(request,True)
        return envelope(scope,services['agent'].run_episode(scope,body.subject,body.mode))
    return app
