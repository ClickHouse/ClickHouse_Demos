from uuid import uuid4
from types import SimpleNamespace
from fastapi.testclient import TestClient
import pytest
from pricing_lab.api import create_app
from pricing_lab.settings import Settings
from pricing_lab.contracts import WorkshopError


@pytest.fixture
def api_client(scope,clock):
    settings=Settings(session_secret='test-session-secret-at-least-32-characters')
    class Ledger:
        def current_scope(self,owner,scenario): return scope,clock.scenario_now()
    services={'ledger':Ledger(),'source':SimpleNamespace(),'features':SimpleNamespace(),'model':SimpleNamespace(),'publisher':SimpleNamespace(),'scenarios':SimpleNamespace(),'mode':'recorded'}
    with TestClient(create_app(settings,services),base_url=settings.allowed_origin) as client:
        client.services=services
        yield client


def bootstrap(client):
    response=client.post('/api/session',json={},headers={'Origin':'http://localhost:8080'})
    assert response.status_code==200
    assert 'HttpOnly' in response.headers['set-cookie'] and 'SameSite=strict' in response.headers['set-cookie']


def test_session_required_and_origin_checked(api_client):
    assert api_client.post('/api/reset',json={'checkpoint':'lab3_policy','idempotency_key':str(uuid4())},headers={'Origin':'http://localhost:8080'}).status_code==401
    assert api_client.post('/api/session',json={},headers={'Origin':'https://untrusted.example'}).status_code==403


def test_cannot_apply_arbitrary_price_or_owner(api_client):
    bootstrap(api_client)
    response=api_client.post(f'/api/candidates/{uuid4()}/approve',json={'price_minor':1,'owner_id':'other','idempotency_key':str(uuid4())},headers={'Origin':'http://localhost:8080','X-Pricing-Generation':'1'})
    assert response.status_code==422


def test_invalid_uuid_and_body_limits(api_client):
    bootstrap(api_client)
    assert api_client.get('/api/decisions/invalid').status_code==422
    assert api_client.post('/api/scenarios',content='x'*20000,headers={'Origin':'http://localhost:8080'}).status_code==413


def test_dependency_outage_preserves_liveness_and_redacts_errors(api_client):
    api_client.services['ledger'].current_scope=lambda *args:(_ for _ in ()).throw(RuntimeError('postgresql://secret:password@host'))
    assert api_client.get('/api/health').status_code==200
    bootstrap(api_client)
    result=api_client.get('/api/freshness')
    assert result.status_code==503 and 'password' not in result.text


def test_generation_conflict_and_application_failure_are_structured(api_client,scope):
    bootstrap(api_client)
    headers={'Origin':'http://localhost:8080','X-Pricing-Generation':'2'}
    response=api_client.post(f'/api/candidates/{uuid4()}/approve',json={'idempotency_key':str(uuid4())},headers=headers)
    assert response.status_code==409 and response.json()['error']['code']=='generation_changed'
    def failed(*args): raise WorkshopError('application_failed','Refresh the proposal and retry.')
    api_client.services['publisher'].approve_and_apply=failed
    headers['X-Pricing-Generation']='1'
    response=api_client.post(f'/api/candidates/{uuid4()}/approve',json={'idempotency_key':str(uuid4())},headers=headers)
    assert response.status_code==503 and response.json()['error']['action']=='Refresh the proposal and retry.'

@pytest.mark.integration
def test_api_proposal_approval_and_sale_use_actual_database_facts(ledger,source,features,model,clock,scope,subject,analytics):
    from pricing_lab.publisher import MockPublisher
    from pricing_lab.scenarios import ScenarioService
    from cdc_utils import replay_all
    settings=Settings(session_secret='integration-session-secret-at-least-32-characters')
    services={'ledger':ledger,'source':source,'features':features,'model':model,'publisher':MockPublisher(ledger,clock,[model.version]),'scenarios':ScenarioService(source,ledger,features,clock),'mode':'live'}
    with TestClient(create_app(settings,services),base_url=settings.allowed_origin) as client:
        bootstrap(client)
        headers={'Origin':settings.allowed_origin,'X-Pricing-Generation':'1'}
        proposal=client.post('/api/candidates',json={'subject':subject.model_dump()},headers=headers)
        assert proposal.status_code==200
        assert proposal.json()['scope']==scope.model_dump()
        candidate=next(r for r in proposal.json()['candidates'] if r['candidate']['price_minor']==650)
        assert candidate['policy']['status']=='requires_review'
        approved=client.post(f"/api/candidates/{candidate['candidate']['candidate_id']}/approve",json={'idempotency_key':str(uuid4())},headers=headers)
        assert approved.status_code==200
        receipt=approved.json()['receipt']
        sale=client.post('/api/sales',json={'event_id':str(uuid4()),'subject':subject.model_dump(),'quantity':1,'occurred_at':clock.scenario_now().isoformat(),'expected_price_version':receipt['applied_price_version']},headers=headers)
        assert sale.status_code==200 and sale.json()['data']['unit_price_minor']==receipt['price_minor']
        replay_all(analytics,ledger)
        fresh=client.get('/api/freshness')
        assert fresh.status_code==200 and fresh.json()['data']['stock_units']==29
        assert client.get(f"/api/decisions/{receipt['decision_id']}").json()['data']['receipt']==receipt
        assert client.get('/api/queries/arbitrary_sql').status_code==404
        timeline=client.get('/api/timeline')
        assert timeline.status_code==200
        events=timeline.json()['data']['events']
        assert any(e['kind']=='applied' and e['evidence_id']==receipt['receipt_id'] and e['price_minor']==650 for e in events)
        assert any(e['kind']=='sale' and e['applied_price_version']==2 and e['price_minor']==650 for e in events)
        assert any(e['kind']=='cdc' and e['applied_price_version']==2 and e['price_minor']==650 for e in events)

def test_reset_retry_after_lost_ack_returns_same_generation(api_client,scope,clock):
    bootstrap(api_client)
    key=uuid4();current=[scope]
    api_client.services['ledger'].current_scope=lambda *args:(current[0],clock.scenario_now())
    api_client.services['ledger'].reset_request_origin=lambda owner,scenario,request_key: {'from_generation':1,'to_generation':2,'checkpoint':'lab3_policy'} if request_key==key else None
    def reset(old,checkpoint,request_key):
        assert old.generation==1 and request_key==key
        current[0]=scope.model_copy(update={'generation':2});return current[0]
    api_client.services['scenarios'].reset=reset
    headers={'Origin':'http://localhost:8080','X-Pricing-Generation':'1'}
    body={'checkpoint':'lab3_policy','idempotency_key':str(key)}
    assert api_client.post('/api/reset',json=body,headers=headers).status_code==200
    retry=api_client.post('/api/reset',json=body,headers=headers)
    assert retry.status_code==200 and retry.json()['scope']['generation']==2
    body['idempotency_key']=str(uuid4())
    assert api_client.post('/api/reset',json=body,headers=headers).status_code==409


def test_sale_exposes_revision_for_cdc_barrier(api_client,subject,clock):
    bootstrap(api_client)
    event,revision=uuid4(),uuid4()
    api_client.services['source'].create_sale=lambda *args:dict(event_id=event,inventory_revision_id=revision,quantity=1,unit_price_minor=800,applied_price_version=1,commit_acknowledged_at=clock.wall_now())
    response=api_client.post('/api/sales',json={'event_id':str(event),'subject':subject.model_dump(),'quantity':1,'occurred_at':clock.scenario_now().isoformat(),'expected_price_version':1},headers={'Origin':'http://localhost:8080','X-Pricing-Generation':'1'})
    assert response.status_code==200
    assert response.json()['data']['inventory_revision_id']==str(revision)


def test_unsupported_subject_returns_model_hold(api_client,golden_snapshot,model):
    bootstrap(api_client)
    api_client.services['model']=model
    api_client.services['features'].capture=lambda *args:golden_snapshot.model_copy(update={'subject':golden_snapshot.subject.model_copy(update={'sku':999})})
    api_client.services['ledger'].current_state=lambda *args:{'rules':{'price_step_minor':50,'floor_minor':550}}
    result=api_client.post('/api/candidates',json={'subject':{'sku':999,'store_id':1,'channel':'store'}},headers={'Origin':'http://localhost:8080','X-Pricing-Generation':'1'})
    assert result.status_code==409 and result.json()['error']['code']=='model_unsupported'
