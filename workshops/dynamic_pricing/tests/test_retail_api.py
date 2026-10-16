from datetime import UTC,datetime
from types import SimpleNamespace
from uuid import uuid4
import pytest
from fastapi.testclient import TestClient
from pricing_lab.api import create_app
from pricing_lab.settings import Settings


@pytest.fixture
def retail_client():
    settings=Settings(session_secret='test-session-secret-at-least-32-characters')
    run=uuid4()
    service=SimpleNamespace(run_id=run,context=lambda:{'cutoff':'2026-01-30T08:00:00Z'},
        overview=lambda district:{'rows':[]},opportunities=lambda district,cutoff:[],
        compare=lambda subject,cutoff:{'store_id':subject.store_id},
        propose=lambda body:{'proposal_id':str(uuid4())},apply=lambda request,reviewer:{'price_minor':700},
        outcomes=lambda receipt:[],control=lambda body:{'status':'stopped'})
    with TestClient(create_app(settings,{'retail':service}),base_url=settings.allowed_origin) as client:
        yield client


def session(client):
    assert client.post('/api/session',json={},headers={'Origin':'http://localhost:8080'}).status_code==200


def test_retail_routes_require_session_and_mutation_origin(retail_client):
    assert retail_client.get('/api/retail/context').status_code==401
    session(retail_client)
    assert retail_client.get('/api/retail/context').json()['mode']=='synthetic'
    assert retail_client.post('/api/retail/stream',json={'action':'stop'},headers={'Origin':'https://other.example'}).status_code==403


def test_retail_subject_bounds_and_private_error_redaction(retail_client):
    session(retail_client)
    headers={'Origin':'http://localhost:8080'}
    body={'subject':{'store_id':5000,'sku':101},'cutoff':'2026-01-30T08:00:00Z'}
    response=retail_client.post('/api/retail/compare',json=body,headers=headers)
    assert response.status_code==200 and response.json()['data']['store_id']==5000
    body['subject']['store_id']=5001
    assert retail_client.post('/api/retail/compare',json=body,headers=headers).status_code==422
    retail_client.app.state.services['retail'].context=lambda:(_ for _ in ()).throw(RuntimeError('postgresql://secret:password@private'))
    result=retail_client.get('/api/retail/context')
    assert result.status_code==503 and 'password' not in result.text


from test_retail_analysis import opportunity_case

@pytest.mark.integration
def test_retail_api_compares_and_applies_real_sql(ledger,analytics,opportunity_case,tmp_path):
    from pricing_lab.clock import Clock
    from pricing_lab.retail_api import RetailWorkbench
    from pricing_lab.retail_source import RetailSource
    from pricing_lab.retail_approval import RetailApprovalService
    from pricing_lab.retail_outcomes import OutcomeService
    analysis,run,cutoff=opportunity_case
    batch=analytics.query(f'SELECT batch_id FROM {analytics.database}.retail_batches_cdc LIMIT 1')[0]['batch_id']
    with ledger.connect() as conn:
        conn.execute('INSERT INTO pricing.retail_runs(run_id,manifest_digest,logical_end,seed,sequence) VALUES (%s,%s,%s,200017,1)',(run,'fixture',cutoff))
        conn.execute("INSERT INTO pricing.retail_state VALUES (%s,1,1,101,20,'2026-01-30 10:00:00+00',800,400,20,50,ARRAY[600,700,800],1,1,1)",(run,))
        conn.execute("INSERT INTO pricing.retail_batches(batch_id,run_id,sequence,payload_sha256,event_count,transaction_count,logical_end,created_at,status,acknowledged_at) VALUES (%s,%s,1,%s,2,2,%s,now(),'acknowledged',now())",(batch,run,'0'*64,cutoff))
    source=RetailSource(ledger,tmp_path/'spool')
    workbench=RetailWorkbench(ledger,analytics,source,analysis,RetailApprovalService(ledger,analysis,Clock(cutoff)),OutcomeService(analytics))
    settings=Settings(session_secret='test-session-secret-at-least-32-characters')
    with TestClient(create_app(settings,{'retail':workbench}),base_url=settings.allowed_origin) as client:
        session(client);headers={'Origin':settings.allowed_origin}
        body={'subject':{'store_id':1,'sku':101},'cutoff':cutoff.isoformat()}
        comparison=client.post('/api/retail/compare',json=body,headers=headers)
        assert comparison.status_code==200,comparison.text
        assert comparison.json()['data']['query_id']
        assert next(r for r in comparison.json()['data']['rows'] if r['recommended'])['price_minor']==700
        proposal=client.post('/api/retail/proposals',json=dict(body,price_minor=700),headers=headers)
        assert proposal.status_code==200,proposal.text
        receipt=client.post('/api/retail/apply',json={'proposal_id':proposal.json()['data']['proposal_id'],'idempotency_key':str(uuid4())},headers=headers)
        assert receipt.status_code==200,receipt.text
        assert receipt.json()['data']['price_minor']==700
