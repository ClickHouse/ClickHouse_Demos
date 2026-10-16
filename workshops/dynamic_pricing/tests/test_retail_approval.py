from datetime import UTC,datetime,timedelta
from types import SimpleNamespace
from uuid import uuid4
from concurrent.futures import ThreadPoolExecutor
import pytest
from pricing_lab.retail_approval import RetailApprovalService
from pricing_lab.retail_analytics import RetailAnalytics
from pricing_lab.retail_contracts import PriceInput,RetailApproval
from pricing_lab.clock import Clock
from pricing_lab.contracts import WorkshopError

pytestmark=pytest.mark.integration


@pytest.fixture
def approval_case(ledger,analytics,retail_fixture):
    inputs=PriceInput.model_validate(retail_fixture['input'])
    run=uuid4()
    with ledger.connect() as conn:
        conn.execute('INSERT INTO pricing.retail_runs(run_id,manifest_digest,logical_end,seed) VALUES (%s,%s,%s,200017)',(run,'fixture',inputs.cutoff))
        conn.execute('INSERT INTO pricing.retail_state VALUES (%s,5000,20,101,20,%s,800,400,20,50,ARRAY[600,700,800],1,1,1)',(run,inputs.expires_at))
    analysis=SimpleNamespace(price_inputs=lambda *args:inputs,compare=RetailAnalytics(analytics).compare,
                             model_version=lambda:'retail-fixture-v2')
    now=datetime(2026,10,16,tzinfo=UTC)
    clock=Clock(inputs.cutoff,wall=lambda:now)
    service=RetailApprovalService(ledger,analysis,clock)
    proposal=service.propose(inputs,700)
    request=RetailApproval(proposal_id=proposal,idempotency_key=uuid4())
    return SimpleNamespace(service=service,request=request,ledger=ledger,run=run,analysis=analysis,clock=clock,inputs=inputs)


def test_repeated_and_concurrent_approval_returns_one_receipt(approval_case):
    c=approval_case
    with ThreadPoolExecutor(max_workers=2) as pool:
        receipts=list(pool.map(lambda _:c.service.apply(c.request,'learner-1'),range(2)))
    assert receipts[0]==receipts[1]
    assert c.service.apply(c.request,'learner-1')==receipts[0]
    with c.ledger.connect() as conn:
        assert conn.execute('SELECT count(*) n FROM pricing.retail_receipts').fetchone()['n']==1
        assert conn.execute('SELECT current_price_minor FROM pricing.retail_state').fetchone()['current_price_minor']==700


@pytest.mark.parametrize('change',['stock','price','policy','model','lease','expiry','owner'])
def test_changed_or_unauthorized_proposal_does_not_apply(approval_case,change):
    c=approval_case
    with c.ledger.connect() as conn:
        if change=='stock':conn.execute('UPDATE pricing.retail_state SET stock_units=19,inventory_version=2')
        if change=='price':conn.execute('UPDATE pricing.retail_state SET current_price_minor=750,price_version=2')
        if change=='policy':conn.execute('UPDATE pricing.retail_state SET rule_version=2')
        if change=='expiry':conn.execute('UPDATE pricing.retail_runs SET logical_end=%s',(c.inputs.expires_at,))
    if change=='model': c.analysis.model_version=lambda:'other-model'
    if change=='lease': c.clock._wall=lambda:datetime(2026,10,17,tzinfo=UTC)
    with pytest.raises(WorkshopError):
        c.service.apply(c.request,'other-user' if change=='owner' else 'learner-1')
    with c.ledger.connect() as conn:
        assert conn.execute('SELECT count(*) n FROM pricing.retail_receipts').fetchone()['n']==0


def test_reused_key_for_another_proposal_conflicts(approval_case):
    c=approval_case
    second=c.service.propose(c.inputs,600)
    c.service.apply(c.request,'learner-1')
    with pytest.raises(WorkshopError,match='idempotency_conflict'):
        c.service.apply(RetailApproval(proposal_id=second,idempotency_key=c.request.idempotency_key),'learner-1')


def test_competing_price_approvals_cannot_both_commit(approval_case):
    c=approval_case
    other=RetailApproval(proposal_id=c.service.propose(c.inputs,600),idempotency_key=uuid4())
    def apply(request):
        try:return c.service.apply(request,'learner-1')
        except WorkshopError as error:return error.code
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(apply,[c.request,other]))
    assert sum(not isinstance(r,str) for r in results)==1
    assert 'proposal_changed' in results


def test_lost_response_after_commit_reuses_receipt(approval_case):
    c=approval_case
    committed=c.service.apply(c.request,'learner-1')
    try:
        raise ConnectionError('The response was lost after the transaction committed.')
    except ConnectionError:
        retried=c.service.apply(c.request,'learner-1')
    assert retried==committed


def test_captured_acknowledgment_must_still_be_fresh(approval_case):
    c=approval_case
    inputs=c.inputs.model_copy(update={'sales_sequence':1})
    c.analysis.price_inputs=lambda *args:inputs
    with c.ledger.connect() as conn:
        conn.execute("INSERT INTO pricing.retail_batches(batch_id,run_id,sequence,payload_sha256,event_count,transaction_count,logical_end,created_at,status,acknowledged_at) VALUES (%s,%s,1,%s,1,1,%s,%s,'acknowledged',%s)",(uuid4(),c.run,'0'*64,inputs.cutoff,c.clock.wall_now(),c.clock.wall_now()))
    request=RetailApproval(proposal_id=c.service.propose(inputs,700),idempotency_key=uuid4())
    later=c.clock.wall_now()+timedelta(seconds=180);c.clock._wall=lambda:later
    with pytest.raises(WorkshopError,match='proposal_changed'):
        c.service.apply(request,'learner-1')
