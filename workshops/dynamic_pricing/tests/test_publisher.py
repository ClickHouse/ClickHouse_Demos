from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from uuid import uuid4
import pytest
from pricing_lab.publisher import MockPublisher
from pricing_lab.simulator import simulate
from pricing_lab.contracts import WorkshopError,SaleRequest

pytestmark=pytest.mark.integration

@pytest.fixture
def candidate(features,scope,subject,model,ledger):
    candidate=next(c for c in simulate(features.capture(scope,subject),model,[650],17,128) if c.price_minor==650)
    ledger.store_candidates([candidate]);return candidate

@pytest.fixture
def publisher(ledger,clock,model): return MockPublisher(ledger,clock,model_versions=[model.version])


def test_double_approval_returns_one_receipt(publisher,scope,candidate):
    key=uuid4()
    first=publisher.approve_and_apply(scope,candidate.candidate_id,scope.owner_id,key)
    second=publisher.approve_and_apply(scope,candidate.candidate_id,scope.owner_id,key)
    assert first==second and publisher.ledger.receipt_count(candidate.candidate_id)==1
    assert publisher.ledger.get_receipt(scope,first.decision_id)==first


def test_two_keys_concurrently_apply_one_candidate_once(publisher,scope,candidate):
    with ThreadPoolExecutor(max_workers=2) as pool:
        results=list(pool.map(lambda key:publisher.approve_and_apply(scope,candidate.candidate_id,scope.owner_id,key),[uuid4(),uuid4()]))
    assert results[0]==results[1]
    assert publisher.ledger.receipt_count(candidate.candidate_id)==1


def test_wrong_owner_and_stale_rules_block_application(publisher,scope,candidate,ledger):
    with pytest.raises(WorkshopError,match='reviewer_forbidden'):
        publisher.approve_and_apply(scope,candidate.candidate_id,'instructor',uuid4())
    with ledger.connect() as conn: conn.execute('UPDATE pricing.subject_state SET rule_version=2')
    with pytest.raises(WorkshopError,match='policy_blocked'):
        publisher.approve_and_apply(scope,candidate.candidate_id,scope.owner_id,uuid4())
    assert ledger.receipt_count(candidate.candidate_id)==0


def test_failed_acknowledgement_preserves_price_and_records_attempt(publisher,scope,candidate,subject,ledger,monkeypatch):
    def fail(*args): raise RuntimeError('publisher unavailable')
    monkeypatch.setattr(publisher,'_apply_transaction',fail)
    with pytest.raises(WorkshopError,match='application_failed'):
        publisher.approve_and_apply(scope,candidate.candidate_id,scope.owner_id,uuid4())
    assert ledger.current_state(scope,subject)['current_price_minor']==800
    assert ledger.receipt_count(candidate.candidate_id)==0
    with ledger.connect() as conn:
        assert conn.execute('SELECT status FROM pricing.application_attempts').fetchone()['status']=='application_failed'
        assert conn.execute('SELECT status FROM pricing.decisions').fetchone()['status']=='approved'


def test_sale_after_application_uses_receipt_price_and_canonical_cdc(publisher,scope,candidate,source,subject,clock,features,analytics,ledger):
    from cdc_utils import replay_all
    receipt=publisher.approve_and_apply(scope,candidate.candidate_id,scope.owner_id,uuid4())
    sale=source.create_sale(scope,SaleRequest(event_id=uuid4(),subject=subject,quantity=1,occurred_at=clock.scenario_now(),expected_price_version=receipt.applied_price_version))
    assert sale['unit_price_minor']==receipt.price_minor
    assert sale['applied_price_version']==receipt.applied_price_version
    replay_all(analytics,ledger)
    assert analytics.live_current(scope,subject)[0]['unit_price_minor']==receipt.price_minor


def test_expired_lease_reset_and_learner_rejection_block(publisher,scope,candidate,ledger,clock):
    publisher.reject(scope,candidate.candidate_id,scope.owner_id,'Keep the current price.')
    with pytest.raises(WorkshopError,match='rejected_by_learner'):
        publisher.approve_and_apply(scope,candidate.candidate_id,scope.owner_id,uuid4())
    with ledger.connect() as conn: conn.execute('UPDATE pricing.scenarios SET generation=2')
    with pytest.raises(WorkshopError,match='generation_changed'):
        publisher.approve_and_apply(scope,candidate.candidate_id,scope.owner_id,uuid4())

def test_rules_changed_after_review_and_expired_lease_cannot_apply(publisher,scope,candidate,ledger,subject,monkeypatch):
    original=publisher._apply_transaction
    def changed(*args):
        with ledger.connect() as conn: conn.execute('UPDATE pricing.subject_state SET rule_version=2')
        return original(*args)
    monkeypatch.setattr(publisher,'_apply_transaction',changed)
    with pytest.raises(WorkshopError,match='policy_blocked'):
        publisher.approve_and_apply(scope,candidate.candidate_id,scope.owner_id,uuid4())
    assert ledger.current_state(scope,subject)['current_price_minor']==800
    assert ledger.receipt_count(candidate.candidate_id)==0


def test_expired_real_lease_cannot_be_reset_by_business_clock(publisher,scope,candidate,clock,monkeypatch):
    monkeypatch.setattr(clock,'_wall',lambda:candidate.expires_at+timedelta(seconds=1))
    with pytest.raises(WorkshopError,match='policy_blocked'):
        publisher.approve_and_apply(scope,candidate.candidate_id,scope.owner_id,uuid4())


def test_ambiguous_analytics_delivery_preserves_receipt_and_retries_logically_once(publisher,scope,candidate,ledger,features,analytics,clock,monkeypatch):
    from pricing_lab.outbox import OutboxWorker
    receipt=publisher.approve_and_apply(scope,candidate.candidate_id,scope.owner_id,uuid4())
    worker=OutboxWorker(ledger,analytics,clock)
    original=worker.analytics.client.insert
    def lost_ack(*args,**kwargs):
        original(*args,**kwargs)
        raise TimeoutError('acknowledgement lost')
    monkeypatch.setattr(worker.analytics.client,'insert',lost_ack)
    try:
        result=worker.drain(scope)
        assert result['failed']>0
        assert ledger.get_receipt(scope,receipt.decision_id)==receipt
        monkeypatch.setattr(worker.analytics.client,'insert',original)
        assert worker.drain(scope)['delivered']>0
        result=analytics.query(f"SELECT count() AS n FROM {analytics.database}.application_events WHERE event_kind='receipt'")
        assert result[0]['n']==1
        with ledger.connect() as conn:
            assert conn.execute('SELECT count(*) AS n FROM pricing.outbox WHERE delivered_at IS NULL').fetchone()['n']==0
    finally: worker.close()

def test_receipt_insert_failure_rolls_back_price_change(publisher,scope,candidate,ledger,subject):
    with ledger.connect() as conn:
        conn.execute("""CREATE FUNCTION pricing.fail_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
          BEGIN RAISE EXCEPTION 'receipt unavailable'; END $$;
          CREATE TRIGGER fail_receipt BEFORE INSERT ON pricing.receipts FOR EACH ROW EXECUTE FUNCTION pricing.fail_receipt();""")
    with pytest.raises(WorkshopError,match='application_failed'):
        publisher.approve_and_apply(scope,candidate.candidate_id,scope.owner_id,uuid4())
    state=ledger.current_state(scope,subject)
    assert state['current_price_minor']==800 and state['applied_price_version']==1
    assert ledger.receipt_count(candidate.candidate_id)==0


def test_idempotency_key_cannot_be_reused_for_another_candidate(publisher,scope,candidate,ledger,model):
    key=uuid4()
    publisher.approve_and_apply(scope,candidate.candidate_id,scope.owner_id,key)
    other=simulate(candidate.snapshot,model,[700],17,128)[0]
    ledger.store_candidates([other])
    with pytest.raises(WorkshopError,match='idempotency_conflict'):
        publisher.approve_and_apply(scope,other.candidate_id,scope.owner_id,key)
