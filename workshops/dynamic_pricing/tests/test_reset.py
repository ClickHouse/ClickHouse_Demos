from concurrent.futures import ThreadPoolExecutor
from threading import Event
from uuid import uuid4
import pytest
from pricing_lab.scenarios import ScenarioService
from pricing_lab.contracts import WorkshopError,StaleGeneration,SaleRequest

pytestmark=pytest.mark.integration

@pytest.fixture
def scenarios(source,ledger,features,clock,analytics,monkeypatch):
    from cdc_utils import replay_all
    original=features.await_revisions
    def replay_then_wait(*args,**kwargs):
        replay_all(analytics,ledger)
        return original(*args,**kwargs)
    monkeypatch.setattr(features,'await_revisions',replay_then_wait)
    return ScenarioService(source,ledger,features,clock)


def test_reset_retries_same_generation_and_retains_import(scenarios,scope,ledger,loader,subject):
    before=loader.active(scope);key=uuid4()
    restored=scenarios.reset(scope,'lab3_policy',key)
    assert restored.generation==2
    assert scenarios.reset(scope,'lab3_policy',key)==restored
    assert loader.active(restored)['run_id']==before['run_id']
    with pytest.raises(StaleGeneration): ledger.current_state(scope,subject)
    assert ledger.current_state(restored,subject)['stock_units']==100
    with ledger.connect() as conn:
        assert conn.execute('SELECT count(*) AS n FROM pricing.candidates WHERE generation=2').fetchone()['n']>=2


def test_unknown_checkpoint_does_not_change_generation(scenarios,scope,ledger,subject):
    with pytest.raises(WorkshopError,match='checkpoint_unknown'):
        scenarios.reset(scope,'../../bad',uuid4())
    assert ledger.current_state(scope,subject)['scope']==scope


def test_reset_waits_for_in_flight_source_transaction(scenarios,scope,source,subject,clock,ledger,monkeypatch):
    entered=Event();release=Event();original=source._record
    def paused(*args,**kwargs):
        entered.set();assert release.wait(5);return original(*args,**kwargs)
    monkeypatch.setattr(source,'_record',paused)
    request=SaleRequest(event_id=uuid4(),subject=subject,quantity=1,occurred_at=clock.scenario_now(),expected_price_version=1)
    with ThreadPoolExecutor(max_workers=2) as pool:
        old=pool.submit(source.create_sale,scope,request);assert entered.wait(5)
        reset=pool.submit(scenarios.reset,scope,'lab2_features',uuid4())
        assert not reset.done();release.set();old.result();restored=reset.result()
    assert ledger.current_state(restored,subject)['stock_units']==30


def test_known_scenario_trigger_is_idempotent(scenarios,scope,ledger,subject):
    key=uuid4()
    first=scenarios.trigger(scope,'near_expiry',key)
    assert scenarios.trigger(scope,'near_expiry',key)==first
    state=ledger.current_state(scope,subject)
    assert (state['inventory_expires_at']-state['scenario_cutoff']).total_seconds()==600
    with pytest.raises(WorkshopError,match='event_conflict'):
        scenarios.trigger(scope,'rule_conflict',key)

def test_old_approval_and_delayed_generation_rows_cannot_affect_reset(scenarios,scope,ledger,features,subject,model,clock,analytics):
    from pricing_lab.publisher import MockPublisher
    from pricing_lab.simulator import simulate
    from cdc_utils import replay_all
    old=simulate(features.capture(scope,subject),model,[650],17,128)[0]
    ledger.store_candidates([old])
    restored=scenarios.reset(scope,'lab3_policy',uuid4())
    with pytest.raises(StaleGeneration): MockPublisher(ledger,clock,[model.version]).approve_and_apply(scope,old.candidate_id,scope.owner_id,uuid4())
    replay_all(analytics,ledger) # Includes retained old-generation facts.
    assert features.capture(restored,subject).stock_units==100


def test_all_checkpoints_restore_known_business_time(scenarios,scope,ledger,subject):
    from pricing_lab.scenarios import CHECKPOINTS
    for checkpoint in CHECKPOINTS:
        scope=scenarios.reset(scope,checkpoint,uuid4())
        assert ledger.current_state(scope,subject)['scenario_cutoff'].isoformat()=='2026-01-30T12:00:00+00:00'


def test_delayed_reset_cdc_does_not_report_readiness_and_retry_reuses_generation(source,ledger,features,clock,scope,analytics,monkeypatch):
    from pricing_lab.features import SnapshotHeld
    from cdc_utils import replay_all
    service=ScenarioService(source,ledger,features,clock)
    monkeypatch.setattr(features,'timeout_seconds',.05)
    key=uuid4()
    with pytest.raises(SnapshotHeld): service.reset(scope,'lab2_features',key)
    replay_all(analytics,ledger)
    assert service.reset(scope,'lab2_features',key).generation==2

def test_every_checkpoint_through_live_cli(ledger,features,analytics,scope,subject,monkeypatch,capsys):
    from cdc_utils import replay_all
    from pricing_lab.cli import main
    from pricing_lab.settings import Settings
    from pricing_lab.features import FeatureService
    settings=analytics.settings.model_copy(update={'postgres_dsn':__import__('pydantic').SecretStr(ledger._dsn)})
    monkeypatch.setattr(Settings,'from_env',classmethod(lambda cls:settings))
    original=FeatureService.await_revisions
    def replay_wait(self,*args,**kwargs):
        replay_all(analytics,ledger)
        return original(self,*args,**kwargs)
    monkeypatch.setattr(FeatureService,'await_revisions',replay_wait)
    from pricing_lab.scenarios import CHECKPOINTS
    from pricing_lab.settings import LAB_ROOT
    try:
        for checkpoint in CHECKPOINTS:
            assert main(['reset','--participant',scope.owner_id,'--checkpoint',checkpoint,'--request-id',str(uuid4())])==0
            assert 'reset_request_id' in capsys.readouterr().out
    finally:
        for checkpoint in CHECKPOINTS:
            (LAB_ROOT/'.run'/f'reset-{scope.owner_id}-{checkpoint}.json').unlink(missing_ok=True)
