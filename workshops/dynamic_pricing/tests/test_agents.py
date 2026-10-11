from types import SimpleNamespace
from uuid import uuid4
import pytest
from pricing_lab.agents import AgentRuntime
from pricing_lab.contracts import WorkshopError


@pytest.fixture
def agent(scope,subject,clock):
    class Ledger:
        def current_scope(self,*args): return scope,clock.scenario_now()
        def current_state(self,*args): return {'stock_units':30,'current_price_minor':800,'applied_price_version':1,'source_versions':{'price':'1'}}
        def last_acknowledged_price(self,*args): return 800
        def get_candidate(self,*args): raise WorkshopError('candidate_missing','Create a fresh proposal.')
        def list_candidates(self,*args): return []
    return AgentRuntime(Ledger(),SimpleNamespace(),SimpleNamespace(),clock,owner_id=scope.owner_id,scenario_id=scope.scenario_id)


def test_agent_cannot_publish_or_override_rules(agent,scope,subject):
    for tool in ('publish_price','approve_and_apply','set_rules','execute_sql'):
        assert agent.dispatch(tool,{'price_minor':1},scope,subject)['status']=='denied'
    assert agent.ledger.last_acknowledged_price(scope,subject)==800


def test_fabricated_selection_and_argument_injection_denied(agent,scope,subject):
    assert agent.dispatch('select_candidate',{'candidate_id':str(uuid4())},scope,subject)['status']=='denied'
    assert agent.dispatch('get_evidence',{'owner_id':'other'},scope,subject)['status']=='denied'


def test_owner_and_generation_cannot_cross(agent,scope,subject):
    for changed in (scope.model_copy(update={'owner_id':'other'}),scope.model_copy(update={'generation':2})):
        assert agent.dispatch('get_evidence',{},changed,subject)['status']=='denied'


def test_missing_provider_is_held_not_claimed_live(agent,scope,subject):
    result=agent.run_episode(scope,subject,'live')
    assert result['status']=='held' and result['reason']=='agent_not_configured'
    assert result['observations']==[]


@pytest.mark.parametrize('error,reason',[(TimeoutError(),'provider_timeout'),(WorkshopError('quota_exhausted','Use replay.'),'quota_exhausted')])
def test_provider_failures_hold_without_price_changes(agent,scope,subject,error,reason):
    class Provider:
        def decide(self,*args): raise error
    agent.provider=Provider()
    result=agent.run_episode(scope,subject,'live')
    assert result['status']=='held' and result['reason']==reason
    assert agent.ledger.last_acknowledged_price(scope,subject)==800


def test_replay_facts_and_provenance(agent,scope,subject):
    result=agent.run_episode(scope,subject,'replay')
    assert result['mode']=='recorded' and result['cloud_verified'] is False
    assert result['episode']['snapshot']['stock_units']==30
    assert 'not a recorded Cloud or agent execution' in result['provenance']


@pytest.mark.integration
def test_live_tools_select_real_candidates_without_applying(ledger,features,model,clock,scope,subject):
    runtime=AgentRuntime(ledger,features,model,clock,owner_id=scope.owner_id,scenario_id=scope.scenario_id)
    proposal=runtime.dispatch('request_candidates',{},scope,subject)
    assert proposal['status']=='observed'
    candidate=next(c for c in proposal['candidates'] if c['price_minor']==650)
    selected=runtime.dispatch('select_candidate',{'candidate_id':candidate['candidate_id']},scope,subject)
    assert selected['status']=='selected' and selected['learner_review_required'] and not selected['applied']
    assert selected['evidence']['policy']['status']=='requires_review'
    assert ledger.last_acknowledged_price(scope,subject)==800
    other=subject.model_copy(update={'sku':102})
    assert runtime.dispatch('select_candidate',{'candidate_id':candidate['candidate_id']},scope,other)['status']=='denied'


def test_provider_cannot_loop_past_tool_budget(agent,scope,subject):
    class Provider:
        def decide(self,*args): return 'get_evidence',{}
    agent.provider=Provider()
    result=agent.run_episode(scope,subject,'live')
    assert result['reason']=='episode_budget_exhausted' and len(result['observations'])==4
