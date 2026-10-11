from datetime import timedelta
import pytest
from pricing_lab.policy import validate_candidate
from pricing_lab.simulator import simulate

@pytest.fixture
def candidate(model,golden_snapshot): return simulate(golden_snapshot,model,[700],17,128)[0]
@pytest.fixture
def trusted(candidate):
    return {**candidate.snapshot.model_dump(),'candidate_ids':[candidate.candidate_id],'model_versions':[candidate.model_version]}
@pytest.fixture
def rules(golden_data): return golden_data['rules']

def test_current_candidate_requires_learner_approval(candidate,trusted,rules,clock):
    assert validate_candidate(candidate,trusted,rules,clock.wall_now()).status in ('eligible','requires_review')

@pytest.mark.parametrize('change,reason',[
    ({'applied_price_version':2},'price_version_changed'),({'stock_units':1},'stock_changed'),
    ({'rule_version':2},'rules_changed'),({'candidate_ids':[]},'candidate_unknown'),
    ({'model_versions':[]},'model_unsupported'),
])
def test_trusted_state_changes_block_application(candidate,trusted,rules,clock,change,reason):
    trusted.update(change)
    result=validate_candidate(candidate,trusted,rules,clock.wall_now())
    assert result.status in ('rejected','held') and reason in result.reason_ids

def test_floor_and_step_are_checked_independently(candidate,trusted,rules,clock):
    result=validate_candidate(candidate.model_copy(update={'price_minor':499}),trusted,rules,clock.wall_now())
    assert result.status=='rejected' and {'below_floor','invalid_price_step'}<=set(result.reason_ids)

def test_expired_lease_and_stale_sources_hold(candidate,trusted,rules,clock):
    assert validate_candidate(candidate,trusted,rules,candidate.expires_at+timedelta(seconds=1)).status=='held'
    altered=candidate.model_copy(update={'snapshot':candidate.snapshot.model_copy(update={'source_ages_seconds':{'inventory':121}})})
    assert 'stale_source' in validate_candidate(altered,trusted,rules,clock.wall_now()).reason_ids

def test_generation_and_subject_cannot_be_fabricated(candidate,trusted,rules,clock):
    trusted['scope']=candidate.snapshot.scope.model_copy(update={'generation':2})
    assert 'scope_changed' in validate_candidate(candidate,trusted,rules,clock.wall_now()).reason_ids
    trusted['subject']=candidate.snapshot.subject.model_copy(update={'sku':102})
    assert 'subject_changed' in validate_candidate(candidate,trusted,rules,clock.wall_now()).reason_ids

def test_negative_teaching_fixtures_cannot_override_policy(candidate,trusted,rules,clock):
    import json
    from pathlib import Path
    fixtures=json.loads((Path(__file__).resolve().parents[1]/'fixtures/negative_candidates.json').read_text())
    for fixture in fixtures:
        altered=candidate.model_copy(update={'price_minor':fixture['price_minor']})
        result=validate_candidate(altered,trusted,rules,clock.wall_now())
        assert result.status=='rejected' and fixture['reason_id'] in result.reason_ids
