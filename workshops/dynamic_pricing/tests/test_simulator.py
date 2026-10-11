import pytest
from pricing_lab.simulator import simulate
from pricing_lab.model import ModelUnsupported


def test_stock_and_money_conserved_with_overwhelming_demand(golden_snapshot):
    class Demand:
        version='constant-test'
        def predict(self,*args): return 1000.
        def ensemble_predict(self,*args): return [1000.]
    candidates=simulate(golden_snapshot,Demand(),[700],17,128)
    assert {c.price_minor for c in candidates}=={700,800}
    for c in candidates:
        p=c.projection
        assert p['expected_units']==30 and p['expired_units']==0
        assert p['receipts_minor']==p['expected_units']*c.price_minor
        assert p['gross_margin_minor']==p['receipts_minor']-12000
        assert p['net_recovery_minor']==p['receipts_minor']-600
        assert p['full_contribution_minor']==p['net_recovery_minor']-12000
    assert candidates[0].price_minor==800


def test_reproducible_draws_and_second_plan_never_applied(model,golden_snapshot):
    first=simulate(golden_snapshot,model,[700,600],17,1024)
    second=simulate(golden_snapshot,model,[600,700],17,1024)
    assert [(c.price_minor,c.projection) for c in first]==[(c.price_minor,c.projection) for c in second]
    for c in first:
        assert c.projection['expected_units']+c.projection['expired_units']==30
        assert c.snapshot.applied_price_version==1
        assert 'second_window_price_minor' in c.projection
        assert 'demand_recovery_p10_minor' in c.projection
        assert 'parameter_advantage_min_minor' in c.projection


def test_expired_or_unsupported_stock_is_held(model,golden_snapshot):
    with pytest.raises(ModelUnsupported):
        simulate(golden_snapshot.model_copy(update={'inventory_expires_at':golden_snapshot.scenario_cutoff}),model,[700],17,128)
    with pytest.raises(ModelUnsupported): simulate(golden_snapshot,model,[100],17,128)


@pytest.mark.integration
def test_candidates_are_persisted_and_scope_checked(ledger,scope,model,golden_snapshot):
    from pricing_lab.contracts import Scope,WorkshopError
    candidates=simulate(golden_snapshot,model,[700],17,128)
    ledger.store_candidates(candidates)
    assert ledger.get_candidate(candidates[0].candidate_id)==candidates[0]
    with pytest.raises(WorkshopError):
        ledger.get_candidate(candidates[0].candidate_id,Scope(owner_id='other',scenario_id='grocery',generation=1))

def test_stock_cap_is_applied_per_trial_not_to_mean(golden_snapshot):
    from datetime import timedelta
    class Demand:
        version='poisson-test'
        def predict(self,*args): return .5
        def ensemble_predict(self,*args): return [.5]
    snapshot=golden_snapshot.model_copy(update={'stock_units':1,'inventory_expires_at':golden_snapshot.scenario_cutoff+timedelta(hours=1)})
    candidate=simulate(snapshot,Demand(),[],19,10000)[0]
    assert .60<float(candidate.projection['expected_units'])<.66
    assert candidate.projection['expected_units']<1


def test_parameter_sensitivity_changes_winner(manual_artifact,golden_snapshot):
    from datetime import timedelta
    from pricing_lab.model import DemandModel
    from pricing_lab.policy import validate_candidate
    from copy import deepcopy
    artifact=deepcopy(manual_artifact)
    low=artifact['coefficients'].copy();low[-1]=0.
    high=artifact['coefficients'].copy();high[-1]=-4.
    artifact['ensemble']=[low,high]
    s=golden_snapshot.model_copy(update={'inventory_expires_at':golden_snapshot.scenario_cutoff+timedelta(hours=2)})
    candidates=simulate(s,DemandModel(artifact),[700],17,10000)
    markdown=next(c for c in candidates if c.price_minor==700)
    assert markdown.projection['parameter_advantage_min_minor']<0<markdown.projection['parameter_advantage_max_minor']
