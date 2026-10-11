"""Evaluate fixed development scenarios against the hidden synthetic generator."""
from datetime import datetime,UTC,timedelta
from pathlib import Path
from uuid import uuid4
import json
import numpy as np
from pricing_lab.contracts import Scope,Subject,Snapshot
from pricing_lab.model import ModelUnsupported
from pricing_lab.simulator import simulate,trading_windows,_means,_score
from pricing_lab.settings import LAB_ROOT


class KnownGenerator:
    def predict(self,snapshot,price,start,exposure):
        # Independent data-generator formula, not fitted coefficients.
        rate={101:4.,102:6.}[snapshot.subject.sku]
        reference={101:800,102:1000}[snapshot.subject.sku]
        return exposure*rate*(1+.1*(snapshot.subject.store_id-1))*(1+.025*(start.hour-13))*(1.15 if start.weekday()>=5 else 1)*(price/reference)**-1.8


def validate_decisions(model,oracle):
    if oracle.get('kind')!='predeclared-synthetic-v1': raise ValueError('Use a declared synthetic validation fixture.')
    golden=json.loads((LAB_ROOT/'fixtures/golden/scenario.json').read_text())
    cutoff=datetime.fromisoformat(oracle['date']);now=datetime.now(UTC)
    snapshot=Snapshot(snapshot_id=uuid4(),scope=Scope(owner_id='validation',scenario_id='grocery',generation=1),
        subject=Subject(sku=101,store_id=1),scenario_cutoff=cutoff,captured_at=now,expires_at=now+timedelta(minutes=60),
        source_versions={'inventory':'1','price':'1','cost':'1'},source_ages_seconds={'inventory':0,'price':0,'cost':0},**golden['state'])
    if cutoff.date().isoformat() not in model.artifact['splits']['test']:
        raise ValueError('Decision scenarios must use untouched test dates.')
    prices=oracle['prices_minor'];cases=[]
    for case in oracle['cases']:
        s=snapshot.model_copy(update={'stock_units':case['stock_units'],'inventory_expires_at':cutoff+timedelta(hours=case['hours_to_expiry'])})
        chosen=simulate(s,model,prices,oracle['policy_seed'],oracle['policy_trials'])[0]
        windows=trading_windows(s)
        means=_means(s,KnownGenerator(),prices,windows)
        draws=np.clip(np.random.default_rng(oracle['oracle_seed']).random((2,oracle['oracle_trials'])),1e-12,1-1e-12)
        optimum=_score(s,prices,means,draws)
        best=max(optimum,key=lambda p:(optimum[p][0],p==s.current_price_minor,p))
        fixed={p:int(chosen.projection['second_window_price_minor']) if p==chosen.price_minor else optimum[p][1] for p in prices}
        actual=_score(s,prices,means,draws,fixed_second=fixed)[chosen.price_minor][0]
        regret=max(0.,optimum[best][0]-actual)/max(1.,abs(optimum[best][0]))
        cases.append({'name':case['name'],'proposed_price_minor':chosen.price_minor,
                      'second_window_price_minor':fixed[chosen.price_minor],'oracle_price_minor':best,
                      'oracle_recovery_minor':optimum[best][0],'selected_recovery_minor':actual,'regret_ratio':regret})
    support=True;monotonic=True
    for sku,store in model.artifact['subjects']:
        ref=model.artifact['references'][str(sku)]
        ratios=model.artifact['supported_price_ratios'][str(sku)]
        allowed=range(int(np.ceil(ratios[0]*ref/50))*50,int(ratios[1]*ref)+1,50)
        s=snapshot.model_copy(update={'subject':Subject(sku=sku,store_id=store),'reference_price_minor':ref})
        try:
            predictions=[model.predict(s,p,cutoff,1.) for p in allowed]
            support &= bool(predictions) and all(np.isfinite(v) and v>0 for v in predictions)
            monotonic &= all(a>=b for a,b in zip(predictions,predictions[1:]))
        except ModelUnsupported: support=False
    held=[]
    for s,price in [(snapshot,oracle['insufficient_evidence']['unsupported_price_minor']),
                    (snapshot.model_copy(update={'subject':Subject(sku=oracle['insufficient_evidence']['unknown_sku'],store_id=1)}),800)]:
        try: simulate(s,model,[price],17,128);held.append(False)
        except ModelUnsupported: held.append(True)
    passed=all(c['regret_ratio']<=.05 for c in cases) and support and monotonic and all(held)
    return {'decision_passed':bool(passed),'decision_validation':'passed' if passed else 'failed',
            'decision_cases':cases,'support_passed':bool(support),'monotonic_passed':bool(monotonic),
            'insufficient_evidence_held':all(held),'policy_trials':oracle['policy_trials'],
            'oracle_trials':oracle['oracle_trials'],'oracle_seed':oracle['oracle_seed'],'synthetic':True}
