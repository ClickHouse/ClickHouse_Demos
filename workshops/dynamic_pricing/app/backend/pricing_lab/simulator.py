"""Two-window stock-capped projections; no price application side effects."""
from datetime import timedelta
from decimal import Decimal
from uuid import uuid4
import numpy as np
from scipy.stats import poisson
from .contracts import Candidate
from .model import ModelUnsupported


def trading_windows(snapshot):
    start, end = snapshot.scenario_cutoff, snapshot.inventory_expires_at
    if end <= start or (end-start).total_seconds()>48*3600 or not snapshot.stock_units:
        raise ModelUnsupported()
    segments=[]
    cursor=start
    while cursor<end:
        boundary=min(end,cursor.replace(minute=0,second=0,microsecond=0)+timedelta(hours=1))
        if 10<=cursor.hour<18:
            segments.append((cursor,(boundary-cursor).total_seconds()/3600))
        cursor=boundary
    if not segments:
        raise ModelUnsupported()
    half=sum(h for _,h in segments)/2
    windows=[[],[]];used=0.
    for start,hours in segments:
        first=min(hours,max(0.,half-used))
        if first: windows[0].append((start,first))
        if first<hours: windows[1].append((start+timedelta(hours=first),hours-first))
        used+=hours
    return windows


def _means(snapshot,model,prices,windows,ensemble=False):
    result={}
    for price in prices:
        per_window=[]
        for segments in windows:
            values=[model.ensemble_predict(snapshot,price,start,hours) if ensemble else [model.predict(snapshot,price,start,hours)] for start,hours in segments]
            total=np.sum(values,axis=0)
            if not np.isfinite(total).all() or np.any(total<0) or np.any(total>1e8):
                raise ModelUnsupported()
            per_window.append(total)
        result[price]=per_window
    return result


def _score(snapshot,prices,means,draws,member=0,fixed_second=None):
    demands={p:[poisson.ppf(draws[w],means[p][w][member]).astype(np.int64) for w in range(2)] for p in prices}
    scored={}
    for price in prices:
        sold1=np.minimum(demands[price][0],snapshot.stock_units)
        remaining=snapshot.stock_units-sold1
        plans=[]
        for next_price in ([fixed_second[price]] if fixed_second else prices):
            sold2=np.minimum(demands[next_price][1],remaining)
            sold=sold1+sold2;waste=snapshot.stock_units-sold
            receipts=sold1*price+sold2*next_price
            recovery=receipts-sold*snapshot.selling_cost_minor-waste*snapshot.disposal_cost_minor
            plans.append((float(recovery.mean()),next_price,sold,waste,receipts,recovery))
        best=max(plans,key=lambda p:(p[0],p[1]==snapshot.current_price_minor,p[1]))
        scored[price]=best
    return scored


def simulate(snapshot,model,prices_minor,seed,trials):
    if not isinstance(trials,int) or not 128<=trials<=200000 or len(prices_minor)>20:
        raise ValueError('Use 128–200,000 trials and at most 20 candidate prices.')
    prices=sorted(set([snapshot.current_price_minor,*prices_minor]))
    if any(type(p) is not int or p<=0 for p in prices): raise ValueError('Prices must be positive integer minor units.')
    windows=trading_windows(snapshot)
    means=_means(snapshot,model,prices,windows)
    draws=np.clip(np.random.default_rng(seed).random((2,trials)),1e-12,1-1e-12)
    results=_score(snapshot,prices,means,draws)
    ensemble_means=_means(snapshot,model,prices,windows,True)
    members=len(ensemble_means[prices[0]][0])
    sensitive=[_score(snapshot,prices,ensemble_means,draws,i) for i in range(members)]
    candidates=[]
    def avg(values): return Decimal(int(values.sum()))/Decimal(trials)
    for price in prices:
        _,next_price,sold,waste,receipts,recovery=results[price]
        advantages=[r[price][0]-r[snapshot.current_price_minor][0] for r in sensitive]
        projection={
            'expected_units':avg(sold),'expired_units':avg(waste),'receipts_minor':avg(receipts),
            'gross_margin_minor':avg(receipts-sold*snapshot.unit_cost_minor),
            'selling_cost_minor':avg(sold*snapshot.selling_cost_minor),
            'disposal_cost_minor':avg(waste*snapshot.disposal_cost_minor),
            'net_recovery_minor':avg(recovery),
            'full_contribution_minor':avg(recovery-snapshot.stock_units*snapshot.unit_cost_minor),
            'second_window_price_minor':Decimal(next_price),'seed':Decimal(seed),'trials':Decimal(trials),
            'demand_recovery_p10_minor':Decimal(str(float(np.quantile(recovery,.1)))),
            'demand_recovery_p90_minor':Decimal(str(float(np.quantile(recovery,.9)))),
            'parameter_advantage_min_minor':Decimal(str(min(advantages))),
            'parameter_advantage_max_minor':Decimal(str(max(advantages))),
        }
        candidates.append(Candidate(candidate_id=uuid4(),snapshot=snapshot,model_version=model.version,
                                    price_minor=price,created_at=snapshot.captured_at,expires_at=snapshot.expires_at,projection=projection))
    return sorted(candidates,key=lambda c:(-c.projection['net_recovery_minor'],c.price_minor!=snapshot.current_price_minor,-c.price_minor))
