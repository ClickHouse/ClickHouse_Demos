"""Held-out synthetic demand evidence; decision validation is a separate gate."""
import numpy as np

from pricing_lab.model import DemandModel
from .train import _matrix,deviance,split_dates


def validate_model(artifact:dict,rows:list[dict],oracle:dict)->dict:
    model=DemandModel(artifact)
    eligible=[r for r in rows if r["open"] and r["single_price"] and r["available_units"]>r["units"]]
    _,_,test=split_dates(eligible)
    x,y,offset=_matrix(test,model.encoding,artifact["references"])
    mean=np.exp(np.einsum("ij,j->i",x,artifact["coefficients"],optimize=False)+offset)
    baseline=np.exp(np.einsum("ij,j->i",x,artifact["no_price_coefficients"],optimize=False)+offset)
    observed=float(y.sum())
    ratio=float(mean.sum()/observed) if observed else None
    groups=[]
    for sku,store in artifact["subjects"]:
        mask=np.asarray([r["sku"]==sku and r["store_id"]==store for r in test])
        count=float(y[mask].sum())
        groups.append({"sku":sku,"store_id":store,"observed_units":count,"rows":int(mask.sum()),
                       "calibration_ratio":float(mean[mask].sum()/count) if count else None,
                       "adequately_populated":count>=500})
    covered=True
    for sku in artifact["encoding"]["skus"]:
        ref=artifact["references"][str(sku)]
        low,high=artifact["supported_price_ratios"][str(sku)]
        permitted=set(range(int(np.ceil(low*ref/50))*50,int(high*ref)+1,50))
        observed_prices={r["offered_price_minor"] for r in test if r["sku"]==sku}
        covered &= permitted<=observed_prices
    score,baseline_score=deviance(y,mean),deviance(y,baseline)
    passed=covered and ratio is not None and .9<=ratio<=1.1 and score<baseline_score
    passed=passed and all(.9<=g["calibration_ratio"]<=1.1 for g in groups if g["adequately_populated"])
    report={"demand_passed":bool(passed),"calibration_ratio":ratio,"test_rows":len(test),
            "test_observed_units":observed,"test_predicted_units":float(mean.sum()),"groups":groups,
            "held_out_grid_covered":bool(covered),"minimum_group_observed_units":500,"test_deviance":score,"no_price_test_deviance":baseline_score,
            "mae":float(np.abs(y-mean).mean()),"synthetic":True,"decision_validation":"pending"}
    if oracle:
        from .decision_validation import validate_decisions
        report.update(validate_decisions(model,oracle))
    return report
