"""Fit the prepared synthetic model offline from ClickHouse feature rows."""
from datetime import datetime
import hashlib
import json

import numpy as np
import pyarrow.parquet as pq
from scipy.optimize import minimize

from pricing_lab.analytics import identifier
from pricing_lab.history import HistoryLoader,resolve_manifest
from pricing_lab.model import FEATURE_SCHEMA_HASH,design_row,feature_names,ModelInvalid
from pricing_lab.settings import LAB_ROOT


def split_dates(rows:list[dict])->tuple[list[dict],list[dict],list[dict]]:
    dates=sorted({r["hour"].date() for r in rows})
    if len(dates)!=90:
        raise ValueError("Training requires 90 complete business dates.")
    return tuple([r for r in rows if r["hour"].date() in set(period)] for period in (dates[:60],dates[60:75],dates[75:]))


def deviance(observed,mean):
    observed,mean=np.asarray(observed,dtype=float),np.asarray(mean,dtype=float)
    terms=mean.copy()
    nonzero=observed>0
    terms[nonzero]=observed[nonzero]*np.log(observed[nonzero]/mean[nonzero])-(observed[nonzero]-mean[nonzero])
    return float(2*terms.sum())


def _matrix(rows,encoding,references):
    x=np.asarray([design_row(encoding,r["sku"],r["store_id"],r["hour"],r["offered_price_minor"],references[str(r["sku"])]) for r in rows])
    return x,np.asarray([r["units"] for r in rows],dtype=float),np.log([r["exposure_hours"] for r in rows])


def _fit(x,y,offset,penalty,no_price=False,start=None):
    initial=np.zeros(x.shape[1]) if start is None else np.asarray(start).copy()
    if start is None:
        initial[0]=np.log(max(float(y.mean()),1e-6))
        initial[-1]=0 if no_price else -1
    def objective(coef):
        eta=np.einsum("ij,j->i",x,coef,optimize=False)+offset
        if not np.isfinite(eta).all() or np.any(eta>700):
            return np.inf,np.zeros_like(coef)
        mean=np.exp(eta)
        gradient=np.einsum("ij,i->j",x,mean-y,optimize=False)
        gradient[1:]+=2*penalty*coef[1:]
        return float(np.sum(mean-y*eta)+penalty*np.square(coef[1:]).sum()),gradient
    return minimize(objective,initial,method="L-BFGS-B",jac=True,
                    bounds=[(None,None)]*(x.shape[1]-1)+([(0,0)] if no_price else [(None,0)]),
                    options={"maxiter":1000,"ftol":1e-11,"gtol":1e-6})


def train_model(rows:list[dict],seed:int)->dict:
    eligible=[]
    for row in rows:
        if row["units"]<0 or row["offered_price_minor"]<=0 or row["reference_price_minor"]<=0 or not np.isfinite(row["exposure_hours"]) or row["exposure_hours"]<=0:
            raise ValueError("Invalid calibration count, price, or exposure.")
        if row["open"] and row["single_price"] and row["available_units"]>row["units"]:
            eligible.append(row)
    train,validation,test=split_dates(eligible)
    encoding={"skus":sorted({r["sku"] for r in train}),"stores":sorted({r["store_id"] for r in train}),
              "hours":sorted({r["hour"].hour for r in train}),"weekdays":sorted({r["hour"].weekday() for r in train})}
    references={str(s):next(r["reference_price_minor"] for r in train if r["sku"]==s) for s in encoding["skus"]}
    if any(r["reference_price_minor"]!=references.get(str(r["sku"])) for r in eligible):
        raise ValueError("Reference prices must remain fixed and supported.")
    x,y,offset=_matrix(train,encoding,references)
    vx,vy,voffset=_matrix(validation,encoding,references)
    options=[]
    for penalty in (.0001,.01,1.):
        result=_fit(x,y,offset,penalty)
        if result.success and np.isfinite(result.x).all():
            options.append((deviance(vy,np.exp(np.einsum("ij,j->i",vx,result.x,optimize=False)+voffset)),penalty,result))
    if not options:
        return {"status":"failed","fit_diagnostics":{"converged":False,"reason":"optimizer_failed"},"synthetic":True}
    _,penalty,best=min(options,key=lambda item:item[0])
    no_price_options=[]
    for baseline_penalty in (.0001,.01,1.):
        result=_fit(x,y,offset,baseline_penalty,no_price=True)
        if result.success:
            no_price_options.append((deviance(vy,np.exp(np.einsum("ij,j->i",vx,result.x,optimize=False)+voffset)),result))
    if not no_price_options:
        return {"status":"failed","fit_diagnostics":{"converged":False,"reason":"baseline_optimizer_failed"},"synthetic":True}
    baseline=min(no_price_options,key=lambda item:item[0])[1]
    dates=sorted({r["hour"].date() for r in train})
    indices={date:np.asarray([i for i,r in enumerate(train) if r["hour"].date()==date]) for date in dates}
    rng=np.random.default_rng(seed)
    ensemble=[]
    for _ in range(5):
        draws=rng.choice(len(dates),len(dates),replace=True)
        sampled=np.concatenate([indices[dates[int(i)]] for i in draws])
        result=_fit(x[sampled],y[sampled],offset[sampled],penalty,start=best.x)
        if not result.success or not np.isfinite(result.x).all():
            return {"status":"failed","fit_diagnostics":{"converged":False,"reason":"bootstrap_optimizer_failed"},"synthetic":True}
        ensemble.append(result.x.tolist())
    serialized=json.dumps(eligible,sort_keys=True,default=lambda v:v.isoformat() if isinstance(v,datetime) else str(v))
    artifact={"status":"ready","feature_schema_hash":FEATURE_SCHEMA_HASH,"feature_names":feature_names(encoding),
              "encoding":encoding,"references":references,"subjects":sorted({(r["sku"],r["store_id"]) for r in train}),
              "supported_price_ratios":{str(s):[min(r["offered_price_minor"]/references[str(s)] for r in train if r["sku"]==s),
                                                 max(r["offered_price_minor"]/references[str(s)] for r in train if r["sku"]==s)] for s in encoding["skus"]},
              "coefficients":best.x.tolist(),"ensemble":ensemble,"no_price_coefficients":baseline.x.tolist(),
              "fit_diagnostics":{"converged":True,"iterations":int(best.nit),"regularization":penalty},
              "seed":seed,"data_hash":hashlib.sha256(serialized.encode()).hexdigest(),
              "training_sql_hash":hashlib.sha256((LAB_ROOT/"sql/clickhouse/005_training_rows.sql").read_bytes()).hexdigest(),
              "splits":{"train":[d.isoformat() for d in dates],"validation":sorted({r["hour"].date().isoformat() for r in validation}),
                        "test":sorted({r["hour"].date().isoformat() for r in test})},
              "training_rows":len(train),"synthetic":True,"distribution":"development","decision_validation":"pending"}
    artifact["subjects"]=[list(pair) for pair in artifact["subjects"]]
    artifact["model_version"]="poisson-grocery-v1-"+hashlib.sha256(json.dumps(artifact,sort_keys=True).encode()).hexdigest()[:12]
    return artifact


def export_training_rows(analytics,ledger,scope,manifest_uri:str)->list[dict]:
    from pathlib import Path
    manifest,source=resolve_manifest(manifest_uri)
    active=HistoryLoader(analytics,ledger).active(scope)
    name=identifier("availability_"+active["run_id"].hex)
    db=analytics.database
    file=next(f for f in manifest["companion_files"] if f["key"]=="availability.parquet")
    analytics.command(f"""CREATE TABLE IF NOT EXISTS {db}.{name}
      (sku UInt32,store_id UInt16,hour DateTime64(3,'UTC'),available_units UInt32,open Bool,
       exposure_hours Float64,offered_price_minor Int64,reference_price_minor Int64,unit_cost_minor Int64,calibration Bool)
      ENGINE=MergeTree ORDER BY(sku,store_id,hour)""")
    count=analytics.query(f"SELECT count() AS count FROM {db}.{name}")[0]["count"]
    if count==0:
        if isinstance(source,Path):
            analytics.client.insert_arrow(f"{db}.{name}",pq.read_table(source/file["key"]))
        else:
            from urllib.parse import urljoin
            analytics.command(f"INSERT INTO {db}.{name} SELECT * FROM s3({{url:String}},'Parquet')",{"url":urljoin(source,file["key"])})
    if analytics.query(f"SELECT count() AS count FROM {db}.{name}")[0]["count"]!=file["rows"]:
        raise ValueError("Availability import did not reconcile. Start a fresh dataset import.")
    text=(LAB_ROOT/"sql/clickhouse/005_training_rows.sql").read_text()
    for token,value in [("__DATABASE__",db),("__AVAILABILITY__",name),("__RAW__",active["raw_table"])]: text=text.replace(token,identifier(value))
    return analytics.query(text.rstrip().removesuffix(";"))
