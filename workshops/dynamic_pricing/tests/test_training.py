from copy import deepcopy
from scipy.optimize import OptimizeResult
import numpy as np
import pytest

from pricing.train import split_dates,train_model,export_training_rows
from pricing.validate import validate_model
from generator.generate import generate_dataset


def test_temporal_split_is_60_15_15_and_disjoint(calibration_rows):
    train,validation,test=split_dates(calibration_rows)
    assert len({r["hour"].date() for r in train})==60
    assert len({r["hour"].date() for r in validation})==15
    assert len({r["hour"].date() for r in test})==15
    assert max(r["hour"] for r in train)<min(r["hour"] for r in validation)
    assert max(r["hour"] for r in validation)<min(r["hour"] for r in test)


@pytest.mark.filterwarnings("error::RuntimeWarning")
def test_bounded_fit_has_held_out_evidence(calibration_rows):
    artifact=train_model(calibration_rows,seed=17)
    assert artifact["status"]=="ready"
    assert -.8 > artifact["coefficients"][-1] > -3.
    assert artifact["fit_diagnostics"]["converged"]
    report=validate_model(artifact,calibration_rows,oracle={})
    assert report["demand_passed"]
    assert report["test_deviance"]<report["no_price_test_deviance"]
    assert .9<=report["calibration_ratio"]<=1.1
    assert report["decision_validation"]=="pending"


def test_failed_optimizer_produces_failed_report_not_clipped_model(calibration_rows,monkeypatch):
    monkeypatch.setattr("pricing.train.minimize",lambda *a,**k:OptimizeResult(success=False,message="fit failed",x=np.zeros(17)))
    artifact=train_model(calibration_rows,seed=17)
    assert artifact["status"]=="failed"
    assert "coefficients" not in artifact
    assert not artifact["fit_diagnostics"]["converged"]


@pytest.mark.integration
def test_clickhouse_export_retains_eligible_zero_sale_hours(analytics,ledger,scope,tmp_path):
    from pricing_lab.history import HistoryLoader
    uri=generate_dataset(tmp_path,40000,17,"core").as_uri()
    HistoryLoader(analytics,ledger).load(uri,scope)
    rows=export_training_rows(analytics,ledger,scope,uri)
    assert len(rows)==2880
    assert any(r["units"]==0 for r in rows)
    assert all(r["units"]>=0 and r["units"]<r["available_units"] for r in rows)
    from pricing.prepare import prepare
    artifact,report=prepare(analytics,ledger,scope,uri,tmp_path/'model')
    assert artifact['status']=='ready'
    assert report['demand_passed'] and report['decision_passed']
    assert (tmp_path/'model/model.json').is_file()


def test_failed_preparation_preserves_accepted_artifacts(tmp_path,manual_artifact,monkeypatch):
    import importlib,json
    preparation=importlib.import_module('pricing.prepare')
    accepted=b'previous accepted model\n'
    (tmp_path/'model.json').write_bytes(accepted)
    (tmp_path/'validation.json').write_bytes(b'previous accepted validation\n')
    monkeypatch.setattr(preparation,'export_training_rows',lambda *args:[])
    monkeypatch.setattr(preparation,'train_model',lambda *args:manual_artifact.copy())
    monkeypatch.setattr(preparation,'validate_model',lambda *args:{'demand_passed':False,'decision_passed':False,'decision_validation':'failed'})
    with pytest.raises(ValueError): preparation.prepare(None,None,None,'unused',tmp_path)
    assert (tmp_path/'model.json').read_bytes()==accepted
    assert (tmp_path/'validation.json').read_bytes()==b'previous accepted validation\n'
    assert json.loads((tmp_path/'rejected-validation.json').read_text())['decision_passed'] is False
