import json
import pytest

from pricing_lab.model import DemandModel,ModelInvalid,ModelUnsupported


def test_price_response_uses_fixed_reference(model,golden_snapshot):
    base=model.predict(golden_snapshot,800,golden_snapshot.scenario_cutoff,1.)
    marked=model.predict(golden_snapshot,700,golden_snapshot.scenario_cutoff,1.)
    assert base == pytest.approx(4.)
    assert base <= marked
    with pytest.raises(ModelUnsupported):
        model.predict(golden_snapshot,1,golden_snapshot.scenario_cutoff,1.)


@pytest.mark.parametrize("value",[float("nan"),float("inf")])
def test_nonfinite_artifact_never_suggests_price(manual_artifact,tmp_path,value):
    manual_artifact["coefficients"][0]=value
    path=tmp_path/"bad.json"
    path.write_text(json.dumps(manual_artifact))
    with pytest.raises(ModelInvalid):
        DemandModel.from_artifact(path)


def test_positive_elasticity_or_wrong_schema_is_rejected(manual_artifact,tmp_path):
    for change in ("beta","schema"):
        artifact=json.loads(json.dumps(manual_artifact))
        if change=="beta": artifact["coefficients"][-1]=.2
        else: artifact["feature_schema_hash"]="wrong-schema"
        path=tmp_path/"bad.json"
        path.write_text(json.dumps(artifact))
        with pytest.raises(ModelInvalid): DemandModel.from_artifact(path)


def test_unknown_subject_and_invalid_exposure_hold(model,golden_snapshot):
    wrong=golden_snapshot.model_copy(update={"subject":golden_snapshot.subject.model_copy(update={"sku":999})})
    with pytest.raises(ModelUnsupported): model.predict(wrong,800,wrong.scenario_cutoff,1.)
    with pytest.raises(ModelInvalid): model.predict(golden_snapshot,800,golden_snapshot.scenario_cutoff,float("nan"))


def test_exposure_integrates_complete_supported_hours(model,golden_snapshot):
    assert model.predict(golden_snapshot,800,golden_snapshot.scenario_cutoff,2.5)==pytest.approx(10.)
    assert model.ensemble_predict(golden_snapshot,800,golden_snapshot.scenario_cutoff,1.)==[4.]

@pytest.mark.parametrize('field',['demand_passed','decision_passed'])
def test_explicitly_rejected_validation_cannot_load(manual_artifact,field):
    manual_artifact['validation']={field:False}
    with pytest.raises(ModelInvalid): DemandModel(manual_artifact)
