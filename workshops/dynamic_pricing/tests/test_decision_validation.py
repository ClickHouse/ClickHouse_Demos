import json
from pathlib import Path
from pricing.decision_validation import validate_decisions
from pricing_lab.model import DemandModel


def test_development_model_meets_predeclared_synthetic_regret_gate():
    root=Path(__file__).resolve().parents[1]
    model=DemandModel.from_artifact(root/'pricing/artifacts/development/model.json')
    report=validate_decisions(model,json.loads((root/'fixtures/decision_validation.json').read_text()))
    assert report['decision_passed']
    assert all(case['regret_ratio']<=.05 for case in report['decision_cases'])
    assert report['support_passed'] and report['monotonic_passed']
    assert report['insufficient_evidence_held']
