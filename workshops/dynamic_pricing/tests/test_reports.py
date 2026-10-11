from pathlib import Path
from uuid import uuid4
import json
import pytest
from pricing_lab.reports import export_report,EXPORT_ROOT
from pricing_lab.publisher import MockPublisher
from pricing_lab.simulator import simulate
from pricing_lab.contracts import WorkshopError

pytestmark=pytest.mark.integration


def test_export_uses_persisted_review_and_receipt_facts(features,scope,subject,model,ledger,clock):
    candidate=simulate(features.capture(scope,subject),model,[650],17,128)[0]
    ledger.store_candidates([candidate])
    receipt=MockPublisher(ledger,clock,[model.version]).approve_and_apply(scope,candidate.candidate_id,scope.owner_id,uuid4())
    destination=EXPORT_ROOT/(str(uuid4())+'.json')
    try:
        path=export_report(scope,receipt.decision_id,destination,ledger=ledger)
        report=json.loads(path.read_text())
        assert report['receipt']==receipt.model_dump(mode='json')
        assert report['reviewer_id']==scope.owner_id
        assert report['simulation_label']=='projected'
        assert report['candidate']['price_minor']==candidate.price_minor
        assert 'postgresql://' not in path.read_text() and 'password' not in path.read_text()
    finally:
        destination.unlink(missing_ok=True);destination.with_suffix('.md').unlink(missing_ok=True)


def test_missing_decision_and_traversal_are_rejected(scope,ledger,tmp_path):
    with pytest.raises(WorkshopError,match='decision_missing'):
        export_report(scope,uuid4(),EXPORT_ROOT/'missing.json',ledger=ledger)
    with pytest.raises(WorkshopError,match='report_path_invalid'):
        export_report(scope,uuid4(),tmp_path/'escape.json',ledger=ledger)
    with pytest.raises(WorkshopError,match='report_path_invalid'):
        export_report(scope,uuid4(),EXPORT_ROOT/'..'/'escape.json',ledger=ledger)
