"""Maintainer entry point: export aggregated ClickHouse rows and fit offline."""
import argparse
import json
from pathlib import Path

from pricing_lab.analytics import AnalyticsClient
from pricing_lab.contracts import Scope
from pricing_lab.ledger import Ledger
from pricing_lab.settings import Settings
from .train import export_training_rows, train_model
from .validate import validate_model


def prepare(analytics, ledger, scope, manifest_uri, output, seed=17):
    rows = export_training_rows(analytics, ledger, scope, manifest_uri)
    artifact = train_model(rows, seed)
    oracle=json.loads((Path(__file__).resolve().parents[1]/'fixtures/decision_validation.json').read_text())
    report = validate_model(artifact, rows, oracle) if artifact['status'] == 'ready' else artifact
    artifact['validation'] = report
    artifact['decision_validation'] = report.get('decision_validation', 'failed')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    if not report.get('demand_passed') or not report.get('decision_passed'):
        artifact['status']='failed'
        for name,payload in [('rejected-model.json',artifact),('rejected-validation.json',report)]:
            (output/name).write_text(json.dumps(payload,indent=2,sort_keys=True)+'\n')
        raise ValueError('Model validation failed. Inspect rejected-validation.json; the accepted model is unchanged.')
    # Replace each accepted file atomically only after both gates pass.
    for name,payload in [('validation.json',report),('model.json',artifact)]:
        temporary=output/(name+'.tmp')
        temporary.write_text(json.dumps(payload,indent=2,sort_keys=True)+'\n')
        temporary.replace(output/name)
    return artifact, report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--seed', type=int, default=17)
    parser.add_argument('--generation', type=int, default=1)
    args = parser.parse_args()
    settings = Settings.from_env()
    analytics = AnalyticsClient(settings)
    try:
        prepare(analytics, Ledger(settings.postgres_dsn.get_secret_value()),
                Scope(owner_id=settings.owner_id, scenario_id=settings.scenario_id, generation=args.generation),
                args.manifest, args.output, args.seed)
    finally:
        analytics.close()


if __name__ == '__main__':
    main()
