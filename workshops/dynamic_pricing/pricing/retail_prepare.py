"""Prepare immutable, auditable price-response parameters from synthetic exposure."""
import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from scipy.optimize import minimize

from generator.manifest import read_manifest, sha256_file
from pricing_lab.retail_contracts import PriceInput
from .retail_reference import evaluate_reference


def _arrays(rows):
    x = np.array([np.log(r['offered_price_minor'] / r['reference_price_minor']) for r in rows])
    exposure = np.array([r['exposure_hours'] * r['calendar_factor'] for r in rows])
    y = np.array([r['units'] for r in rows], dtype=float)
    return x, exposure, y


def _fit(rows):
    x, exposure, y = _arrays(rows)
    def objective(beta):
        eta = beta[0] + beta[1] * x + np.log(exposure)
        mu = np.exp(eta)
        residual = mu - y
        return float(np.sum(mu - y * eta)), np.array([residual.sum(), (residual * x).sum()])
    fit = minimize(objective, [np.log(max(y.sum() / exposure.sum(), .001)), -1.0],
                   jac=True, method='L-BFGS-B', bounds=[(-12, 12), (-6, 0)])
    if not fit.success or not np.isfinite(fit.x).all():
        raise ValueError('Poisson fit did not converge')
    return fit.x


def _metrics(observed, predicted):
    y, mu = np.array(observed), np.array(predicted)
    return {'observed_units': float(y.sum()), 'predicted_units': float(mu.sum()),
            'mean_absolute_error': float(np.mean(np.abs(y - mu))) if len(y) else None}


def prepare_retail_model(manifest_path: Path, output: Path) -> Path:
    manifest_path, output = Path(manifest_path), Path(output)
    if output.exists() and any(output.iterdir()):
        raise ValueError('Choose an empty model output directory.')
    manifest = read_manifest(manifest_path)
    if manifest['dataset_version'] != 'retail-chain-v2':
        raise ValueError('Use retail-chain-v2 calibration.')
    companions = {f['key']: f for f in manifest['companion_files']}
    for key, meta in companions.items():
        path = manifest_path.parent / key
        if path.parent.resolve() != manifest_path.parent.resolve() or sha256_file(path) != meta['sha256']:
            raise ValueError('Companion checksum or path mismatch.')
    calibration = pq.read_table(manifest_path.parent / 'calibration.parquet').to_pylist()
    products = pq.read_table(manifest_path.parent / 'products.parquet').to_pylist()
    stores = pq.read_table(manifest_path.parent / 'stores.parquet').to_pylist()
    dates = sorted({r['hour'].date() for r in calibration})
    train_end, val_end = dates[int(len(dates) * .6) - 1], dates[int(len(dates) * .8) - 1]
    version = 'retail-model-v2-' + sha256_file(manifest_path)[:12]
    parameters, rejections = [], []
    observed, predicted = {'validation': [], 'test': []}, {'validation': [], 'test': []}
    for product in products:
        for group in sorted({s['store_group'] for s in stores}):
            rows = [r for r in calibration if r['sku'] == product['sku'] and r['store_group'] == group
                    and r['open'] and r['exposure_hours'] > 0 and r['calendar_factor'] > 0
                    and 0 <= r['units'] < r['available_units']]
            train = [r for r in rows if r['hour'].date() <= train_end]
            subsets = {'validation': [r for r in rows if train_end < r['hour'].date() <= val_end],
                       'test': [r for r in rows if r['hour'].date() > val_end]}
            reason = None
            if len(train) < 40 or len({r['offered_price_minor'] for r in train}) < 3 or any(len(v) < 16 for v in subsets.values()):
                reason = 'Insufficient uncensored exposure or randomized price support.'
            else:
                try:
                    intercept, elasticity = _fit(train)
                except ValueError as exc:
                    reason = str(exc)
            if reason:
                rejections.append({'sku': product['sku'], 'store_group': group, 'reason': reason})
                continue
            supported = sorted({r['offered_price_minor'] for r in train})
            parameters.append({'model_version': version, 'sku': product['sku'], 'store_group': group,
                               'elasticity': float(elasticity), 'reference_rate': float(np.exp(intercept)),
                               'min_price_ratio': min(supported) / product['reference_price_minor'],
                               'max_price_ratio': max(supported) / product['reference_price_minor']})
            for name, subset in subsets.items():
                x, exposure, y = _arrays(subset)
                mu = exposure * np.exp(intercept + elasticity * x)
                observed[name].extend(y.tolist())
                predicted[name].extend(mu.tolist())
    if not parameters:
        raise ValueError('No supported model groups; check calibration exposure.')
    fixture = json.loads((Path(__file__).resolve().parents[1] / 'fixtures/retail/scenario.json').read_text())
    fitted = next((p for p in parameters if p['sku'] == 101 and p['store_group'] == 3), parameters[0])
    checks = {}
    for name, stock in [('markdown', 20), ('keep_price', 4)]:
        inputs = PriceInput.model_validate({**fixture['input'], 'stock_units': stock,
                                          'elasticity': str(fitted['elasticity'])})
        best = next(r for r in evaluate_reference(inputs) if r.recommended)
        checks[name] = {'recommended_price_minor': best.price_minor,
                        'incremental_profit_minor': float(best.incremental_profit_minor)}
    output.mkdir(parents=True, exist_ok=True)
    scenarios = [dict(model_version=version, scenario=n, multiplier=m, weight=w)
                 for n, m, w in [('low', .5, .25), ('typical', 1., .5), ('high', 1.5, .25)]]
    files = []
    for name, rows in [('parameters.parquet', parameters), ('scenarios.parquet', scenarios)]:
        path = output / name
        pq.write_table(pa.Table.from_pylist(rows), path, compression='zstd')
        files.append(dict(key=name, sha256=sha256_file(path), rows=len(rows)))
    metrics = {name: _metrics(observed[name], predicted[name]) for name in observed}
    calibration_ok = all(m['observed_units'] > 0 and .9 <= m['predicted_units'] / m['observed_units'] <= 1.1
                         for m in metrics.values())
    report = {'schema_version': 'retail-model-v2', 'model_version': version,
              'preparation_version': '2.0', 'preparation_sha256': sha256_file(Path(__file__)),
              'dataset_sha256': sha256_file(manifest_path), 'synthetic': True,
              'calibration_seed': manifest['seed'] + 100000, 'evaluation_seed': manifest['seed'] + 200000,
              'split': dict(train_end=str(train_end), validation_end=str(val_end), test_end=str(dates[-1]),
                            method='chronological dates, floor(60%)/floor(80%)/100%'),
              'history_rate_scale': manifest['history_rate_scale'],
              'scenario_interpretation': 'Sensitivity assumptions, not confidence intervals.',
              'coverage_groups': len(parameters), 'rejections': rejections, 'files': files,
              'decision_checks': checks, **metrics,
              'released': not rejections and calibration_ok and checks['markdown']['incremental_profit_minor'] > 0
                          and checks['keep_price']['recommended_price_minor'] == 800}
    path = output / 'manifest.json'
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    path = prepare_retail_model(args.manifest, args.output)
    print(path.read_text())


if __name__ == '__main__':
    main()
