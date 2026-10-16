import json
import math
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from generator.manifest import sha256_file
from generator.retail import generate_retail
from pricing.retail_prepare import prepare_retail_model


def test_preparation_covers_catalog_and_reports_holdout(tmp_path):
    manifest = generate_retail(tmp_path / 'data', 1000)
    path = prepare_retail_model(manifest, tmp_path / 'model')
    report = json.loads(path.read_text())
    assert report['schema_version'] == 'retail-model-v2'
    assert report['released'] is True
    assert report['rejections'] == []
    assert report['split']['train_end'] < report['split']['validation_end'] < report['split']['test_end']
    params = pq.read_table(path.parent / 'parameters.parquet').to_pylist()
    assert len(params) == 80
    assert all(math.isfinite(p['elasticity']) and -6 <= p['elasticity'] <= 0 for p in params)
    assert abs(sum(p['elasticity'] for p in params) / 80 + 2) < .3
    assert .9 < report['test']['predicted_units'] / report['test']['observed_units'] < 1.1
    assert report['test']['mean_absolute_error'] >= 0
    assert report['decision_checks']['markdown']['incremental_profit_minor'] > 0
    assert report['decision_checks']['keep_price']['recommended_price_minor'] == 800
    for f in report['files']:
        assert sha256_file(path.parent / f['key']) == f['sha256']
    scenarios = pq.read_table(path.parent / 'scenarios.parquet').to_pylist()
    assert sum(s['weight'] for s in scenarios) == 1
    assert [s['scenario'] for s in scenarios] == ['low', 'typical', 'high']
    assert report['calibration_seed'] != report['evaluation_seed']
    with pytest.raises(ValueError, match='empty'):
        prepare_retail_model(manifest, path.parent)


def test_censored_or_missing_exposure_is_rejected(tmp_path):
    manifest_path = generate_retail(tmp_path / 'data', 100)
    manifest = json.loads(manifest_path.read_text())
    calibration = manifest_path.parent / 'calibration.parquet'
    rows = pq.read_table(calibration).to_pylist()
    for row in rows:
        if row['sku'] == 101:
            row['exposure_hours'] = 0
        elif row['sku'] == 102:
            row['available_units'] = 0
    pq.write_table(pa.Table.from_pylist(rows), calibration)
    meta = next(f for f in manifest['companion_files'] if f['key'] == calibration.name)
    meta.update(sha256=sha256_file(calibration), bytes=calibration.stat().st_size)
    manifest_path.write_text(json.dumps(manifest))
    report = json.loads(prepare_retail_model(manifest_path, tmp_path / 'model').read_text())
    assert report['released'] is False
    assert {r['sku'] for r in report['rejections']} == {101, 102}
    assert all(r['reason'] for r in report['rejections'])
