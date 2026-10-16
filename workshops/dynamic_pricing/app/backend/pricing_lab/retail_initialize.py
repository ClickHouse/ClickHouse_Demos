"""Load the prepared artifact and initialize an instance-owned retail run."""
import json
import math
from pathlib import Path
import pyarrow.parquet as pq
from generator.manifest import checked_path,sha256_file
from .retail_schema import install_retail_schema


def install_retail_model(analytics,path):
    path=Path(path)
    manifest=json.loads(path.read_text())
    if manifest.get('schema_version')!='retail-model-v2' or not manifest.get('released'):
        raise ValueError('Use the validated prepared retail model artifact.')
    files={f['key']:f for f in manifest['files']}
    data={}
    for name in ('parameters.parquet','scenarios.parquet'):
        file=checked_path(path.parent,name)
        if sha256_file(file)!=files[name]['sha256']:
            raise ValueError('Prepared model checksum mismatch.')
        data[name]=pq.read_table(file)
    parameters=data['parameters.parquet'].to_pylist();scenarios=data['scenarios.parquet'].to_pylist()
    if {(r['sku'],r['store_group']) for r in parameters}!={(sku,group) for sku in range(101,121) for group in range(4)} or len(parameters)!=80:
        raise ValueError('Prepared model catalog coverage is incomplete.')
    if any(not math.isfinite(r['elasticity']) or not -6<=r['elasticity']<=0 or not 0<r['min_price_ratio']<=r['max_price_ratio']<=1 for r in parameters):
        raise ValueError('Prepared model contains unsupported price response values.')
    if not scenarios or any(not math.isfinite(r[k]) or r[k]<0 for r in scenarios for k in ('weight','multiplier')):
        raise ValueError('Prepared model scenario weights and multipliers must be finite and nonnegative.')
    if any(r['model_version']!=manifest['model_version'] for r in parameters+scenarios) or abs(sum(r['weight'] for r in scenarios)-1)>1e-9:
        raise ValueError('Prepared model versions or scenario weights differ.')
    install_retail_schema(analytics)
    for name,table in [('parameters.parquet','retail_model_parameters'),('scenarios.parquet','retail_demand_scenarios')]:
        existing=analytics.query(f'SELECT * FROM {analytics.database}.{table} ORDER BY ALL')
        expected=data[name].to_pylist()
        canonical=lambda rows:sorted(json.dumps(r,sort_keys=True) for r in rows)
        if existing:
            if canonical(existing)!=canonical(expected):raise ValueError('A different prepared model is installed in this database.')
        else:analytics.client.insert_arrow(f'{analytics.database}.{table}',data[name])
    return manifest['model_version']
