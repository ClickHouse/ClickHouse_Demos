import json
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from generator.retail import generate_retail
from generator.manifest import sha256_file
from pricing.retail_prepare import prepare_retail_model
from pricing_lab.retail_initialize import install_retail_model

pytestmark=pytest.mark.integration


def test_prepared_model_installation_is_idempotent(analytics,tmp_path):
    data=generate_retail(tmp_path/'data',100)
    artifact=prepare_retail_model(data,tmp_path/'model')
    version=install_retail_model(analytics,artifact)
    assert install_retail_model(analytics,artifact)==version
    assert analytics.query(f'SELECT count() n FROM {analytics.database}.retail_model_parameters')[0]['n']==80


def test_invalid_scenario_cannot_be_installed(analytics,tmp_path):
    data=generate_retail(tmp_path/'data',100)
    artifact=prepare_retail_model(data,tmp_path/'model')
    file=artifact.parent/'scenarios.parquet'
    rows=pq.read_table(file).to_pylist()
    rows[0]['weight']=-1;rows[1]['weight']=1.75
    pq.write_table(pa.Table.from_pylist(rows),file)
    manifest=json.loads(artifact.read_text())
    next(f for f in manifest['files'] if f['key']==file.name)['sha256']=sha256_file(file)
    artifact.write_text(json.dumps(manifest))
    with pytest.raises(ValueError,match='scenario'):
        install_retail_model(analytics,artifact)
