from uuid import uuid4
import pytest
from pricing_lab.retail_cdc import configure_retail_cdc, batch_visible

pytestmark = pytest.mark.integration


def test_completion_metadata_alone_is_not_sales_readiness(analytics):
    from pricing_lab.retail_schema import install_retail_schema
    install_retail_schema(analytics)
    db=analytics.database
    analytics.command(f'''CREATE TABLE {db}.retail_batches_cdc (batch_id UUID, run_id UUID, sequence Int64,
      event_count Int64, status String, _peerdb_version UInt64, _peerdb_is_deleted UInt8)
      ENGINE=ReplacingMergeTree(_peerdb_version) ORDER BY batch_id''')
    batch,run=uuid4(),uuid4()
    analytics.client.insert(f'{db}.retail_batches_cdc',[[batch,run,1,1,'acknowledged',1,0]],column_names=['batch_id','run_id','sequence','event_count','status','_peerdb_version','_peerdb_is_deleted'])
    assert batch_visible(analytics,run,batch) is False


def test_deleted_latest_inventory_does_not_resurrect_old_version(analytics):
    db=analytics.database
    analytics.command(f'''CREATE TABLE {db}.retail_inventory_history_cdc (run_id UUID, store_id Int32, sku Int32,
      version Int64,stock_units Int32,expires_at DateTime64(3,'UTC'), effective_sequence Int64,
      _peerdb_version UInt64,_peerdb_is_deleted UInt8)
      ENGINE=ReplacingMergeTree(_peerdb_version) ORDER BY (run_id,store_id,sku,version)''')
    run=uuid4()
    analytics.command(f"INSERT INTO {db}.retail_inventory_history_cdc VALUES ('{run}',1,101,1,20,'2026-01-30 10:00:00',0,1,0),('{run}',1,101,2,19,'2026-01-30 10:00:00',1,2,0),('{run}',1,101,2,19,'2026-01-30 10:00:00',1,3,1)")
    configure_retail_cdc(analytics, only_inventory=True)
    assert analytics.query(f'SELECT * FROM {db}.retail_inventory_current') == []
