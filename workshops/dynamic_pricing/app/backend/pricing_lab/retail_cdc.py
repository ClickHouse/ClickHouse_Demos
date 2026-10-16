"""Canonical operational evidence keeps CDC versions and physical sales distinct."""
from .analytics import identifier
from .settings import LAB_ROOT

RETAIL_TABLES = ('retail_runs','retail_batches','retail_inventory_history','retail_price_history',
                 'retail_policy_history','retail_receipts')


def configure_retail_cdc(analytics, only_inventory=False):
    sql=(LAB_ROOT/'sql/clickhouse/009_retail_cdc.sql').read_text().replace('__DATABASE__',identifier(analytics.database))
    statements=[s for s in sql.split(';') if s.strip()]
    for statement in statements[:1] if only_inventory else statements:
        analytics.command(statement)


def batch_visible(analytics,run_id,batch_id):
    db=identifier(analytics.database)
    metadata=analytics.query(f'''SELECT event_count FROM {db}.retail_batches_cdc FINAL
      WHERE run_id={{run:UUID}} AND batch_id={{batch:UUID}} AND status='acknowledged' AND _peerdb_is_deleted=0''',
      {'run':run_id,'batch':batch_id})
    if len(metadata)!=1:
        return False
    actual=analytics.query(f'''SELECT count() n,uniqExact(event_id) distinct_n FROM {db}.retail_sales
      WHERE run_id={{run:UUID}} AND batch_id={{batch:UUID}}''',{'run':run_id,'batch':batch_id})[0]
    return actual['n']==actual['distinct_n']==metadata[0]['event_count']
