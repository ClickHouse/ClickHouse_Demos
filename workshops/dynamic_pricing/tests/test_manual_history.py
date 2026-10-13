"""A console import must become active without inserting or deleting its facts."""
from uuid import uuid4
import pytest
from pricing_lab.history import HistoryLoader
from pricing_lab.contracts import WorkshopError

pytestmark=pytest.mark.integration


def console_set(loader,scope,manifest_uri):
    from pricing_lab.history import resolve_manifest
    manifest,source=resolve_manifest(manifest_uri)
    data={'manifest':manifest,'sandbox_id':scope.sandbox_id,'raw_table':'hist_raw_console','rollup_table':'hist_hour_console','mv_table':'hist_mv_console'}
    loader._create_set(data)
    for file in manifest['files']: loader._insert_file(source,file,data,uuid4())
    return data


def test_register_console_history_preserves_rows_and_is_idempotent(loader,scope,manifest_uri,analytics):
    console_set(loader,scope,manifest_uri)
    first=loader.register_console(manifest_uri,scope)
    assert first['canonical_rows']==3 and first['net_units']==4 and first['receipts_minor']==2900
    assert first['source']=='sql_console'
    second=loader.register_console(manifest_uri,scope)
    assert second['run_id']==first['run_id']
    assert analytics.query(f'SELECT count() AS n FROM {analytics.database}.hist_raw_console')[0]['n']==3
    assert analytics.query(f'SELECT count() AS n FROM {analytics.database}.sales_history')[0]['n']==3
    assert analytics.query(f'SELECT sum(row_count) AS n FROM {analytics.database}.sales_history_hourly')[0]['n']==3
    assert loader.active(scope)['raw_table']=='hist_raw_console'
    assert loader.verify(first['run_id'])['sold_cost_minor']==1600
    assert loader.active(scope).get('measurement') is None


def test_bad_console_totals_preserve_previous_active_import_and_console_tables(loader,scope,manifest_uri,analytics):
    active=loader.load(manifest_uri,scope)
    console_set(loader,scope,manifest_uri)
    analytics.command(f'TRUNCATE TABLE {analytics.database}.hist_hour_console')
    with pytest.raises(ValueError,match='reconcile'): loader.register_console(manifest_uri,scope)
    assert loader.active(scope)['run_id']==active['run_id']
    assert analytics.query(f'SELECT count() AS n FROM {analytics.database}.hist_raw_console')[0]['n']==3
    assert analytics.query(f'SELECT count() AS n FROM {analytics.database}.sales_history')[0]['n']==3


def test_console_registration_rejects_shifted_hourly_groups(loader,scope,manifest_uri,analytics):
    console_set(loader,scope,manifest_uri)
    db=analytics.database
    analytics.command(f'TRUNCATE TABLE {db}.hist_hour_console')
    analytics.command(f"""INSERT INTO {db}.hist_hour_console
      SELECT sandbox_id,store_id+1,sku,toStartOfHour(event_time),sum(quantity),sum(receipts_minor),
             sum(sold_cost_minor),count() FROM {db}.hist_raw_console
      GROUP BY sandbox_id,store_id,sku,toStartOfHour(event_time)""")
    with pytest.raises(ValueError,match='grouped facts'): loader.register_console(manifest_uri,scope)


def test_console_registration_rejects_wrong_rollup_sort_key(loader,scope,manifest_uri,analytics):
    console_set(loader,scope,manifest_uri)
    db=analytics.database
    analytics.command(f'DROP TABLE {db}.hist_mv_console SYNC')
    analytics.command(f'DROP TABLE {db}.hist_hour_console SYNC')
    analytics.command(f"""CREATE TABLE {db}.hist_hour_console (
      sandbox_id String, store_id UInt16, sku UInt32, hour DateTime('UTC'),
      net_units Int64, receipts_minor Int64, sold_cost_minor Int64, row_count UInt64
    ) ENGINE=SummingMergeTree ORDER BY (sandbox_id,hour,store_id,sku)""")
    analytics.command(f"""CREATE MATERIALIZED VIEW {db}.hist_mv_console TO {db}.hist_hour_console AS
      SELECT sandbox_id,store_id,sku,toStartOfHour(event_time) AS hour,
             sum(quantity) AS net_units,sum(receipts_minor) AS receipts_minor,
             sum(sold_cost_minor) AS sold_cost_minor,count() AS row_count
      FROM {db}.hist_raw_console GROUP BY sandbox_id,store_id,sku,hour""")
    analytics.command(f"""INSERT INTO {db}.hist_hour_console
      SELECT sandbox_id,store_id,sku,toStartOfHour(event_time),sum(quantity),sum(receipts_minor),
             sum(sold_cost_minor),count() FROM {db}.hist_raw_console
      GROUP BY sandbox_id,store_id,sku,toStartOfHour(event_time)""")
    with pytest.raises(WorkshopError,match='console_history_schema'): loader.register_console(manifest_uri,scope)


def test_console_registration_rejects_other_owner_before_promotion(loader,scope,manifest_uri,analytics):
    from pricing_lab.contracts import Scope
    console_set(loader,Scope(owner_id='other',scenario_id=scope.scenario_id,generation=scope.generation),manifest_uri)
    with pytest.raises(WorkshopError,match='console_history_scope'): loader.register_console(manifest_uri,scope)
    assert analytics.query('SELECT count() AS n FROM system.tables WHERE database={db:String} AND name=\'sales_history\'',{'db':analytics.database})[0]['n']==0


def test_failed_console_promotion_never_drops_manually_loaded_tables(loader,scope,manifest_uri,analytics,monkeypatch):
    active=loader.load(manifest_uri,scope)
    console_set(loader,scope,manifest_uri)
    original=analytics.command;failed=False
    def command(sql,parameters=None):
        nonlocal failed
        if 'CREATE OR REPLACE VIEW' in sql and '.sales_history_hourly ' in sql and not failed:
            failed=True;raise RuntimeError('Lost console-view acknowledgement')
        return original(sql,parameters)
    monkeypatch.setattr(analytics,'command',command)
    with pytest.raises(RuntimeError): loader.register_console(manifest_uri,scope)
    assert loader.active(scope)['run_id']==active['run_id']
    assert analytics.query(f'SELECT count() AS n FROM {analytics.database}.hist_raw_console')[0]['n']==3
