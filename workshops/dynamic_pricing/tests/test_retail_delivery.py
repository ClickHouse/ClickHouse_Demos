from datetime import UTC, datetime
from uuid import uuid4
import pytest
from generator.retail import generate_retail
from pricing_lab.retail_history import RetailHistory
from pricing_lab.retail_source import RetailSource, make_batch
from pricing_lab.retail_delivery import RetailDelivery
from pricing_lab.retail_stream import generate_batch

pytestmark = pytest.mark.integration


@pytest.fixture
def pipeline(analytics, ledger, tmp_path):
    path = generate_retail(tmp_path / 'data', 100)
    history = RetailHistory(analytics, ledger)
    history.load(path.as_uri())
    history.create_rollup()
    source = RetailSource(ledger, tmp_path / 'spool')
    run_id = source.initialize(path.as_uri())
    return source, RetailDelivery(analytics, source), run_id, analytics


def staged(pipeline):
    source, delivery, run, analytics = pipeline
    state = source.state(run, limit=1)
    rows = generate_batch(run, 1, state, 3, datetime(2026,1,30,2,0,2,tzinfo=UTC), 17)
    batch = make_batch(run, 1, rows, datetime(2026,1,30,2,0,2,tzinfo=UTC))
    before = state[0]['stock_units']
    source.stage(batch, rows)
    return batch, rows, before


def test_lost_ack_reconciles_raw_and_both_rollups(pipeline, monkeypatch):
    source, delivery, run, analytics = pipeline
    batch, rows, before = staged(pipeline)
    original = delivery._insert
    def lost(*args):
        original(*args)
        raise TimeoutError('lost acknowledgment')
    monkeypatch.setattr(delivery, '_insert', lost)
    with pytest.raises(TimeoutError):
        delivery.deliver(batch)
    monkeypatch.setattr(delivery, '_insert', original)
    assert delivery.deliver(batch)['status'] == 'acknowledged'
    assert not source.pending(run)
    assert source.state(run, limit=1)[0]['stock_units'] == before - 3
    assert analytics.query(f'SELECT sum(quantity) n FROM {analytics.database}.retail_sales WHERE run_id={{run:UUID}}', {'run':run})[0]['n'] == 3


def test_source_restart_and_changed_payload(pipeline):
    source, delivery, run, analytics = pipeline
    batch, rows, before = staged(pipeline)
    restarted = RetailSource(source.ledger, source.spool)
    assert restarted.pending(run) == [batch]
    assert restarted.stage(batch, rows) == batch
    bad = [dict(rows[0], quantity=2), *rows[1:]]
    with pytest.raises(ValueError, match='hash'):
        restarted.stage(batch, bad)
    assert delivery.deliver(batch)['status'] == 'acknowledged'


def test_missing_spool_holds_run(pipeline):
    source, delivery, run, analytics = pipeline
    batch, _, _ = staged(pipeline)
    source.payload_path(batch).unlink()
    with pytest.raises(ValueError, match='payload'):
        delivery.deliver(batch)
    assert source.run(run)['status'] == 'held'


def test_stale_price_does_not_commit_stock(pipeline):
    source, delivery, run, analytics = pipeline
    state = source.state(run, limit=1)
    rows = generate_batch(run,1,state,1,datetime(2026,1,30,2,0,2,tzinfo=UTC),17)
    rows[0]['price_version'] += 1
    batch = make_batch(run,1,rows,datetime(2026,1,30,2,0,2,tzinfo=UTC))
    with pytest.raises(ValueError, match='version'):
        source.stage(batch, rows)
    assert source.state(run,limit=1)[0]['stock_units'] == state[0]['stock_units']


def test_expired_retry_can_reconcile_but_cannot_blindly_insert(pipeline):
    from datetime import timedelta
    source, delivery, run, analytics = pipeline
    batch, rows, before = staged(pipeline)
    old = batch.model_copy(update={'created_at':batch.created_at-timedelta(hours=1)})
    with pytest.raises(ValueError, match='window'):
        delivery.deliver(old)
    assert source.run(run)['status'] == 'held'


def test_return_cannot_be_replayed_in_another_batch(pipeline):
    from datetime import timedelta
    source, delivery, run, analytics = pipeline
    batch, rows, before = staged(pipeline)
    delivery.deliver(batch)
    state = source.state(run,limit=1)[0]
    returned = dict(rows[0], event_id=uuid4(), transaction_id=uuid4(), original_event_id=rows[0]['event_id'],
                    quantity=-1, receipts_minor=-rows[0]['receipts_minor'], sold_cost_minor=-rows[0]['sold_cost_minor'],
                    inventory_version=state['inventory_version'], event_time=batch.logical_end+timedelta(seconds=1))
    second = make_batch(run,2,[returned],batch.logical_end+timedelta(seconds=2))
    source.stage(second,[returned]); delivery.deliver(second)
    assert source.state(run,limit=1)[0]['stock_units'] == before - 2
    returned.update(event_id=uuid4(),transaction_id=uuid4(),inventory_version=state['inventory_version']+1)
    third=make_batch(run,3,[returned],second.logical_end+timedelta(seconds=2))
    with pytest.raises(ValueError, match='returned'):
        source.stage(third,[returned])


def test_equal_totals_with_wrong_event_id_are_held(pipeline):
    source,delivery,run,analytics=pipeline
    batch,rows,_=staged(pipeline)
    wrong=[dict(rows[0],event_id=uuid4()),*rows[1:]]
    delivery._insert(batch,wrong)
    with pytest.raises(ValueError,match='identity'):
        delivery.deliver(batch)
    assert source.run(run)['status']=='held'


def test_partial_delivery_is_held_without_reinsert(pipeline):
    source,delivery,run,analytics=pipeline
    batch,rows,_=staged(pipeline)
    delivery._insert(batch,rows[:1])
    with pytest.raises(ValueError,match='Partial'):
        delivery.deliver(batch)
    assert delivery._raw(batch)['rows']==1
    assert source.run(run)['status']=='held'


def test_deleted_source_subject_does_not_commit(pipeline):
    source,delivery,run,analytics=pipeline
    rows=generate_batch(run,1,source.state(run,limit=1),1,datetime(2026,1,30,2,0,2,tzinfo=UTC),17)
    batch=make_batch(run,1,rows,datetime(2026,1,30,2,0,2,tzinfo=UTC))
    with source.ledger.connect() as conn:
        conn.execute('DELETE FROM pricing.retail_state WHERE run_id=%s AND store_id=1 AND sku=101',(run,))
    with pytest.raises(ValueError,match='deleted'):
        source.stage(batch,rows)
    assert not source.pending(run)


def test_stopped_repair_rebuilds_missing_aggregate_from_verified_raw(pipeline):
    source,delivery,run,analytics=pipeline
    batch,rows,_=staged(pipeline)
    delivery._insert(batch,rows)
    analytics.command(f'TRUNCATE TABLE {analytics.database}.retail_district_minute')
    with pytest.raises(ValueError,match='aggregates'):
        delivery.deliver(batch)
    assert delivery.rebuild_rollups(run)['status']=='stopped'
    assert delivery.deliver(batch)['status']=='acknowledged'
