import psycopg
import pytest

from cdc_utils import replay_table

pytestmark = pytest.mark.integration


def test_current_view_deduplicates_updates_and_excludes_tombstones(features, source, ledger, analytics, scope, subject, sale_request, postgres_dsn):
    source.create_sale(scope, sale_request)
    replay_table(analytics, ledger, "sales_transactions")
    assert analytics.live_current(scope, subject)[0]["receipts_minor"] == 1600
    with psycopg.connect(postgres_dsn) as conn:
        conn.execute("UPDATE pricing.sales_transactions SET unit_price_minor=700,receipts_minor=quantity*700")
    replay_table(analytics, ledger, "sales_transactions", version=2)
    rows = analytics.live_current(scope, subject)
    assert len(rows) == 1 and rows[0]["receipts_minor"] == 1400
    replay_table(analytics, ledger, "sales_transactions", version=3, deleted=True)
    assert analytics.live_current(scope, subject) == []


def test_late_live_event_reconciles_combined_old_bucket_without_history_mv(features, source, ledger, analytics, scope, subject, sale_request):
    source.create_sale(scope, sale_request.model_copy(update={"occurred_at":sale_request.occurred_at.replace(day=29,hour=10)}))
    replay_table(analytics, ledger, "sales_transactions")
    result = features.reconcile(scope, subject)
    assert result["raw"] == result["rollup"]
    assert result["raw"]["receipts_minor"] == 4500
    history = analytics.query(f"SELECT sum(receipts_minor) AS receipts FROM {analytics.database}.sales_history_hourly")
    assert history[0]["receipts"] == 2900


def test_old_generation_rows_do_not_enter_current_view(features, source, ledger, analytics, scope, subject, sale_request):
    source.create_sale(scope, sale_request)
    replay_table(analytics, ledger, "sales_transactions")
    assert analytics.live_current(scope.model_copy(update={"generation":2}), subject) == []
