import pytest
from pricing_lab.retail_contracts import PriceInput
from pricing_lab.retail_outcomes import paired_demand


def test_identical_prices_have_identical_paired_demands(retail_fixture):
    inputs=PriceInput.model_validate(retail_fixture['input'])
    for sequence in range(50):
        applied,unchanged=paired_demand(200017,sequence,inputs,800)
        assert applied==unchanged
        assert applied>=0


def test_pair_is_repeatable_and_branches_use_same_draw(retail_fixture):
    inputs=PriceInput.model_validate(retail_fixture['input'])
    for sequence in range(50):
        pair=paired_demand(200017,sequence,inputs,700)
        assert pair==paired_demand(200017,sequence,inputs,700)
        assert pair[0]>=pair[1]


@pytest.mark.integration
def test_independent_stocks_and_source_cursor(ledger,analytics,retail_fixture,tmp_path):
    from datetime import UTC,datetime
    from types import SimpleNamespace
    from uuid import uuid4
    from pricing_lab.clock import Clock
    from pricing_lab.retail_contracts import RetailApproval
    from pricing_lab.retail_approval import RetailApprovalService
    from pricing_lab.retail_analytics import RetailAnalytics
    from pricing_lab.retail_source import RetailSource,make_batch
    from pricing_lab.retail_outcomes import pair_rows
    inputs=PriceInput.model_validate(retail_fixture['input']);run=uuid4()
    with ledger.connect() as conn:
        conn.execute('INSERT INTO pricing.retail_runs(run_id,manifest_digest,logical_end,seed) VALUES (%s,%s,%s,200017)',(run,'fixture',inputs.cutoff))
        conn.execute('INSERT INTO pricing.retail_state VALUES (%s,5000,20,101,20,%s,800,400,20,50,ARRAY[600,700,800],1,1,1)',(run,inputs.expires_at))
    analysis=SimpleNamespace(price_inputs=lambda *args:inputs,compare=RetailAnalytics(analytics).compare,model_version=lambda:'retail-fixture-v2')
    approval=RetailApprovalService(ledger,analysis,Clock(inputs.cutoff))
    receipt=approval.apply(RetailApproval(proposal_id=approval.propose(inputs,700),idempotency_key=uuid4()),'learner-1')
    source=RetailSource(ledger,tmp_path/'spool')
    with ledger.connect() as conn:
        conn.execute('UPDATE pricing.retail_comparisons SET comparison_stock=2 WHERE receipt_id=%s',(receipt.receipt_id,))
    rows=pair_rows(source,run,1,inputs.expires_at)
    applied=sum(r['quantity'] for r in rows if r['branch']=='applied')
    control=sum(r['quantity'] for r in rows if r['branch']=='comparison')
    assert 0<applied<=20 and 0<=control<=2
    batch=make_batch(run,1,rows,inputs.expires_at)
    source.stage(batch,rows);source.stage(batch,rows)
    assert source.state(run,limit=1)[0]['stock_units']==20-applied
    with ledger.connect() as conn:
        result=conn.execute('SELECT * FROM pricing.retail_comparisons WHERE receipt_id=%s',(receipt.receipt_id,)).fetchone()
    assert result['comparison_stock']==2-control
    assert result['logical_end']==inputs.expires_at
    assert pair_rows(source,run,2,inputs.expires_at)==[]


@pytest.mark.integration
def test_outcome_series_uses_actual_sales_and_labels_waste_at_expiry(analytics,retail_fixture):
    import json
    from uuid import uuid4
    from pricing_lab.retail_schema import install_retail_schema
    from pricing_lab.retail_outcomes import OutcomeService
    install_retail_schema(analytics); db=analytics.database
    run,receipt,batch=uuid4(),uuid4(),uuid4()
    analytics.command(f'CREATE TABLE {db}.retail_receipts_cdc (run_id UUID,receipt_id UUID,data String,_peerdb_version UInt64,_peerdb_is_deleted UInt8) ENGINE=ReplacingMergeTree(_peerdb_version) ORDER BY receipt_id')
    analytics.command(f"CREATE TABLE {db}.retail_batches_cdc (run_id UUID,batch_id UUID,event_count UInt64,logical_end DateTime64(3,'UTC'),status String,_peerdb_version UInt64,_peerdb_is_deleted UInt8) ENGINE=ReplacingMergeTree(_peerdb_version) ORDER BY batch_id")
    data=dict(inputs=retail_fixture['input'],receipt=dict(price_minor=700),started_at='2026-01-30T08:00:00Z')
    analytics.client.insert(f'{db}.retail_receipts_cdc',[[run,receipt,json.dumps(data),1,0]],column_names=['run_id','receipt_id','data','_peerdb_version','_peerdb_is_deleted'])
    analytics.command(f"INSERT INTO {db}.retail_batches_cdc VALUES ('{run}','{batch}',2,'2026-01-30 09:00:00','acknowledged',1,0)")
    for branch,quantity,price in [('applied',2,700),('comparison',1,800)]:
        analytics.command(f"INSERT INTO {db}.retail_sales (event_id,transaction_id,store_id,district_id,sku,quantity,price_minor,receipts_minor,sold_cost_minor,event_time,run_id,batch_id,receipt_id,branch,source) VALUES ('{uuid4()}','{uuid4()}',5000,20,101,{quantity},{price},{quantity*price},{quantity*400},'2026-01-30 08:59:00','{run}','{batch}','{receipt}','{branch}','live')")
    from pricing_lab.retail_outcomes import install_retail_outcomes
    install_retail_outcomes(analytics)
    before=OutcomeService(analytics).series(run,receipt)
    applied=[r for r in before if r['branch']=='applied'][-1]
    assert applied['remaining_stock']==18 and applied['receipts_minor']==1400
    assert applied['expired'] is False and applied['waste_units']==0
    analytics.command(f"INSERT INTO {db}.retail_batches_cdc VALUES ('{run}','{batch}',2,'2026-01-30 10:00:00','acknowledged',2,0)")
    after=OutcomeService(analytics).series(run,receipt)
    applied=[r for r in after if r['branch']=='applied'][-1]
    assert applied['expired'] is True and applied['waste_units']==18
    assert applied['contribution_minor']==1400-40-8000-900
    assert analytics.query(f"SELECT sum(quantity) units FROM {db}.retail_sales WHERE branch='applied'")[0]['units']==2
