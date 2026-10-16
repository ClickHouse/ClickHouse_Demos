"""Isolated retail edge fixture: actual databases; explicit test-only CDC replay."""
import os,json,sys
from datetime import UTC,datetime,timedelta
from pathlib import Path
from uuid import uuid4
from psycopg.types.json import Jsonb
from pricing_lab.settings import Settings,LAB_ROOT
from pricing_lab.analytics import AnalyticsClient
from pricing_lab.ledger import Ledger
from pricing_lab.contracts import Scope,Subject
from pricing_lab.retail_source import RetailSource,make_batch
from pricing_lab.retail_delivery import RetailDelivery
from pricing_lab.retail_outcomes import pair_rows,install_retail_outcomes
from pricing_lab.retail_schema import create_rollup_target,rollup_select
from test_retail_analysis import opportunity_case

settings=Settings(_env_file=None,postgres_dsn=os.environ['PRICING_TEST_POSTGRES_DSN'],clickhouse_host='127.0.0.1',clickhouse_port=int(os.environ['PRICING_TEST_CLICKHOUSE_PORT']),clickhouse_secure=False,clickhouse_database='pricing',clickhouse_password='test-only-password')
a=AnalyticsClient(settings);ledger=Ledger(settings.postgres_dsn.get_secret_value())
if len(sys.argv)==1:
    a.bootstrap_database();ledger.migrate()
    golden=json.loads((LAB_ROOT/'fixtures/golden/scenario.json').read_text())
    ledger.initialize(Scope(owner_id='learner-1',scenario_id='grocery',generation=1),Subject(sku=101,store_id=1,channel='store'),golden['state'],golden['rules'],datetime(2026,1,30,8,tzinfo=UTC))
    _,run,cutoff=opportunity_case.__wrapped__(a)
    batch=a.query('SELECT batch_id FROM pricing.retail_batches_cdc LIMIT 1')[0]['batch_id']
    with ledger.connect() as conn:
        conn.execute('INSERT INTO pricing.retail_runs(run_id,manifest_digest,logical_end,seed,sequence) VALUES (%s,%s,%s,200017,1)',(run,'edge-fixture',cutoff))
        conn.execute("INSERT INTO pricing.retail_state VALUES (%s,1,1,101,20,'2026-01-30 10:00:00+00',800,400,20,50,ARRAY[600,700,800],1,1,1)",(run,))
        conn.execute("INSERT INTO pricing.retail_batches(batch_id,run_id,sequence,payload_sha256,event_count,transaction_count,logical_end,created_at,status,acknowledged_at) VALUES (%s,%s,1,%s,2,2,%s,now(),'acknowledged',now())",(batch,run,'0'*64,cutoff))
    create_rollup_target(a,'retail_district_minute',False);select,keys=rollup_select('pricing',False)
    a.command(f'INSERT INTO pricing.retail_district_minute {select} GROUP BY {keys}')
    a.command(f'CREATE MATERIALIZED VIEW pricing.retail_district_minute_mv TO pricing.retail_district_minute AS {select} GROUP BY {keys}')
    a.command('CREATE TABLE pricing.retail_receipts_cdc(run_id UUID,receipt_id UUID,data String,_peerdb_version UInt64,_peerdb_is_deleted UInt8) ENGINE=ReplacingMergeTree(_peerdb_version) ORDER BY receipt_id')
    install_retail_outcomes(a)
else:
    with ledger.connect() as conn:
        run=conn.execute('SELECT * FROM pricing.retail_runs LIMIT 1').fetchone()
        receipts=conn.execute('SELECT * FROM pricing.retail_receipts').fetchall()
    source=RetailSource(ledger,LAB_ROOT/'.run/edge-retail-spool')
    end=run['logical_end']+timedelta(hours=2)
    rows=pair_rows(source,run['run_id'],2,end)
    batch=make_batch(run['run_id'],2,rows,end);source.stage(batch,rows)
    RetailDelivery(a,source).deliver(batch)
    for r in receipts:
        a.client.insert('pricing.retail_receipts_cdc',[[run['run_id'],r['receipt_id'],json.dumps(r['data']),1,0]],column_names=['run_id','receipt_id','data','_peerdb_version','_peerdb_is_deleted'])
    a.client.insert('pricing.retail_batches_cdc',[[run['run_id'],batch.batch_id,2,len(rows),'acknowledged',end,datetime.now(UTC),1,0]],column_names=['run_id','batch_id','sequence','event_count','status','logical_end','acknowledged_at','_peerdb_version','_peerdb_is_deleted'])
    print(f'Delivered {len(rows)} actual paired events; CDC metadata replay is test-only.')
a.close()
