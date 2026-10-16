"""Durable source commits precede analytical delivery; retries reuse immutable bytes."""
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
from uuid import UUID, uuid4, uuid5

import pyarrow.parquet as pq
from psycopg.types.json import Jsonb

from .history import resolve_manifest, public_read
from .settings import LAB_ROOT
from .retail_contracts import RetailBatch


def payload_bytes(rows):
    return (json.dumps(rows, sort_keys=True, separators=(',', ':'), default=str) + '\n').encode()


def make_batch(run_id, sequence, rows, logical_end):
    return RetailBatch(run_id=run_id, batch_id=uuid5(run_id,f'batch/{sequence}'), sequence=sequence,
                       payload_sha256=hashlib.sha256(payload_bytes(rows)).hexdigest(), event_count=len(rows),
                       transaction_count=len({str(r['transaction_id']) for r in rows}), logical_end=logical_end,
                       created_at=datetime.now(UTC))


class UncommittedBatchConflict(ValueError):
    """Operational state changed before the batch committed; regenerate safely."""


class RetailSource:
    def __init__(self, ledger, spool):
        self.ledger, self.spool = ledger, Path(spool)
        self.spool.mkdir(parents=True, exist_ok=True)

    def migrate(self):
        with self.ledger.connect() as conn:
            conn.execute((LAB_ROOT / 'sql/postgres/003_retail.sql').read_text())

    def initialize(self, manifest_uri):
        from urllib.parse import urljoin
        import io
        manifest, root = resolve_manifest(manifest_uri, expected_version='retail-chain-v2')
        digest = hashlib.sha256(json.dumps(manifest,sort_keys=True).encode()).hexdigest()
        def read(name):
            source = root / name if isinstance(root, Path) else io.BytesIO(public_read(urljoin(root,name),lambda r:r.read()))
            return pq.read_table(source).to_pylist()
        products = {r['sku']:r for r in read('products.parquet')}
        inventory = read('inventory.parquet')
        with self.ledger.connect() as conn:
            conn.execute('SELECT pg_advisory_xact_lock(84927641)')
            existing = conn.execute('SELECT run_id,manifest_digest FROM pricing.retail_runs ORDER BY created_at DESC LIMIT 1').fetchone()
            if existing:
                if existing['manifest_digest'] != digest:
                    raise ValueError('A different retail dataset is active.')
                return existing['run_id']
            run = uuid4()
            conn.execute('INSERT INTO pricing.retail_runs(run_id,manifest_digest,logical_end,seed) VALUES (%s,%s,%s,%s)',
                         (run,digest,manifest['history_cutover'],manifest['seed']+200000))
            columns = 'run_id,store_id,district_id,sku,stock_units,expires_at,current_price_minor,unit_cost_minor,selling_cost_minor,disposal_cost_minor,permitted_prices_minor,inventory_version,price_version,rule_version'
            with conn.cursor().copy(f'COPY pricing.retail_state({columns}) FROM STDIN') as copy:
                for r in inventory:
                    p=products[r['sku']]
                    copy.write_row((run,r['store_id'],(r['store_id']-1)//250+1,r['sku'],r['stock_units'],r['expires_at'],r['current_price_minor'],p['unit_cost_minor'],p['selling_cost_minor'],p['disposal_cost_minor'],p['permitted_prices_minor'],1,1,1))
            conn.execute('INSERT INTO pricing.retail_inventory_history SELECT run_id,store_id,sku,inventory_version,stock_units,expires_at,0 FROM pricing.retail_state WHERE run_id=%s',(run,))
            conn.execute('INSERT INTO pricing.retail_price_history SELECT run_id,store_id,sku,price_version,current_price_minor,0 FROM pricing.retail_state WHERE run_id=%s',(run,))
            conn.execute('INSERT INTO pricing.retail_policy_history(run_id,store_id,sku,version,permitted_prices_minor,unit_cost_minor,selling_cost_minor,disposal_cost_minor) SELECT run_id,store_id,sku,rule_version,permitted_prices_minor,unit_cost_minor,selling_cost_minor,disposal_cost_minor FROM pricing.retail_state WHERE run_id=%s',(run,))
            return run

    def run(self, run_id):
        with self.ledger.connect() as conn:
            row = conn.execute('SELECT * FROM pricing.retail_runs WHERE run_id=%s',(run_id,)).fetchone()
        if row is None:
            raise ValueError('Retail run is missing.')
        return row

    def state(self, run_id, limit=100000):
        with self.ledger.connect() as conn:
            return conn.execute('SELECT * FROM pricing.retail_state WHERE run_id=%s ORDER BY store_id,sku LIMIT %s',(run_id,limit)).fetchall()

    def comparisons(self,run_id):
        with self.ledger.connect() as conn:
            return conn.execute('SELECT c.*,s.price_version,s.inventory_version FROM pricing.retail_comparisons c JOIN pricing.retail_state s USING(run_id,store_id,sku) WHERE c.run_id=%s AND c.active ORDER BY c.receipt_id',(run_id,)).fetchall()

    def _advance_comparisons(self,conn,batch,rows):
        comparisons=conn.execute('SELECT * FROM pricing.retail_comparisons WHERE run_id=%s AND active ORDER BY receipt_id FOR UPDATE',(batch.run_id,)).fetchall()
        known={c['receipt_id'] for c in comparisons}
        for row in rows:
            receipt=row.get('receipt_id',UUID(int=0))
            if (row.get('branch')=='comparison' and not receipt.int) or (receipt.int and receipt not in known):
                raise UncommittedBatchConflict('The comparison changed; regenerate this batch.')
        for comparison in comparisons:
            associated=[r for r in rows if r.get('receipt_id')==comparison['receipt_id']]
            applied=sum(r['quantity'] for r in associated if r.get('branch','applied')=='applied')
            control=sum(r['quantity'] for r in associated if r.get('branch')=='comparison')
            inputs=comparison['inputs']
            for row in associated:
                expected_price=comparison['price_minor'] if row['branch']=='applied' else inputs['current_price_minor']
                if row['price_minor']!=expected_price or row['quantity']<0 or row['store_id']!=comparison['store_id'] or row['sku']!=comparison['sku']:
                    raise ValueError('Paired sales differ from the acknowledged comparison.')
            if applied>comparison['applied_stock'] or control>comparison['comparison_stock']:
                raise ValueError('Paired sales exceed independent branch stock.')
            conn.execute('UPDATE pricing.retail_comparisons SET applied_stock=applied_stock-%s,comparison_stock=comparison_stock-%s,logical_end=%s,sequence=%s WHERE receipt_id=%s',
                         (applied,control,batch.logical_end,batch.sequence,comparison['receipt_id']))

    def payload_path(self, batch):
        return self.spool / f'{batch.batch_id}-{batch.payload_sha256}.json'

    def payload(self, batch, hold=True):
        path = self.payload_path(batch)
        if not path.exists() or hashlib.sha256(path.read_bytes()).hexdigest() != batch.payload_sha256:
            if hold:
                self.hold(batch.run_id)
            raise ValueError('Batch payload is missing or has a different hash; delivery held.')
        rows = json.loads(path.read_text())
        for r in rows:
            for key in ('event_id','transaction_id','original_event_id','receipt_id'):
                if key in r:
                    r[key] = UUID(r[key])
            r['event_time'] = datetime.fromisoformat(r['event_time'])
        return rows

    def hold(self, run_id):
        with self.ledger.connect() as conn:
            conn.execute("UPDATE pricing.retail_runs SET status='held' WHERE run_id=%s",(run_id,))

    def _claim_return(self, conn, batch, row):
        original_id = row['original_event_id']
        if conn.execute('SELECT 1 FROM pricing.retail_return_claims WHERE run_id=%s AND original_event_id=%s',
                        (batch.run_id, original_id)).fetchone():
            raise ValueError('This sale has already been returned.')
        batches = conn.execute("SELECT * FROM pricing.retail_batches WHERE run_id=%s AND status='acknowledged' ORDER BY sequence DESC", (batch.run_id,)).fetchall()
        original = None
        for metadata in batches:
            previous = RetailBatch.model_validate({k:metadata[k] for k in RetailBatch.model_fields})
            original = next((r for r in self.payload(previous, hold=False) if r['event_id'] == original_id), None)
            if original:
                break
        if not original or original['quantity'] <= 0 or row['quantity'] != -original['quantity']:
            raise ValueError('Return must reverse an acknowledged original sale once.')
        if any(row[k] != original[k] for k in ('store_id','sku','price_minor','price_version')) or any(row[k] != -original[k] for k in ('receipts_minor','sold_cost_minor')):
            raise ValueError('Return amounts differ from the original sale.')
        conn.execute('INSERT INTO pricing.retail_return_claims VALUES (%s,%s,%s)', (batch.run_id,original_id,batch.batch_id))

    def stage(self, batch, rows):
        if not 0 <= len(rows) <= 10000 or len({str(r['event_id']) for r in rows}) != len(rows):
            raise ValueError('Use up to 10,000 uniquely identified sales lines per batch; an empty batch advances the completed clock.')
        encoded = payload_bytes(rows)
        if hashlib.sha256(encoded).hexdigest() != batch.payload_sha256:
            raise ValueError('Batch payload hash mismatch.')
        if len(rows) != batch.event_count or len({str(r['transaction_id']) for r in rows}) != batch.transaction_count:
            raise ValueError('Batch counts mismatch.')
        path = self.payload_path(batch)
        temporary = self.spool / f'.{uuid4().hex}.tmp'
        try:
            with temporary.open('xb') as f:
                f.write(encoded); f.flush(); os.fsync(f.fileno())
            os.link(temporary, path)
            fd = os.open(self.spool, os.O_RDONLY)
            try: os.fsync(fd)
            finally: os.close(fd)
        except FileExistsError:
            if path.read_bytes() != encoded:
                raise ValueError('Immutable batch hash conflict.')
        finally:
            temporary.unlink(missing_ok=True)
        with self.ledger.connect() as conn:
            run = conn.execute('SELECT * FROM pricing.retail_runs WHERE run_id=%s FOR UPDATE',(batch.run_id,)).fetchone()
            if not run or run['status'] == 'held':
                raise ValueError('Retail run is missing or held.')
            existing = conn.execute('SELECT * FROM pricing.retail_batches WHERE batch_id=%s',(batch.batch_id,)).fetchone()
            if existing:
                if existing['payload_sha256'] != batch.payload_sha256:
                    raise ValueError('Immutable batch hash conflict.')
                return RetailBatch.model_validate({k:existing[k] for k in RetailBatch.model_fields})
            if batch.sequence != run['sequence'] + 1 or batch.logical_end <= run['logical_end']:
                raise ValueError('Batch sequence or time does not follow the source cursor.')
            if conn.execute("SELECT 1 FROM pricing.retail_batches WHERE run_id=%s AND status!='acknowledged' LIMIT 1",(batch.run_id,)).fetchone():
                raise ValueError('Deliver the pending batch before advancing source state.')
            grouped = {}
            for r in rows:
                if r.get('branch','applied') not in ('applied','comparison'):
                    raise ValueError('Unknown sales branch.')
                if r.get('branch')=='comparison':
                    continue
                grouped.setdefault((r['store_id'],r['sku']),[]).append(r)
            keys=sorted(grouped)
            stores=[k[0] for k in keys]; products=[k[1] for k in keys]
            locked=conn.execute('SELECT * FROM pricing.retail_state WHERE run_id=%s AND (store_id,sku) IN (SELECT * FROM unnest(%s::integer[],%s::integer[])) ORDER BY store_id,sku FOR UPDATE',(batch.run_id,stores,products)).fetchall()
            states={(r['store_id'],r['sku']):r for r in locked}
            stocks=[]
            for (store,sku), events in sorted(grouped.items()):
                current = states.get((store,sku))
                if not current:
                    raise ValueError('Source product was deleted.')
                for r in events:
                    if r['inventory_version'] != current['inventory_version'] or (r['quantity'] > 0 and (r['price_version'] != current['price_version'] or r['price_minor'] != current['current_price_minor'])):
                        raise UncommittedBatchConflict('Source version changed; regenerate this uncommitted batch.')
                    if r['quantity'] < 0:
                        self._claim_return(conn, batch, r)
                    elif r['quantity'] == 0:
                        raise ValueError('A sales line must have a nonzero quantity.')
                    if r['receipts_minor'] != r['price_minor'] * r['quantity'] or r['sold_cost_minor'] != current['unit_cost_minor'] * r['quantity']:
                        raise ValueError('Sales arithmetic differs from source state.')
                stock = current['stock_units'] - sum(r['quantity'] for r in events)
                if stock < 0:
                    raise ValueError('Insufficient stock.')
                stocks.append(stock)
            conn.execute("""WITH updated AS (
                UPDATE pricing.retail_state s SET stock_units=d.stock_units,inventory_version=s.inventory_version+1
                FROM unnest(%s::integer[],%s::integer[],%s::integer[]) d(store_id,sku,stock_units)
                WHERE s.run_id=%s AND s.store_id=d.store_id AND s.sku=d.sku
                RETURNING s.run_id,s.store_id,s.sku,s.inventory_version,s.stock_units,s.expires_at)
                INSERT INTO pricing.retail_inventory_history SELECT *,%s FROM updated""",(stores,products,stocks,batch.run_id,batch.sequence))
            self._advance_comparisons(conn,batch,rows)
            conn.execute('INSERT INTO pricing.retail_batches(batch_id,run_id,sequence,payload_sha256,event_count,transaction_count,logical_end,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)',tuple(getattr(batch,k) for k in ('batch_id','run_id','sequence','payload_sha256','event_count','transaction_count','logical_end','created_at')))
            conn.execute('UPDATE pricing.retail_runs SET sequence=%s,logical_end=%s WHERE run_id=%s',(batch.sequence,batch.logical_end,batch.run_id))
        return batch

    def pending(self, run_id):
        with self.ledger.connect() as conn:
            rows = conn.execute("SELECT * FROM pricing.retail_batches WHERE run_id=%s AND status='pending' ORDER BY sequence",(run_id,)).fetchall()
        return [RetailBatch.model_validate({k:r[k] for k in RetailBatch.model_fields}) for r in rows]

    def acknowledge(self, batch):
        with self.ledger.connect() as conn:
            conn.execute("UPDATE pricing.retail_batches SET status='acknowledged',acknowledged_at=now() WHERE batch_id=%s AND payload_sha256=%s",(batch.batch_id,batch.payload_sha256))
