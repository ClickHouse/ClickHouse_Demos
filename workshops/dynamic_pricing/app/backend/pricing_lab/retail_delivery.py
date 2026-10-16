"""Synchronous, bounded batch delivery with independent raw and rollup reconciliation."""
from datetime import UTC, datetime
from .retail_source import payload_bytes


class RetailDelivery:
    def __init__(self, analytics, source):
        self.analytics, self.source = analytics, source
        self.db = analytics.database

    def _insert(self, batch, rows):
        table = f'{self.db}.retail_sales'
        columns = [k for k in rows[0] if k != 'inventory_version']
        extra = ['run_id','batch_id','sequence','source','emitted_at']
        if 'branch' not in columns:
            columns.append('branch')
        values = [[r.get(k,'applied') if k=='branch' else r[k] for k in columns] + [batch.run_id,batch.batch_id,batch.sequence,'live',batch.created_at] for r in rows]
        engine = self.analytics.query('SELECT engine FROM system.tables WHERE database={db:String} AND name=\'retail_sales\'', {'db':self.db})[0]['engine']
        if engine == 'MergeTree':
            self.analytics.command(f'ALTER TABLE {table} MODIFY SETTING non_replicated_deduplication_window=10000')
        settings = {'async_insert':0, 'insert_deduplicate':1,
                    'insert_deduplication_token':str(batch.batch_id), 'deduplicate_blocks_in_dependent_materialized_views':1}
        self.analytics.client.insert(table, values, column_names=columns+extra, settings=settings)

    def _raw(self, batch):
        return self.analytics.query(f'''SELECT count() rows, uniqExact(event_id) unique_rows,
          sum(quantity) units, sum(receipts_minor) receipts, sum(sold_cost_minor) costs,
          uniqExact(transaction_id) transactions FROM {self.db}.retail_sales
          WHERE run_id={{run:UUID}} AND batch_id={{batch:UUID}}''', {'run':batch.run_id,'batch':batch.batch_id})[0]

    def _verify_rollups(self, batch):
        params={'run':batch.run_id}
        raw=self.analytics.query(f'''SELECT count() rows, sum(quantity) units, sum(receipts_minor) receipts,
          sum(sold_cost_minor) costs, uniqExact(transaction_id) transactions FROM {self.db}.retail_sales
          WHERE run_id={{run:UUID}} AND source='live' ''',params)[0]
        for table in ('retail_sales_minute','retail_district_minute'):
            aggregate=self.analytics.query(f'''SELECT countMerge(rows_state) rows, sumMerge(units_state) units,
              sumMerge(receipts_state) receipts, sumMerge(cost_state) costs,
              uniqExactMerge(transactions_state) transactions FROM {self.db}.{table}
              WHERE run_id={{run:UUID}} AND source='live' ''',params)[0]
            if aggregate != raw:
                self.source.hold(batch.run_id)
                raise ValueError('Raw sales and aggregates differ; run held for reconciliation.')

    def deliver(self, batch):
        # Serialize delivery across processes, including after a client timeout.
        with self.source.ledger.connect() as guard:
            guard.autocommit=True
            guard.execute('SELECT pg_advisory_lock(hashtextextended(%s,0))',(str(batch.run_id),))
            try:
                metadata=guard.execute('SELECT * FROM pricing.retail_batches WHERE batch_id=%s',(batch.batch_id,)).fetchone()
                if not metadata or metadata['payload_sha256'] != batch.payload_sha256:
                    raise ValueError('Committed source batch is missing or changed.')
                rows=self.source.payload(batch)
                expected=dict(rows=len(rows),unique_rows=len(rows),units=sum(r['quantity'] for r in rows),
                              receipts=sum(r['receipts_minor'] for r in rows),costs=sum(r['sold_cost_minor'] for r in rows),
                              transactions=len({r['transaction_id'] for r in rows}))
                actual=self._raw(batch)
                if actual['rows'] == 0 and rows:
                    if (datetime.now(UTC)-batch.created_at).total_seconds() > 300:
                        self.source.hold(batch.run_id)
                        raise ValueError('Delivery retry window expired; run held without reinserting.')
                    self._insert(batch,rows)
                    actual=self._raw(batch)
                if actual != expected:
                    self.source.hold(batch.run_id)
                    raise ValueError('Partial or mismatched delivery; run held without reinserting.')
                self._verify_identity(batch, rows)
                self._verify_rollups(batch)
                self.source.acknowledge(batch)
                return {'status':'acknowledged','batch_id':str(batch.batch_id),'rows':len(rows)}
            finally:
                guard.execute('SELECT pg_advisory_unlock(hashtextextended(%s,0))',(str(batch.run_id),))

    def _verify_identity(self, batch, rows):
        if not rows:
            if self._raw(batch)['rows']:
                raise ValueError('An empty clock batch contains unexpected sales.')
            return
        fields = [k for k in rows[0] if k != 'inventory_version']
        stored = self.analytics.query(f"SELECT {','.join(fields)} FROM {self.db}.retail_sales WHERE run_id={{run:UUID}} AND batch_id={{batch:UUID}}", {'run':batch.run_id,'batch':batch.batch_id})
        expected_rows = [{k:r[k] for k in fields} for r in rows]
        canonical = lambda values: sorted(payload_bytes([r]) for r in values)
        if canonical(stored) != canonical(expected_rows):
            self.source.hold(batch.run_id)
            raise ValueError('Delivered event identity or fields differ from the stored payload.')

    def rebuild_rollups(self, run_id):
        from .retail_contracts import RetailBatch
        from .retail_schema import create_rollup_target, rollup_select
        from .retail_history import BACKFILL_SETTINGS
        with self.source.ledger.connect() as guard:
            guard.autocommit=True
            if not guard.execute('SELECT pg_try_advisory_lock(84927642) locked').fetchone()['locked']:
                raise ValueError('Stop the writer before rebuilding aggregates.')
            guard.execute('SELECT pg_advisory_lock(hashtextextended(%s,0))',(str(run_id),))
            try:
                if self.source.run(run_id)['status'] != 'held':
                    raise ValueError('Rebuild is available for a held run with the writer stopped.')
                batches=guard.execute('SELECT * FROM pricing.retail_batches WHERE run_id=%s ORDER BY sequence',(run_id,)).fetchall()
                for metadata in batches:
                    batch=RetailBatch.model_validate({k:metadata[k] for k in RetailBatch.model_fields})
                    self._verify_identity(batch,self.source.payload(batch))
                raw=self.analytics.query(f'SELECT count() rows,sum(quantity) units,sum(receipts_minor) receipts,sum(sold_cost_minor) costs,uniqExact(transaction_id) transactions FROM {self.db}.retail_sales')[0]
                for table,store in [('retail_sales_minute',True),('retail_district_minute',False)]:
                    stage=table+'_repair'
                    self.analytics.command(f'DROP TABLE IF EXISTS {self.db}.{stage} SYNC')
                    create_rollup_target(self.analytics,stage,store)
                    select,keys=rollup_select(self.db,store)
                    self.analytics.command(f'INSERT INTO {self.db}.{stage} {select} GROUP BY {keys} SETTINGS {BACKFILL_SETTINGS}')
                    aggregate=self.analytics.query(f'SELECT countMerge(rows_state) rows,sumMerge(units_state) units,sumMerge(receipts_state) receipts,sumMerge(cost_state) costs,uniqExactMerge(transactions_state) transactions FROM {self.db}.{stage}')[0]
                    if aggregate != raw:
                        raise ValueError('Rebuilt aggregate differs from verified raw sales.')
                    self.analytics.command(f'DROP TABLE IF EXISTS {self.db}.{table}_mv SYNC')
                    self.analytics.command(f'EXCHANGE TABLES {self.db}.{table} AND {self.db}.{stage}')
                    self.analytics.command(f'CREATE MATERIALIZED VIEW {self.db}.{table}_mv TO {self.db}.{table} AS {select} GROUP BY {keys}')
                    self.analytics.command(f'DROP TABLE {self.db}.{stage} SYNC')
                guard.execute("UPDATE pricing.retail_runs SET status='stopped' WHERE run_id=%s",(run_id,))
                return {'status':'stopped'}
            finally:
                guard.execute('SELECT pg_advisory_unlock(hashtextextended(%s,0))',(str(run_id),))
                guard.execute('SELECT pg_advisory_unlock(84927642)')
