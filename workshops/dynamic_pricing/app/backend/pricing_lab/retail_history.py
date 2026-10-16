"""Verified immutable history activation and separately taught minute aggregates."""
from contextlib import contextmanager
from datetime import datetime
import hashlib
import json
from pathlib import Path
from urllib.parse import urljoin
from uuid import UUID, uuid4

import pyarrow.parquet as pq
from psycopg.types.json import Jsonb

from .analytics import identifier
from .history import resolve_manifest
from .retail_schema import install_retail_schema, create_rollup_target, rollup_select


BACKFILL_SETTINGS = ('max_threads=1, max_bytes_before_external_group_by=1000000000, '
                     'max_block_size=8192, max_insert_block_size=8192, '
                     'min_insert_block_size_rows=8192, min_insert_block_size_bytes=1000000, '
                     'aggregation_memory_efficient_merge_threads=1')


VALIDATION_SETTINGS = 'max_threads=4, max_bytes_before_external_group_by=1000000000'


class RetailHistory:
    def __init__(self, analytics, ledger):
        self.analytics, self.ledger = analytics, ledger
        self.db = identifier(analytics.database)

    @contextmanager
    def _guard(self):
        with self.ledger.connect() as conn:
            conn.autocommit = True
            if not conn.execute("SELECT pg_try_advisory_lock(84927641) locked").fetchone()['locked']:
                raise ValueError('Wait for the active retail operation to finish.')
            try:
                    yield conn
            finally:
                conn.execute('SELECT pg_advisory_unlock(84927641)')

    def _state(self, conn):
        row = conn.execute('SELECT data FROM pricing.retail_history_state WHERE id').fetchone()
        return row['data'] if row else {}

    def _save(self, conn, state):
        conn.execute('INSERT INTO pricing.retail_history_state(data) VALUES (%s) ON CONFLICT(id) DO UPDATE SET data=excluded.data', (Jsonb(state),))

    def _totals(self, table, batch=None):
        where = ' WHERE batch_id={batch:UUID}' if batch else ''
        return self.analytics.query(f'''SELECT count() rows, sum(quantity) quantity,
          sum(receipts_minor) receipts_minor, sum(sold_cost_minor) sold_cost_minor
          FROM {self.db}.{identifier(table)}{where}''', {'batch': batch} if batch else {})[0]

    def _check(self, actual, expected):
        if any(actual[k] != expected[k] for k in ('rows', 'quantity', 'receipts_minor', 'sold_cost_minor')):
            raise ValueError('Historical totals differ from the verified manifest; no data was activated.')

    def _verify_facts(self, manifest, table="retail_sales"):
        table = identifier(table)
        cutoff = datetime.fromisoformat(manifest['history_cutover'])
        start = datetime.fromisoformat(manifest['history_start'])
        invalid = self.analytics.query(f"""SELECT count() n FROM {self.db}.{table}
          WHERE store_id NOT BETWEEN 1 AND 5000 OR sku NOT BETWEEN 101 AND 120
          OR district_id != intDiv(store_id-1,250)+1 OR event_time < {{start:DateTime64(3)}}
          OR event_time >= {{cutoff:DateTime64(3)}} OR toHour(event_time) NOT BETWEEN 2 AND 9
          OR source != 'history' OR branch != 'applied' OR sequence != 0
          OR receipts_minor != quantity * price_minor""", {'start': start, 'cutoff': cutoff})[0]['n']
        if invalid:
            raise ValueError('Historical facts are outside the manifest catalog, time, or arithmetic bounds.')

    def _insert_shard(self, table, source, file, batch):
        if isinstance(source, Path):
            arrow = pq.read_table(source / file['key'])
            columns = arrow.column_names + ['batch_id']
            rows = [[UUID(v) if k in ('event_id','transaction_id','original_event_id') else v
                     for k, v in row.items()] + [batch] for row in arrow.to_pylist()]
            self.analytics.client.insert(f'{self.db}.{identifier(table)}', rows, column_names=columns)
        else:
            self.analytics.command(f'''INSERT INTO {self.db}.{identifier(table)}
              (event_id, transaction_id, line_number, store_id, district_id, sku, quantity,
               price_minor, receipts_minor, sold_cost_minor, event_time, original_event_id, batch_id)
              SELECT toUUID(event_id), toUUID(transaction_id), line_number, store_id, district_id, sku,
               quantity, price_minor, receipts_minor, sold_cost_minor, event_time, toUUID(original_event_id),
               {{batch:UUID}} FROM s3({{url:String}}, 'NOSIGN', 'Parquet')''',
              {'batch': batch, 'url': urljoin(source, file['key'])})

    def _dimensions(self, source):
        for file, table in [('stores.parquet','retail_stores'), ('products.parquet','retail_products'),
                            ('availability.parquet','retail_availability')]:
            stage = table + '_stage'
            self.analytics.command(f'DROP TABLE IF EXISTS {self.db}.{stage} SYNC')
            self.analytics.command(f'CREATE TABLE {self.db}.{stage} AS {self.db}.{table}')
            if isinstance(source, Path):
                arrow = pq.read_table(source / file)
                self.analytics.client.insert_arrow(f'{self.db}.{stage}', arrow)
            else:
                self.analytics.command(f"INSERT INTO {self.db}.{stage} SELECT * FROM s3({{url:String}}, 'NOSIGN', 'Parquet')", {'url': urljoin(source, file)})
            self.analytics.command(f'EXCHANGE TABLES {self.db}.{table} AND {self.db}.{stage}')
            self.analytics.command(f'DROP TABLE {self.db}.{stage} SYNC')

    def _manifest(self, uri):
        manifest, source = resolve_manifest(uri, expected_version='retail-chain-v2')
        if manifest['dataset_version'] != 'retail-chain-v2':
            raise ValueError('Use the chain-wide retail dataset.')
        digest = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
        total = {k: sum(f[k] for f in manifest['files']) for k in ('rows','quantity','receipts_minor','sold_cost_minor')}
        return manifest, source, digest, total

    def load(self, manifest_uri):
        manifest, source, digest, total = self._manifest(manifest_uri)
        with self._guard() as conn:
            install_retail_schema(self.analytics)
            state = self._state(conn)
            if state.get('digest') and state['digest'] != digest:
                raise ValueError('A different history is registered; use a fresh workshop database.')
            if state.get('active'):
                return {**total, 'registered': True, 'digest': digest}
            stage = 'retail_history_' + digest[:20]
            self.analytics.command(f'CREATE TABLE IF NOT EXISTS {self.db}.{stage} AS {self.db}.retail_sales')
            self._save(conn, dict(digest=digest, manifest=manifest, active=False))
            # An exchange may have succeeded even if its acknowledgment was lost.
            if self._totals('retail_sales')['rows'] == 0:
                for file in manifest['files']:
                    batch = UUID(file['sha256'][:32])
                    actual = self._totals(stage, batch)
                    if actual['rows'] == 0:
                        print(f"Loading {file['key']} ({file['rows']:,} rows)", flush=True)
                        self._insert_shard(stage, source, file, batch)
                        actual = self._totals(stage, batch)
                    self._check(actual, file)
                self._check(self._totals(stage), total)
                self.analytics.command(f'EXCHANGE TABLES {self.db}.retail_sales AND {self.db}.{stage}')
            self._check(self._totals('retail_sales'), total)
            self._verify_facts(manifest)
            self._dimensions(source)
            self._save(conn, dict(digest=digest, manifest=manifest, active=True, rollup=False))
            self.analytics.command(f'DROP TABLE IF EXISTS {self.db}.{stage} SYNC')
            return {**total, 'registered': True, 'digest': digest}

    def register_console(self, manifest_uri):
        manifest, source, digest, total = self._manifest(manifest_uri)
        with self._guard() as conn:
            install_retail_schema(self.analytics)
            state = self._state(conn)
            if state.get('digest') and state['digest'] != digest:
                raise ValueError('A different history is registered.')
            recovered = state.get('console_pending') and self._totals('retail_sales_console')['rows'] == 0
            table = 'retail_sales' if state.get('active') or recovered else 'retail_sales_console'
            self._check(self._totals(table), total)
            self._verify_facts(manifest, table)
            if table == 'retail_sales_console':
                if self._totals('retail_sales')['rows']:
                    raise ValueError('Register history before starting sales.')
                self._save(conn, state | dict(digest=digest, manifest=manifest, console_pending=True))
                self.analytics.command(f'EXCHANGE TABLES {self.db}.retail_sales_console AND {self.db}.retail_sales')
            self._dimensions(source)
            self._save(conn, state | dict(manifest=manifest, digest=digest, active=True, console_pending=False))
        return {**total, 'registered': True, 'digest': digest}

    def create_rollup(self):
        with self._guard() as conn:
            state = self._state(conn)
            if not state.get('active'):
                raise ValueError('Load and register history before creating aggregates.')
            if state.get('rollup'):
                return {'ready': True}
            if self.analytics.query(f"SELECT count() n FROM {self.db}.retail_sales WHERE source != 'history'")[0]['n']:
                raise ValueError('Create aggregates before starting live sales.')
            cutoff = datetime.fromisoformat(state['manifest']['history_cutover'])
            raw = self._totals('retail_sales')
            for target, store in [('retail_sales_minute', True), ('retail_district_minute', False)]:
                print(f'Backfilling {target} from the historical sales. This can take several minutes.', flush=True)
                stage = target + '_stage'
                self.analytics.command(f'DROP TABLE IF EXISTS {self.db}.{target}_mv SYNC')
                self.analytics.command(f'DROP TABLE IF EXISTS {self.db}.{stage} SYNC')
                create_rollup_target(self.analytics, stage, store)
                select, keys = rollup_select(self.db, store)
                self.analytics.command(f"INSERT INTO {self.db}.{stage} {select} WHERE source='history' AND event_time < {{cutoff:DateTime64(3)}} GROUP BY {keys} SETTINGS {BACKFILL_SETTINGS}", {'cutoff': cutoff})
                sums = self.analytics.query(f'''SELECT countMerge(rows_state) rows, sumMerge(units_state) quantity,
                  sumMerge(receipts_state) receipts_minor, sumMerge(cost_state) sold_cost_minor FROM {self.db}.{stage}''')[0]
                self._check(sums, raw)
                self.analytics.command(f'DROP TABLE IF EXISTS {self.db}.{target} SYNC')
                self.analytics.command(f'RENAME TABLE {self.db}.{stage} TO {self.db}.{target}')
                self.analytics.command(f'CREATE MATERIALIZED VIEW {self.db}.{target}_mv TO {self.db}.{target} AS {select} GROUP BY {keys}')
            self._save(conn, state | {'rollup': True})
            return {'ready': True}

    def register_rollup(self):
        """Verify console-built aggregates before attaching the canonical insert views."""
        with self._guard() as conn:
            state=self._state(conn)
            if not state.get('active'):raise ValueError('Load and register history first.')
            if state.get('rollup'):return {'ready':True}
            if self.analytics.query(f"SELECT count() n FROM {self.db}.retail_sales WHERE source!='history'")[0]['n']:
                raise ValueError('Register aggregates before starting live sales.')
            for target,store in [('retail_sales_minute',True),('retail_district_minute',False)]:
                template=target+'_schema_check'
                self.analytics.command(f'DROP TABLE IF EXISTS {self.db}.{template} SYNC')
                create_rollup_target(self.analytics,template,store)
                try:
                    columns=lambda name:[(r['name'],r['type']) for r in self.analytics.query(f'DESCRIBE TABLE {self.db}.{name}')]
                    if columns(target)!=columns(template):raise ValueError('Aggregate schema differs from the workshop schema.')
                    shape=lambda name:self.analytics.query('SELECT engine,sorting_key,partition_key FROM system.tables WHERE database={db:String} AND name={name:String}',{'db':self.db,'name':name})
                    if shape(target)!=shape(template):raise ValueError('Aggregate engine or sort key differs from the workshop schema.')
                finally:self.analytics.command(f'DROP TABLE {self.db}.{template} SYNC')
                keys='district_id,'+('store_id,' if store else '')+'sku,run_id,branch,source,minute'
                raw=f'''SELECT {keys},sum(toInt64(quantity)) units,sum(toInt64(greatest(quantity,0))) sold,
                  sum(receipts_minor) receipts,sum(sold_cost_minor) cost,count() rows,uniqExact(transaction_id) transactions,max(sequence) sequence
                  FROM (SELECT *,toStartOfMinute(event_time) minute FROM {self.db}.retail_sales WHERE district_id={{district:UInt16}}) GROUP BY {keys}'''
                rollup=f'''SELECT {keys},sumMerge(units_state) units,sumMerge(sold_units_state) sold,
                  sumMerge(receipts_state) receipts,sumMerge(cost_state) cost,countMerge(rows_state) rows,
                  uniqExactMerge(transactions_state) transactions,maxMerge(sequence_state) sequence
                  FROM {self.db}.{target} WHERE district_id={{district:UInt16}} GROUP BY {keys}'''
                if self.analytics.query(f'SELECT count() n FROM {self.db}.{target} WHERE district_id NOT BETWEEN 1 AND 20')[0]['n']:
                    raise ValueError('Aggregate grouped facts contain an unknown district.')
                # Bound EXCEPT's hash table as well as aggregation memory. The sort-key
                # prefix prunes each district without repeatedly scanning the whole chain.
                for district in range(1,21):
                    if district in (1,5,10,15,20):
                        print(f'Verifying {target}: district {district} of 20.', flush=True)
                    for left,right in [(raw,rollup),(rollup,raw)]:
                        if self.analytics.query(f'SELECT count() n FROM (({left}) EXCEPT ALL ({right})) SETTINGS {VALIDATION_SETTINGS}',{'district':district})[0]['n']:
                            raise ValueError('Aggregate grouped facts differ from sales; correct the backfill before registering.')
            for target,store in [('retail_sales_minute',True),('retail_district_minute',False)]:
                select,keys=rollup_select(self.db,store)
                self.analytics.command(f'DROP TABLE IF EXISTS {self.db}.{target}_mv SYNC')
                self.analytics.command(f'CREATE MATERIALIZED VIEW {self.db}.{target}_mv TO {self.db}.{target} AS {select} GROUP BY {keys}')
            self._save(conn,state|{'rollup':True})
            return {'ready':True}
