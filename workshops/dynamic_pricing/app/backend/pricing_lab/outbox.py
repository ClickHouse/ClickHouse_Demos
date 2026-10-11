"""Bounded analytical delivery; Postgres remains the authority after failures."""
from datetime import datetime
import json
from psycopg.types.json import Jsonb
from .analytics import AnalyticsClient
from .observability import start_span,trace_link
from .contracts import Scope,Snapshot


class OutboxWorker:
    def __init__(self,ledger,analytics,clock):
        self.ledger,self.clock=ledger,clock
        # Bulk loading can take longer; evidence retries use a separate bounded client.
        self.analytics=AnalyticsClient(analytics.settings,request_timeout_seconds=10)

    def drain(self,scope:Scope,limit:int=50):
        if type(limit) is not int or not 1<=limit<=200: raise ValueError('Deliver between 1 and 200 events per batch.')
        scope_json=Jsonb(scope.model_dump(mode='json'))
        with self.ledger.connect() as conn:
            rows=conn.execute("""SELECT event_id FROM pricing.outbox
              WHERE delivered_at IS NULL AND attempts<8 AND (data->'scope'=%s OR data->'snapshot'->'scope'=%s)
              ORDER BY event_id LIMIT %s""",(scope_json,scope_json,limit)).fetchall()
        counts={'delivered':0,'failed':0}
        for item in rows:
            with self.ledger.connect() as conn:
                row=conn.execute('SELECT * FROM pricing.outbox WHERE event_id=%s AND delivered_at IS NULL AND attempts<8 FOR UPDATE SKIP LOCKED',(item['event_id'],)).fetchone()
                if not row: continue
                try:
                    links=[trace_link(row['data']['trace_context'])] if row['data'].get('trace_context') else []
                    with start_span('pricing.analytics.delivery',{'pricing.owner_id':scope.owner_id,'pricing.generation':scope.generation,'pricing.event_id':str(row['event_id'])},links=links):
                        self._deliver(row)
                except Exception:
                    counts['failed']+=1
                    conn.execute('UPDATE pricing.outbox SET attempts=attempts+1 WHERE event_id=%s',(row['event_id'],))
                else:
                    counts['delivered']+=1
                    conn.execute('UPDATE pricing.outbox SET attempts=attempts+1,delivered_at=%s WHERE event_id=%s',(self.clock.wall_now(),row['event_id']))
        return counts

    def _deliver(self,row):
        payload=row['data'];db=self.analytics.database
        if row['kind']=='snapshot':
            s=Snapshot.model_validate(payload['snapshot'])
            self.analytics.client.insert(f'{db}.input_snapshots',[[s.snapshot_id,s.scope.sandbox_id,s.scope.generation,
                s.subject.sku,s.subject.store_id,s.scenario_cutoff,s.captured_at,s.stock_units,s.current_price_minor,
                s.model_dump_json(),json.dumps(payload['evidence'],sort_keys=True),1]])
        else:
            s=Scope.model_validate(payload['scope'])
            self.analytics.client.insert(f'{db}.evidence_events',[[row['event_id'],s.sandbox_id,s.generation,row['kind'],
                datetime.fromisoformat(payload['occurred_at']),json.dumps(payload['payload'],sort_keys=True),1]])

    def close(self): self.analytics.close()
