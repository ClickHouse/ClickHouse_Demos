"""Known teaching checkpoints restore business state into a fresh generation."""
from datetime import datetime,timedelta
import json
from uuid import uuid4
from psycopg.types.json import Jsonb
from .contracts import Scope,Subject,WorkshopError,StaleGeneration
from .ledger import lock_scope,scope_key,subject_key,SUBJECT_WHERE,SCOPE_WHERE,append_inventory
from .model import DemandModel
from .settings import LAB_ROOT
from .simulator import simulate

CHECKPOINTS=('lab1_baseline','lab2_features','lab3_policy','lab4_exception','lab5_review')
EVENTS=('near_expiry','stale_inventory','rule_conflict','unsupported_subject','expired_candidate')


class ScenarioService:
    def __init__(self,source,ledger,features,clock,model=None):
        self.source,self.ledger,self.features,self.clock=source,ledger,features,clock
        self.model=model

    def _prepared(self,scope,subject,prices):
        snapshot=self.features.capture(scope,subject)
        from .settings import Settings
        model=self.model or DemandModel.from_artifact(Settings.from_env().model_path)
        candidates=simulate(snapshot,model,prices,17,1024)
        self.ledger.store_candidates(candidates)
        return candidates

    def reset(self,scope,checkpoint,idempotency_key):
        if checkpoint not in CHECKPOINTS: raise WorkshopError('checkpoint_unknown','Choose a checkpoint listed in the workshop.')
        fixture=json.loads((LAB_ROOT/'fixtures/checkpoints'/f'{checkpoint}.json').read_text())
        with self.ledger.connect() as conn:
            scenario=conn.execute('SELECT * FROM pricing.scenarios WHERE owner_id=%s AND scenario_id=%s FOR UPDATE',(scope.owner_id,scope.scenario_id)).fetchone()
            previous=conn.execute('SELECT * FROM pricing.resets WHERE owner_id=%s AND scenario_id=%s AND idempotency_key=%s',(scope.owner_id,scope.scenario_id,idempotency_key)).fetchone()
            if previous:
                if previous['checkpoint']!=checkpoint: raise WorkshopError('idempotency_conflict','Use a new reset key for another checkpoint.')
                restored=scope.model_copy(update={'generation':previous['to_generation']})
                if scenario['generation']!=restored.generation: raise WorkshopError('reset_superseded','A later reset replaced this request. Refresh the current generation.')
            else:
                if not scenario or scenario['generation']!=scope.generation: raise StaleGeneration()
                restored=scope.model_copy(update={'generation':scope.generation+1})
                cutoff=datetime.fromisoformat(fixture['scenario_cutoff'])
                conn.execute('UPDATE pricing.scenarios SET generation=%s,scenario_cutoff=%s WHERE owner_id=%s AND scenario_id=%s',
                             (restored.generation,cutoff,scope.owner_id,scope.scenario_id))
                self.ledger.initialize_in_transaction(conn,restored,Subject.model_validate(fixture['subject']),fixture['state'],fixture['rules'],cutoff)
                conn.execute('INSERT INTO pricing.resets VALUES(%s,%s,%s,%s,%s,%s)',
                             (scope.owner_id,scope.scenario_id,idempotency_key,scope.generation,restored.generation,checkpoint))
        self.clock.restore_scenario(datetime.fromisoformat(fixture['scenario_cutoff']))
        subject=Subject.model_validate(fixture['subject'])
        if fixture['requires_cdc']:
            state=self.ledger.current_state(restored,subject)
            self.features.await_revisions(restored,{'inventory':str(state['inventory_revision_id'])},self.features.timeout_seconds)
            guard=self.ledger.connect()
            guard.autocommit=True
            guard_key=f"prepared/{restored.sandbox_id}/{restored.generation}"
            try:
                guard.execute('SELECT pg_advisory_lock(hashtextextended(%s,0))',(guard_key,))
                count=guard.execute(f"SELECT count(*) AS n FROM pricing.candidates WHERE {SCOPE_WHERE} AND (data->>'expires_at')::timestamptz>%s AND data->'snapshot'->'source_versions'->>'inventory'=%s AND (data->'snapshot'->>'applied_price_version')::int=%s",
                    (*scope_key(restored),self.clock.wall_now(),str(state['inventory_revision_id']),state['applied_price_version'])).fetchone()['n']
                if not count: self._prepared(restored,subject,fixture['prepared_prices_minor'])
            finally:
                try: guard.execute('SELECT pg_advisory_unlock(hashtextextended(%s,0))',(guard_key,))
                finally: guard.close()

        return restored

    def trigger(self,scope,event,event_id):
        if event not in EVENTS: raise WorkshopError('scenario_unknown','Choose a scenario event listed in the workshop.')
        subject=Subject(sku=101,store_id=1)
        with self.ledger.connect() as conn:
            scenario=lock_scope(conn,scope)
            state=conn.execute(f'SELECT * FROM pricing.subject_state WHERE {SUBJECT_WHERE} FOR UPDATE',subject_key(scope,subject)).fetchone()
            existing=conn.execute(f'SELECT * FROM pricing.scenario_events WHERE {SCOPE_WHERE} AND event_id=%s',(*scope_key(scope),event_id)).fetchone()
            if existing:
                if existing['event_kind']!=event: raise WorkshopError('event_conflict','Use a new event ID for another scenario.')
                return existing['data']
            cutoff=scenario['scenario_cutoff'];data={'event':event,'mode':'synthetic','scope':scope.model_dump(mode='json')}
            if event in ('near_expiry','stale_inventory'):
                state['inventory_revision_id']=uuid4();state['inventory_version']+=1
                if event=='near_expiry': state['inventory_expires_at']=cutoff+timedelta(minutes=10)
                conn.execute(f'UPDATE pricing.subject_state SET inventory_revision_id=%s,inventory_version=%s,inventory_expires_at=%s WHERE {SUBJECT_WHERE}',
                             (state['inventory_revision_id'],state['inventory_version'],state['inventory_expires_at'],*subject_key(scope,subject)))
                append_inventory(conn,scope,subject,state,cutoff-timedelta(seconds=130) if event=='stale_inventory' else cutoff)
                data['inventory_revision_id']=str(state['inventory_revision_id'])
            elif event=='rule_conflict':
                rules={**state['rules'],'version':state['rule_version']+1,'floor_minor':850}
                conn.execute(f'UPDATE pricing.subject_state SET rule_version=%s,rules=%s WHERE {SUBJECT_WHERE}',(rules['version'],Jsonb(rules),*subject_key(scope,subject)))
                data['rule_version']=rules['version']
            elif event=='unsupported_subject':
                unknown=Subject(sku=999,store_id=1)
                self.ledger.initialize_in_transaction(conn,scope,unknown,state,state['rules'],cutoff)
                data['subject']=unknown.model_dump(mode='json')
            elif event=='expired_candidate':
                row=conn.execute(f'SELECT data FROM pricing.snapshots WHERE {SCOPE_WHERE} LIMIT 1',scope_key(scope)).fetchone()
                if not row: raise WorkshopError('snapshot_missing','Capture a proposal before running the expired-candidate exercise.')
                from .contracts import Snapshot,Candidate
                snapshot=Snapshot.model_validate(row['data']);now=self.clock.wall_now()
                candidate=Candidate(candidate_id=uuid4(),snapshot=snapshot,model_version='expired-teaching-fixture',price_minor=snapshot.current_price_minor,
                    created_at=now-timedelta(minutes=61),expires_at=now-timedelta(minutes=1),projection={'parameter_advantage_min_minor':0,'parameter_advantage_max_minor':0})
                conn.execute('INSERT INTO pricing.candidates VALUES(%s,%s,%s,%s,%s)',(candidate.candidate_id,*scope_key(scope),Jsonb(candidate.model_dump(mode='json'))))
                data['candidate_id']=str(candidate.candidate_id);data['label']='Expired teaching fixture; not an applicable proposal'
            conn.execute('INSERT INTO pricing.scenario_events VALUES(%s,%s,%s,%s,%s,%s)',(*scope_key(scope),event_id,event,Jsonb(data)))
        return data
