"""Learner-authorized mock application with authoritative idempotent receipts."""
from .observability import traced, propagation_headers
from uuid import uuid4
from psycopg.types.json import Jsonb
from .contracts import Receipt,WorkshopError
from .ledger import lock_scope,scope_key,subject_key,SUBJECT_WHERE
from .policy import validate_candidate


def queue_event(conn,kind,scope,at,payload,event_id=None):
    event_id=event_id or uuid4()
    data={'scope':scope.model_dump(mode='json'),'occurred_at':at.isoformat(),'payload':payload,'trace_context':propagation_headers()}
    conn.execute('INSERT INTO pricing.outbox(event_id,kind,data) VALUES(%s,%s,%s)',(event_id,kind,Jsonb(data)))


class MockPublisher:
    def __init__(self,ledger,clock,model_versions=None):
        if model_versions is None:
            from .model import DemandModel
            from .settings import LAB_ROOT
            model_versions=[DemandModel.from_artifact(LAB_ROOT/"pricing/artifacts/development/model.json").version]
        self.ledger,self.clock,self.model_versions=ledger,clock,tuple(model_versions)

    def _authorize(self,scope,reviewer):
        if reviewer!=scope.owner_id:
            raise WorkshopError('reviewer_forbidden','Sign in as the learner who owns this instance.')

    def _lock(self,conn,scope,candidate,key=None):
        scenario=lock_scope(conn,scope)
        if key:
            conn.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',(scope.owner_id+'/'+str(key),))
            previous=conn.execute('SELECT * FROM pricing.application_attempts WHERE owner_id=%s AND idempotency_key=%s',
                                  (scope.owner_id,key)).fetchone()
            if previous and previous['candidate_id']!=candidate.candidate_id:
                raise WorkshopError('idempotency_conflict','Use a new request key for this proposal.')
        state=conn.execute(f'SELECT * FROM pricing.subject_state WHERE {SUBJECT_WHERE} FOR UPDATE',subject_key(scope,candidate.snapshot.subject)).fetchone()
        if not state: raise WorkshopError('subject_missing','Create a fresh proposal from a current subject.')
        return scenario,state

    def _policy(self,conn,scope,candidate,scenario,state):
        snapshot=candidate.snapshot
        trusted={**state,'scope':scope,'subject':snapshot.subject,
                 'source_versions':{'inventory':str(state['inventory_revision_id']),'price':str(state['applied_price_version']),'cost':str(state['cost_version'])},
                 'candidate_ids':[candidate.candidate_id],'model_versions':self.model_versions}
        policy=validate_candidate(candidate,trusted,state['rules'],self.clock.wall_now())
        if scenario['scenario_cutoff']!=snapshot.scenario_cutoff:
            raise WorkshopError('policy_blocked','Source business time changed. Capture a fresh proposal.')
        return policy

    @staticmethod
    def _receipt(conn,candidate):
        row=conn.execute('SELECT data FROM pricing.receipts WHERE candidate_id=%s',(candidate.candidate_id,)).fetchone()
        return Receipt.model_validate(row['data']) if row else None

    @traced('pricing.application')
    def approve_and_apply(self,scope,candidate_id,reviewer_id,idempotency_key):
        self._authorize(scope,reviewer_id)
        candidate=self.ledger.get_candidate(candidate_id,scope)
        with self.ledger.connect() as conn:
            scenario,state=self._lock(conn,scope,candidate,idempotency_key)
            receipt=self._receipt(conn,candidate)
            if receipt: return receipt
            decision=conn.execute('SELECT * FROM pricing.decisions WHERE candidate_id=%s',(candidate_id,)).fetchone()
            if decision and decision['status']=='rejected':
                raise WorkshopError('rejected_by_learner','Create a new proposal to review another price.')
            policy=self._policy(conn,scope,candidate,scenario,state)
            if policy.status not in ('eligible','requires_review'):
                raise WorkshopError('policy_blocked','The proposal no longer passes policy. Capture fresh inputs and review a new proposal.')
            now=self.clock.wall_now()
            if not decision:
                decision_id=uuid4()
                conn.execute("INSERT INTO pricing.decisions VALUES(%s,%s,%s,%s,%s,%s,%s,'approved',%s)",
                             (decision_id,candidate_id,*scope_key(scope),reviewer_id,now,Jsonb({'policy':policy.model_dump(),'workflow':'approved_mock'})))
                queue_event(conn,'policy',scope,now,{'candidate_id':str(candidate_id),**policy.model_dump()})
                queue_event(conn,'approval',scope,now,{'decision_id':str(decision_id),'candidate_id':str(candidate_id),'reviewer_id':reviewer_id})
            else: decision_id=decision['decision_id']
            attempt=conn.execute('SELECT * FROM pricing.application_attempts WHERE owner_id=%s AND idempotency_key=%s',(scope.owner_id,idempotency_key)).fetchone()
            if attempt: attempt_id=attempt['attempt_id']
            else:
                attempt_id=uuid4()
                conn.execute("INSERT INTO pricing.application_attempts VALUES(%s,%s,%s,%s,'applying',%s,%s)",
                             (attempt_id,candidate_id,scope.owner_id,idempotency_key,now,Jsonb({'decision_id':str(decision_id),'scope':scope.model_dump(mode='json')})))
                queue_event(conn,'application_attempt',scope,now,{'attempt_id':str(attempt_id),'candidate_id':str(candidate_id),'status':'applying'})
        try:
            return self._apply_transaction(scope,candidate,decision_id,attempt_id,idempotency_key)
        except Exception as error:
            with self.ledger.connect() as conn:
                # An ambiguous commit may already have succeeded: reconcile before failure.
                receipt=self._receipt(conn,candidate)
                if receipt: return receipt
                conn.execute("UPDATE pricing.application_attempts SET status='application_failed' WHERE attempt_id=%s",(attempt_id,))
                queue_event(conn,'application_attempt',scope,self.clock.wall_now(),{'attempt_id':str(attempt_id),'candidate_id':str(candidate_id),'status':'application_failed'})
            if isinstance(error,WorkshopError): raise
            raise WorkshopError('application_failed','The mock price was not acknowledged. Refresh the proposal and retry the application.') from None

    @traced('pricing.application.commit')
    def _apply_transaction(self,scope,candidate,decision_id,attempt_id,key):
        with self.ledger.connect() as conn:
            scenario,state=self._lock(conn,scope,candidate,key)
            receipt=self._receipt(conn,candidate)
            if receipt:
                conn.execute("UPDATE pricing.application_attempts SET status='applied' WHERE attempt_id=%s",(attempt_id,))
                return receipt
            decision=conn.execute('SELECT * FROM pricing.decisions WHERE decision_id=%s FOR UPDATE',(decision_id,)).fetchone()
            if not decision or decision['status']!='approved' or decision['reviewer_id']!=scope.owner_id:
                raise WorkshopError('approval_missing','Review this proposal before applying a mock price.')
            policy=self._policy(conn,scope,candidate,scenario,state)
            if policy.status not in ('eligible','requires_review'):
                raise WorkshopError('policy_blocked','Inputs changed after review. Capture a fresh proposal.')
            now=self.clock.wall_now();version=state['applied_price_version']+1
            receipt=Receipt(receipt_id=uuid4(),decision_id=decision_id,scope=scope,price_minor=candidate.price_minor,
                            applied_price_version=version,acknowledged_at=now)
            conn.execute(f'UPDATE pricing.subject_state SET current_price_minor=%s,applied_price_version=%s WHERE {SUBJECT_WHERE}',
                         (candidate.price_minor,version,*subject_key(scope,candidate.snapshot.subject)))
            conn.execute('INSERT INTO pricing.price_history(revision_id,owner_id,scenario_id,generation,sku,store_id,channel,source_version,price_minor,effective_at,acknowledged_at) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                         (uuid4(),*subject_key(scope,candidate.snapshot.subject),version,candidate.price_minor,scenario['scenario_cutoff'],now))
            conn.execute('INSERT INTO pricing.receipts VALUES(%s,%s,%s,%s,%s,%s,%s)',
                         (receipt.receipt_id,candidate.candidate_id,decision_id,*scope_key(scope),Jsonb(receipt.model_dump(mode='json'))))
            conn.execute("UPDATE pricing.application_attempts SET status='applied' WHERE attempt_id=%s",(attempt_id,))
            queue_event(conn,'receipt',scope,now,{'candidate_id':str(candidate.candidate_id),**receipt.model_dump(mode='json')},receipt.receipt_id)
            return receipt

    @traced('pricing.review.reject')
    def reject(self,scope,candidate_id,reviewer_id,note):
        self._authorize(scope,reviewer_id)
        if not isinstance(note,str) or len(note)>500: raise WorkshopError('invalid_note','Use a review note of at most 500 characters.')
        candidate=self.ledger.get_candidate(candidate_id,scope)
        with self.ledger.connect() as conn:
            self._lock(conn,scope,candidate)
            old=conn.execute('SELECT * FROM pricing.decisions WHERE candidate_id=%s',(candidate_id,)).fetchone()
            if old:
                if old['status']=='rejected': return old['data']
                raise WorkshopError('already_approved','This proposal has already been approved. Review a new proposal instead.')
            at=self.clock.wall_now();decision_id=uuid4();data={'decision_id':str(decision_id),'workflow':'rejected_by_learner','note':note}
            conn.execute("INSERT INTO pricing.decisions VALUES(%s,%s,%s,%s,%s,%s,%s,'rejected',%s)",
                         (decision_id,candidate_id,*scope_key(scope),reviewer_id,at,Jsonb(data)))
            queue_event(conn,'rejection',scope,at,{'decision_id':str(decision_id),'candidate_id':str(candidate_id),'reviewer_id':reviewer_id,**data})
            return data
