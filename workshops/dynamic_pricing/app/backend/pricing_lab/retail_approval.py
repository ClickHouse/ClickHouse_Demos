"""A learner-approved price is committed with the same locks used by live sales."""
from datetime import datetime,timedelta
from uuid import uuid4
from psycopg.types.json import Jsonb
from .contracts import WorkshopError
from .retail_contracts import PriceInput,RetailReceipt
from .observability import traced


class RetailApprovalService:
    def __init__(self,ledger,analysis,clock):
        self.ledger,self.analysis,self.clock=ledger,analysis,clock

    def propose(self,inputs,price_minor):
        with self.ledger.connect() as conn:
            run=conn.execute('SELECT * FROM pricing.retail_runs ORDER BY created_at DESC LIMIT 1').fetchone()
            owner=conn.execute('SELECT owner_id FROM pricing.scenarios ORDER BY owner_id LIMIT 1').fetchone()
        if not run or not owner:
            raise WorkshopError('scenario_missing','Initialize the learner instance first.')
        authoritative=self.analysis.price_inputs(run['run_id'],inputs.subject,inputs.cutoff)
        rows=self.analysis.compare(authoritative)
        selected=next((r for r in rows if r.price_minor==price_minor),None)
        if selected is None:
            raise WorkshopError('price_not_permitted','Choose one of the permitted prices in this comparison.')
        proposal=uuid4()
        data=dict(inputs=authoritative.model_dump(mode='json'),selected=selected.model_dump(mode='json'),
                  owner=owner['owner_id'],valid_until=(self.clock.wall_now()+timedelta(minutes=5)).isoformat())
        with self.ledger.connect() as conn:
            conn.execute('INSERT INTO pricing.retail_proposals(proposal_id,run_id,data) VALUES (%s,%s,%s)',(proposal,run['run_id'],Jsonb(data)))
        return proposal

    @traced('pricing.retail.apply')
    def apply(self,request,reviewer_id):
        with self.ledger.connect() as conn:
            proposal=conn.execute('SELECT * FROM pricing.retail_proposals WHERE proposal_id=%s',(request.proposal_id,)).fetchone()
            if not proposal:
                raise WorkshopError('proposal_missing','Refresh the price comparison.')
            data=proposal['data'];inputs=PriceInput.model_validate(data['inputs'])
            if reviewer_id!=data['owner']:
                raise WorkshopError('forbidden','Use your learner session to approve this price.')
            run=conn.execute('SELECT * FROM pricing.retail_runs WHERE run_id=%s FOR UPDATE',(proposal['run_id'],)).fetchone()
            existing=conn.execute('SELECT * FROM pricing.retail_receipts WHERE idempotency_key=%s',(request.idempotency_key,)).fetchone()
            if existing:
                if existing['proposal_id']!=request.proposal_id:
                    raise WorkshopError('idempotency_conflict','Use a new approval request for this proposal.')
                return RetailReceipt.model_validate(existing['data']['receipt'])
            current=conn.execute('SELECT * FROM pricing.retail_state WHERE run_id=%s AND store_id=%s AND sku=%s FOR UPDATE',(proposal['run_id'],inputs.subject.store_id,inputs.subject.sku)).fetchone()
            pending=conn.execute("SELECT 1 FROM pricing.retail_batches WHERE run_id=%s AND status!='acknowledged' LIMIT 1",(proposal['run_id'],)).fetchone()
            boundary_ok=inputs.sales_sequence==0 or conn.execute("SELECT acknowledged_at FROM pricing.retail_batches WHERE run_id=%s AND sequence=%s AND status='acknowledged'",(proposal['run_id'],inputs.sales_sequence)).fetchone()
            price=data['selected']['price_minor']
            keys=('stock_units','current_price_minor','inventory_version','price_version','rule_version',
                  'unit_cost_minor','selling_cost_minor','disposal_cost_minor','expires_at')
            if (not run or run['status']=='held' or not current or pending or not boundary_ok
                or (inputs.sales_sequence>0 and (not boundary_ok['acknowledged_at'] or (self.clock.wall_now()-boundary_ok['acknowledged_at']).total_seconds()>120))
                or self.clock.wall_now()>datetime.fromisoformat(data['valid_until'])
                or run['logical_end']>=inputs.expires_at or self.analysis.model_version()!=inputs.model_version
                or any(current[k]!=getattr(inputs,k) for k in keys)
                or price not in current['permitted_prices_minor']):
                raise WorkshopError('proposal_changed','The comparison changed. Refresh it, review the new values, and approve again.')
            receipt=RetailReceipt(receipt_id=uuid4(),proposal_id=request.proposal_id,run_id=proposal['run_id'],
                 subject=inputs.subject,price_minor=price,price_version=current['price_version']+1,
                 acknowledged_at=self.clock.wall_now(),effective_sequence=run['sequence']+1)
            key=(proposal['run_id'],inputs.subject.store_id,inputs.subject.sku)
            conn.execute('UPDATE pricing.retail_state SET current_price_minor=%s,price_version=%s WHERE run_id=%s AND store_id=%s AND sku=%s',(price,receipt.price_version,*key))
            conn.execute('INSERT INTO pricing.retail_price_history VALUES (%s,%s,%s,%s,%s,%s)',(*key,receipt.price_version,price,receipt.effective_sequence))
            stored=dict(started_at=run['logical_end'].isoformat(),receipt=receipt.model_dump(mode='json'),reviewer_id=reviewer_id,inputs=inputs.model_dump(mode='json'))
            conn.execute('INSERT INTO pricing.retail_receipts(receipt_id,proposal_id,run_id,idempotency_key,data,acknowledged_at) VALUES (%s,%s,%s,%s,%s,%s)',(receipt.receipt_id,request.proposal_id,proposal['run_id'],request.idempotency_key,Jsonb(stored),receipt.acknowledged_at))
            conn.execute('UPDATE pricing.retail_comparisons SET active=false WHERE run_id=%s AND store_id=%s AND sku=%s',key)
            conn.execute('INSERT INTO pricing.retail_comparisons(receipt_id,run_id,store_id,sku,inputs,price_minor,applied_stock,comparison_stock,seed,logical_end) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(receipt.receipt_id,*key,Jsonb(inputs.model_dump(mode='json')),price,inputs.stock_units,inputs.stock_units,run['seed'],run['logical_end']))
            return receipt
