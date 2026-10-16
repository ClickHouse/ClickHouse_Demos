"""Authenticated retail workflow with instance-owned run control."""
from datetime import UTC,datetime
from typing import Literal
from uuid import UUID
import time
from fastapi import APIRouter,Depends,Query
from pydantic import AwareDatetime,Field
from psycopg.types.json import Jsonb
from .contracts import Contract,WorkshopError
from .retail_contracts import RetailSubject,RetailApproval


class ComparisonRequest(Contract):
    subject: RetailSubject
    cutoff: AwareDatetime

class ProposalRequest(ComparisonRequest):
    price_minor: int=Field(gt=0,strict=True)

class StreamRequest(Contract):
    action: Literal['start','stop','slow_district']
    district: int=Field(default=1,ge=1,le=20)
    rate: float=Field(default=.5,ge=0,le=2,allow_inf_nan=False)


def retail_router(service,protected,owner):
    router=APIRouter(prefix='/api/retail',dependencies=[Depends(protected)])
    def envelope(data):
        return dict(run_id=service.run_id,mode='synthetic',data=data,generated_at=datetime.now(UTC))
    @router.get('/context')
    def context():return envelope(service.context())
    @router.get('/overview')
    def overview(district:int=Query(0,ge=0,le=20)):return envelope(service.overview(district))
    @router.get('/opportunities')
    def opportunities(cutoff:AwareDatetime,district:int=Query(1,ge=1,le=20)):
        return envelope(service.opportunities(district,cutoff))
    @router.post('/compare')
    def compare(body:ComparisonRequest):return envelope(service.compare(body.subject,body.cutoff))
    @router.post('/proposals')
    def propose(body:ProposalRequest):return envelope(service.propose(body))
    @router.post('/apply')
    def apply(body:RetailApproval):return envelope(service.apply(body,owner))
    @router.get('/outcomes')
    def outcomes(receipt:UUID):return envelope(service.outcomes(receipt))
    @router.post('/stream')
    def stream(body:StreamRequest):return envelope(service.control(body))
    return router


class RetailWorkbench:
    def __init__(self,ledger,analytics,source,analysis,approval,outcome):
        self.ledger,self.analytics,self.source=ledger,analytics,source
        self.analysis,self.approval,self.outcome=analysis,approval,outcome

    def _run(self):
        with self.ledger.connect() as conn:
            row=conn.execute('SELECT * FROM pricing.retail_runs ORDER BY created_at DESC LIMIT 1').fetchone()
        if not row:
            raise WorkshopError('retail_setup_missing','Load the retail history and initialize the run in Module 1.')
        return row

    @property
    def run_id(self):return self._run()['run_id']

    def _tables(self):
        return {r['name'] for r in self.analytics.query('SELECT name FROM system.tables WHERE database={db:String}',{'db':self.analytics.database})}

    def context(self):
        run=self._run();tables=self._tables();context=[]
        if 'retail_analysis_context' in tables:
            context=self.analytics.query(f'SELECT * FROM {self.analytics.database}.retail_analysis_context WHERE run_id={{run:UUID}} LIMIT 1',{'run':run['run_id']})
        data=context[0] if context else dict(cutoff=run['logical_end'].replace(second=0,microsecond=0),model_version='',delivery_age_seconds=None)
        heartbeat=run['control'].get('writer_heartbeat')
        connected=bool(heartbeat and (datetime.now(UTC)-datetime.fromisoformat(heartbeat)).total_seconds()<120)
        return dict(data,writer_connected=connected,stream_state=run['status'],logical_time=run['logical_end'],
                    aggregation_ready='retail_district_minute' in tables,analysis_ready='retail_opportunities' in tables)

    def overview(self,district=0):
        context=self.context();db=self.analytics.database
        params={'cutoff':context['cutoff'],'district':district}
        aggregate=context['aggregation_ready']
        columns=('sumMerge(units_state) units,sumMerge(receipts_state) receipts_minor,uniqExactMerge(transactions_state) transactions'
                 if aggregate else 'sum(quantity) units,sum(receipts_minor) receipts_minor,uniqExact(transaction_id) transactions')
        table='retail_district_minute' if aggregate else 'retail_sales'
        clock='minute' if aggregate else 'event_time'
        sql=f'''SELECT district_id,toStartOfInterval({clock},INTERVAL 15 MINUTE) minute,{columns}
          FROM {db}.{table} WHERE ({'{district:UInt16}'}=0 OR district_id={{district:UInt16}})
          AND {clock}>={{cutoff:DateTime64(3,'UTC')}}-INTERVAL 24 HOUR AND {clock}<{{cutoff:DateTime64(3,'UTC')}}
          AND branch='applied' GROUP BY district_id,minute ORDER BY minute,district_id LIMIT 2000'''
        measured=self.analytics.measured_query(sql,params);rows=measured['rows']
        series={};districts={}
        for r in rows:
            for key,group in [(r['minute'],series),(r['district_id'],districts)]:
                item=group.setdefault(key,dict(units=0,receipts_minor=0,transactions=0))
                for metric in item:item[metric]+=r[metric]
        rate=self.analytics.query(f'''SELECT uniqExact(transaction_id)/60. AS rate FROM {db}.retail_sales
          WHERE run_id={{run:UUID}} AND branch='applied' AND source='live' AND emitted_at>=now()-INTERVAL 60 SECOND''',{'run':self.run_id})[0]['rate']
        return dict(totals={k:sum(r[k] for r in rows) for k in ('units','receipts_minor','transactions')},
            series=[dict(minute=str(k),**v) for k,v in series.items()],districts=[dict(district_id=k,**v) for k,v in districts.items()],
            transactions_per_second=rate,engine='aggregate' if aggregate else 'raw',query_id=measured['query_id'],
            query_seconds=measured['client_seconds'],read_rows=measured['read_rows'],read_bytes=measured['read_bytes'])

    def opportunities(self,district,cutoff):
        return self.analysis.opportunities(self.run_id,district,cutoff)

    def compare(self,subject,cutoff):
        started=time.monotonic()
        inputs=self.analysis.price_inputs(self.run_id,subject,cutoff)
        result=self.analysis.compare(inputs,measured=True)
        return dict(inputs=inputs.model_dump(mode='json'),rows=[r.model_dump(mode='json') for r in result['rows']],
                    query_id=result['query_id'],query_seconds=time.monotonic()-started)

    def propose(self,body):
        inputs=self.analysis.price_inputs(self.run_id,body.subject,body.cutoff)
        return dict(proposal_id=str(self.approval.propose(inputs,body.price_minor)))

    def apply(self,request,reviewer):
        return self.approval.apply(request,reviewer).model_dump(mode='json')

    def outcomes(self,receipt):return self.outcome.series(self.run_id,receipt)

    def control(self,body):
        with self.ledger.connect() as conn:
            run=conn.execute('SELECT * FROM pricing.retail_runs WHERE run_id=%s FOR UPDATE',(self.run_id,)).fetchone()
            if run['status']=='held':
                raise WorkshopError('stream_held','Reconcile the held batch before resuming the sales stream.')
            if body.action=='slow_district':
                control=run['control'];control.setdefault('district_demand',{})[str(body.district)]=body.rate
                conn.execute('UPDATE pricing.retail_runs SET control=%s WHERE run_id=%s',(Jsonb(control),run['run_id']))
                return dict(status=run['status'],district=body.district,demand_multiplier=body.rate)
            status='running' if body.action=='start' else 'stopped'
            conn.execute('UPDATE pricing.retail_runs SET status=%s WHERE run_id=%s',(status,run['run_id']))
            return dict(status=status,action='The separate writer process follows this requested state.')
