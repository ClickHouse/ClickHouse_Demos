from datetime import UTC,datetime,timedelta
from uuid import UUID
from pathlib import Path
import json,time
from generator.retail_catalog import retail_catalog,reference_rate,calendar_factor
from pricing_lab.retail_stream import generate_batch
from app.loadgen.retail_writer import sales_rate
stores,products=retail_catalog();start=datetime(2026,1,30,2,tzinfo=UTC);now=start;finish=start+timedelta(minutes=180);run=UUID(int=20261102)
state={(s['store_id'],p['sku']):dict(store_id=s['store_id'],sku=p['sku'],district_id=s['district_id'],stock_units=20+(s['store_id']+p['sku'])%41,current_price_minor=p['reference_price_minor'],unit_cost_minor=p['unit_cost_minor'],price_version=1,inventory_version=1,demand_multiplier=1.) for s in stores for p in products}
initial=sum(s['stock_units'] for s in state.values());sequence=events=0;started=time.monotonic();recent=[]
while now<finish:
 rate,boundary=sales_rate(now,10000,100000,2)
 end=min(now+timedelta(seconds=min(60,600000/rate)),finish,boundary or finish)
 if now>=start+timedelta(minutes=20):
  for s in state.values():
   if s['district_id']==1:s['demand_multiplier']=.5
 sequence+=1;rows=generate_batch(run,sequence,list(state.values()),int(rate*(end-now).total_seconds()/60),end,200017,window_seconds=(end-now).total_seconds())
 for r in rows:state[(r['store_id'],r['sku'])]['stock_units']-=r['quantity']
 events+=len(rows);now=end
 if sequence%30==0:print(json.dumps({'business_minutes':(now-start).total_seconds()/60,'remaining_stock':sum(s['stock_units'] for s in state.values())}),flush=True)
remaining=sum(s['stock_units'] for s in state.values());district=[s for s in state.values() if s['district_id']==1];stocked=sum(s['stock_units']>0 for s in district)
result=dict(status='passed',business_minutes=180,initial_stock=initial,delivered_transactions=events,remaining_stock=remaining,district_1_stocked_products=stocked,district_1_products=len(district),batches=sequence,elapsed_seconds=time.monotonic()-started,notes='Real deterministic generator; demand multiplier 0.5 in district 1 after minute 20. Inventory capacity evidence; full SQL eligibility verified separately.')
assert remaining==initial-events and stocked>100 and min(s['stock_units'] for s in state.values())>=0
Path('reports').mkdir(parents=True,exist_ok=True)
Path('reports/retail-three-hour-capacity.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result),flush=True)
