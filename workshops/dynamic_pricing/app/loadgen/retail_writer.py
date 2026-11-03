"""Paced retail batches; operational state and cursor survive process restarts."""
import argparse
from datetime import UTC,datetime,timedelta
import json
import time

from pricing_lab.analytics import AnalyticsClient
from pricing_lab.ledger import Ledger
from pricing_lab.retail_source import RetailSource, make_batch, UncommittedBatchConflict
from pricing_lab.retail_delivery import RetailDelivery
from pricing_lab.retail_stream import generate_batch
from pricing_lab.retail_outcomes import pair_rows
from pricing_lab.settings import Settings
from generator.retail_catalog import OPEN_UTC_HOUR


def sales_rate(logical_time, steady_rate, burst_rate, burst_minutes):
    boundary=logical_time.replace(hour=OPEN_UTC_HOUR,minute=0,second=0,microsecond=0)+timedelta(minutes=burst_minutes)
    if burst_rate is not None and logical_time < boundary:
        return burst_rate,boundary
    return steady_rate,None


def pacing(control,logical_time,default_speed):
    until=control.get('fast_forward_until')
    boundary=datetime.fromisoformat(until) if until else None
    return (20,boundary) if boundary and logical_time<boundary else (default_speed,None)


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--transactions-per-minute',type=int,default=10000)
    parser.add_argument('--minutes',type=float,default=10,help='Minutes of synthetic business time to complete (up to 180).')
    parser.add_argument('--burst-transactions-per-minute',type=int)
    parser.add_argument('--burst-minutes',type=float,default=0,help='Initial burst measured from the persisted trading-day start; restarts do not repeat it.')
    parser.add_argument('--seed',type=int,default=200017)
    parser.add_argument('--speed',type=float,default=1,help='Requested business-clock speed; actual progress depends on delivery throughput.')
    args=parser.parse_args(argv)
    if not 1<=args.transactions_per_minute<=1000000 or not 0<args.minutes<=180 or not .1<=args.speed<=20:
        parser.error('Use 1–1,000,000 transactions/minute, up to 180 minutes, and speed 0.1–20.')
    if not ((args.burst_transactions_per_minute is None and args.burst_minutes==0) or
            (args.burst_transactions_per_minute is not None and 1<=args.burst_transactions_per_minute<=1000000 and 0<args.burst_minutes<=180)):
        parser.error('Specify a burst rate of 1–1,000,000 and duration greater than zero and up to 180 minutes together.')
    settings=Settings.from_env(include_inventory=False)
    ledger=Ledger(settings.postgres_dsn.get_secret_value())
    source=RetailSource(ledger,settings.retail_spool)
    analytics=AnalyticsClient(settings)
    delivery=RetailDelivery(analytics,source)
    required={'retail_inventory_current','retail_price_current','retail_policy_current','retail_opportunities'}
    available={r['name'] for r in analytics.query('SELECT name FROM system.tables WHERE database={db:String}',{'db':analytics.database})}
    if not required <= available:
        raise ValueError('Install and verify the retail analytical views before starting live sales.')
    # Lock before selecting the latest run, so fresh-run creation cannot race startup.
    with ledger.connect() as lock:
        lock.autocommit=True
        if not lock.execute('SELECT pg_try_advisory_lock(84927642) locked').fetchone()['locked']:
            raise ValueError('A retail writer is already running.')
        with ledger.connect() as conn:
            history=conn.execute('SELECT data FROM pricing.retail_history_state WHERE id').fetchone()
            if not history or not history['data'].get('rollup'):
                raise ValueError('Create and verify the minute aggregates before starting live sales.')
            run=conn.execute('SELECT * FROM pricing.retail_runs ORDER BY created_at DESC LIMIT 1').fetchone()
            if not run or run['status']=='held':
                raise ValueError('Initialize a retail run and resolve any held batch before starting.')
            if args.seed != run['seed']:
                raise ValueError('Resume with the persisted retail run seed.')
            conn.execute("UPDATE pricing.retail_runs SET status='running' WHERE run_id=%s",(run['run_id'],))
        started=time.monotonic(); transactions=events=0
        try:
            for pending in source.pending(run['run_id']):
                delivery.deliver(pending)
            last_rate=last_speed=None
            finish_at=run['logical_end']+timedelta(minutes=args.minutes)
            deadline=started
            while True:
                current=source.run(run['run_id'])
                if current['logical_end']>=finish_at:
                    break
                with ledger.connect() as conn:
                    heartbeat=datetime.now(UTC).isoformat()
                    conn.execute("UPDATE pricing.retail_runs SET control=jsonb_set(jsonb_set(control,'{writer_heartbeat}',to_jsonb(%s::text)),'{writer_pacing_heartbeat}',to_jsonb(%s::text)) WHERE run_id=%s",(heartbeat,heartbeat,run['run_id']))
                if current['status']=='held':
                    break
                if current['status']!='running':
                    time.sleep(.5)
                    deadline=time.monotonic()
                    continue
                terminal=current['logical_end'].replace(hour=10,minute=0,second=0,microsecond=0)
                if current['logical_end']>=terminal or current['logical_end'].hour<2:
                    print(json.dumps({'status':'trading_window_complete'}),flush=True)
                    break
                rate,burst_end=sales_rate(current['logical_end'],args.transactions_per_minute,args.burst_transactions_per_minute,args.burst_minutes)
                if rate!=last_rate:
                    print(json.dumps({'status':'sales_rate_changed','target_transactions_per_minute':rate,'logical_time':str(current['logical_end'])}),flush=True)
                    last_rate=rate
                speed,acceleration_end=pacing(current['control'],current['logical_end'],args.speed)
                if speed!=last_speed:
                    print(json.dumps({'status':'clock_speed_changed','speed':speed,'until':str(acceleration_end) if acceleration_end else None}),flush=True)
                    last_speed=speed
                interval=min(60.,10.*speed,10000*60/rate)
                next_minute=current['logical_end'].replace(second=0,microsecond=0)+timedelta(minutes=1)
                end=min(current['logical_end']+timedelta(seconds=interval),next_minute,terminal,finish_at,burst_end or finish_at,acceleration_end or finish_at)
                count=max(1,int(rate*(end-current['logical_end']).total_seconds()/60))
                state=source.state(run['run_id'])
                changes=current['control'].get('district_demand',{})
                for item in state:
                    item['demand_multiplier']=changes.get(str(item['district_id']),1.)
                paired=pair_rows(source,run['run_id'],current['sequence']+1,end)
                subjects={(c['store_id'],c['sku']) for c in source.comparisons(run['run_id'])}
                state=[s for s in state if (s['store_id'],s['sku']) not in subjects]
                rows=generate_batch(run['run_id'],current['sequence']+1,state,min(count,10000-len(paired)),end,args.seed,window_seconds=(end-current['logical_end']).total_seconds())+paired
                batch=make_batch(run['run_id'],current['sequence']+1,rows,end)
                try:
                    source.stage(batch,rows,expected_demand_revision=current['control'].get('demand_revision',0))
                except UncommittedBatchConflict:
                    print(json.dumps({'status':'regenerating_after_source_change','sequence':batch.sequence}),flush=True)
                    continue
                delivery.deliver(batch)
                transactions+=len({r['transaction_id'] for r in rows if r['branch']=='applied'}); events+=sum(r['branch']=='applied' for r in rows)
                elapsed=max(time.monotonic()-started,.001)
                print(json.dumps({'sequence':batch.sequence,'batch_id':str(batch.batch_id),'rows':events,
                                  'transactions':transactions,'actual_transactions_per_second':transactions/elapsed,
                                  'actual_rows_per_second':events/elapsed,'logical_end':str(end)}),flush=True)
                if end==terminal:
                    print(json.dumps({'status':'trading_window_complete'}),flush=True)
                    break
                deadline+=(end-current['logical_end']).total_seconds()/speed
                time.sleep(max(0,deadline-time.monotonic()))
        finally:
            with ledger.connect() as conn:
                conn.execute("UPDATE pricing.retail_runs SET status='stopped' WHERE run_id=%s AND status='running'",(run['run_id'],))
            lock.execute('SELECT pg_advisory_unlock(84927642)')
            analytics.close()
    elapsed=max(time.monotonic()-started,.001)
    print(json.dumps({'status':'finished','elapsed_seconds':elapsed,'transactions':transactions,'rows':events,
                      'actual_transactions_per_second':transactions/elapsed}),flush=True)
    return 0


if __name__=='__main__':
    raise SystemExit(main())
