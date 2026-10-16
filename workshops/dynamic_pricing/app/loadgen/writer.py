"""Emit bounded transactional sales using the same atomic SourceStore protocol."""
import argparse
import json
import time
from uuid import uuid4
from pricing_lab.contracts import SaleRequest,Subject,WorkshopError
from pricing_lab.ledger import Ledger
from pricing_lab.settings import Settings
from pricing_lab.source import SourceStore
from pricing_lab.observability import configure


def main():
    if Settings.from_env(include_inventory=False).retail_mode:
        from retail_writer import main as retail_main
        return retail_main()
    parser=argparse.ArgumentParser();parser.add_argument('--count',type=int,default=20);parser.add_argument('--interval',type=float,default=2.)
    args=parser.parse_args()
    if not 1<=args.count<=1000 or not .1<=args.interval<=60: parser.error('Use 1–1,000 sales and an interval of 0.1–60 seconds.')
    settings=Settings.from_env(include_inventory=False);configure('pricing-writer',settings.otel_endpoint);ledger=Ledger(settings.postgres_dsn.get_secret_value());source=SourceStore(settings.postgres_dsn.get_secret_value());subject=Subject(sku=101,store_id=1)
    for _ in range(args.count):
        try:
            scope,cutoff=ledger.current_scope(settings.owner_id,settings.scenario_id);state=ledger.current_state(scope,subject)
            request=SaleRequest(event_id=uuid4(),subject=subject,quantity=1,occurred_at=cutoff,expected_price_version=state['applied_price_version'])
            result=source.create_sale(scope,request)
            print(json.dumps({'event_id':str(result['event_id']),'price_minor':result['unit_price_minor'],'applied_price_version':result['applied_price_version'],'mode':'synthetic'}),flush=True)
        except WorkshopError as error: print(str(error),flush=True);return 1
        time.sleep(args.interval)
    return 0

if __name__=='__main__': raise SystemExit(main())
