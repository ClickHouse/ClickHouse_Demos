"""Initialize an explicitly local test stack and replay its connector-shaped rows."""
from datetime import UTC,datetime
from pathlib import Path
import json
import os
import clickhouse_connect
from pricing_lab.analytics import AnalyticsClient
from pricing_lab.contracts import Scope,Subject
from pricing_lab.features import FeatureService
from pricing_lab.clock import Clock
from pricing_lab.history import HistoryLoader
from pricing_lab.ledger import Ledger
from pricing_lab.settings import Settings,LAB_ROOT
from cdc_utils import create_replay_tables,replay_all


def main():
    settings=Settings(postgres_dsn=os.environ['PRICING_TEST_POSTGRES_DSN'],clickhouse_host='127.0.0.1',clickhouse_port=int(os.environ['PRICING_TEST_CLICKHOUSE_PORT']),clickhouse_secure=False,clickhouse_password='test-only-password')
    admin=clickhouse_connect.get_client(host=settings.clickhouse_host,port=settings.clickhouse_port,password='test-only-password')
    admin.command('CREATE DATABASE IF NOT EXISTS pricing');admin.close()
    ledger=Ledger(settings.postgres_dsn.get_secret_value());ledger.migrate()
    scope=Scope(owner_id='learner-1',scenario_id='grocery',generation=1);subject=Subject(sku=101,store_id=1)
    golden=json.loads((LAB_ROOT/'fixtures/golden/scenario.json').read_text());cutoff=datetime(2026,1,30,12,tzinfo=UTC)
    ledger.initialize(scope,subject,golden['state'],golden['rules'],cutoff)
    analytics=AnalyticsClient(settings)
    create_replay_tables(analytics,ledger);replay_all(analytics,ledger)
    HistoryLoader(analytics,ledger).load((LAB_ROOT/'fixtures/manifests/golden.json').as_uri(),scope)
    FeatureService(analytics,ledger,Clock(cutoff)).configure_views();analytics.close()
    print('Developer fixtures prepared. CDC is local test replay, not ClickPipes replication.')

if __name__=='__main__':main()
