from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID
import json
import os
from urllib.parse import urlsplit
from uuid import uuid4

import psycopg
from psycopg import sql

import pytest

from pricing_lab.clock import Clock
from pricing_lab.contracts import Scope, Snapshot, Subject

LAB_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def private_test_environment(monkeypatch,tmp_path):
    from pricing_lab.settings import Settings
    for key in list(os.environ):
        if (key.startswith('PRICING_') and not key.startswith('PRICING_TEST_')) or key.startswith('AGENT_'):
            monkeypatch.delenv(key,raising=False)
    monkeypatch.setenv("PRICING_PRIVATE_ENV_PATH",str(tmp_path/"absent.env"))
    monkeypatch.setitem(Settings.model_config,"env_file",None)


@pytest.fixture
def clock():
    return Clock(datetime(2026, 1, 30, 12, tzinfo=UTC))


@pytest.fixture
def scope():
    return Scope(owner_id="learner-1", scenario_id="grocery", generation=1)


@pytest.fixture
def subject():
    return Subject(sku=101, store_id=1, channel="store")


@pytest.fixture
def golden_data():
    return json.loads((LAB_ROOT / "fixtures/golden/scenario.json").read_text())


@pytest.fixture
def golden_snapshot(scope, subject, clock, golden_data):
    return Snapshot(
        snapshot_id=UUID("00000000-0000-0000-0000-000000000001"),
        scope=scope, subject=subject,
        scenario_cutoff=clock.scenario_now(), captured_at=clock.wall_now(),
        expires_at=clock.wall_now() + timedelta(minutes=60),
        source_versions={"inventory": "1", "price": "1", "cost": "1"},
        source_ages_seconds={"inventory": 0, "price": 0, "cost": 0},
        **golden_data["state"],
    )


@pytest.fixture
def postgres_dsn():
    base = os.environ.get("PRICING_TEST_POSTGRES_DSN")
    if not base or urlsplit(base).hostname not in ("localhost", "127.0.0.1"):
        pytest.fail("Run scripts/test-integration.sh with its isolated local databases.")
    name = "pricing_test_" + uuid4().hex
    with psycopg.connect(base, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    dsn = base.rsplit("/", 1)[0] + "/" + name
    try:
        yield dsn
    finally:
        with psycopg.connect(base, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


@pytest.fixture
def ledger(postgres_dsn, scope, subject, golden_data, clock):
    from pricing_lab.ledger import Ledger
    ledger = Ledger(postgres_dsn)
    ledger.migrate()
    ledger.initialize(scope, subject, golden_data["state"], golden_data["rules"], clock.scenario_now())
    return ledger


@pytest.fixture
def source(postgres_dsn, ledger):
    from pricing_lab.source import SourceStore
    return SourceStore(postgres_dsn)


@pytest.fixture
def sale_request(subject, clock):
    from pricing_lab.contracts import SaleRequest
    return SaleRequest(event_id=uuid4(), subject=subject, quantity=2,
                       occurred_at=clock.scenario_now(), expected_price_version=1)


@pytest.fixture
def analytics():
    import clickhouse_connect
    from pricing_lab.analytics import AnalyticsClient
    from pricing_lab.settings import Settings
    port = os.environ.get("PRICING_TEST_CLICKHOUSE_PORT")
    if not port:
        pytest.fail("Run scripts/test-integration.sh for real ClickHouse tests.")
    name = "pricing_test_" + uuid4().hex
    admin = clickhouse_connect.get_client(host="127.0.0.1", port=int(port), secure=False,
                                         password="test-only-password", autogenerate_session_id=False)
    admin.command(f"CREATE DATABASE {name}")
    client = AnalyticsClient(Settings(clickhouse_host="127.0.0.1", clickhouse_port=int(port),
                                     clickhouse_secure=False, clickhouse_database=name,
                                     clickhouse_password="test-only-password"))
    try:
        yield client
    finally:
        client.close()
        admin.command(f"DROP DATABASE {name} SYNC")
        admin.close()


@pytest.fixture
def loader(analytics, ledger):
    from pricing_lab.history import HistoryLoader
    return HistoryLoader(analytics, ledger)


@pytest.fixture
def manifest_uri():
    return (LAB_ROOT / "fixtures/manifests/golden.json").as_uri()


@pytest.fixture
def features(analytics, ledger, clock, scope, manifest_uri):
    from pricing_lab.features import FeatureService
    from pricing_lab.history import HistoryLoader
    from cdc_utils import create_replay_tables, replay_all
    create_replay_tables(analytics, ledger)
    replay_all(analytics, ledger)
    HistoryLoader(analytics, ledger).load(manifest_uri, scope)
    features = FeatureService(analytics, ledger, clock)
    features.configure_views()
    return features


@pytest.fixture
def manual_artifact():
    from pricing_lab.model import FEATURE_SCHEMA_HASH
    import math
    coefficients=[math.log(4)]+[0.0]*13+[-1.8]
    return {"status":"ready","model_version":"hand-authored-test-only",
            "feature_schema_hash":FEATURE_SCHEMA_HASH,
            "encoding":{"skus":[101],"stores":[1],"hours":list(range(10,18)),"weekdays":list(range(7))},
            "references":{"101":800},"subjects":[[101,1]],"supported_price_ratios":{"101":[.65,1.]},
            "coefficients":coefficients,"ensemble":[coefficients],"synthetic":True}


@pytest.fixture
def model(manual_artifact,tmp_path):
    from pricing_lab.model import DemandModel
    path=tmp_path/"manual-model.json"
    path.write_text(json.dumps(manual_artifact))
    return DemandModel.from_artifact(path)


@pytest.fixture(scope="session")
def calibration_rows():
    from datetime import timedelta
    import numpy as np
    rng=np.random.default_rng(17)
    rows=[]
    for day in range(90):
        for sku,reference,rate in [(101,800,4),(102,1000,6)]:
            for store in [1,2]:
                for hour in range(10,18):
                    when=datetime(2025,11,1,hour,tzinfo=UTC)+timedelta(days=day)
                    lower=((int(reference*.65)+49)//50)*50
                    price=int(rng.choice(np.arange(lower,reference+1,50)))
                    mean=rate*(1+.1*(store-1))*(1+.025*(hour-13))*(1.15 if when.weekday()>=5 else 1)*(price/reference)**-1.8
                    rows.append({"sku":sku,"store_id":store,"hour":when,"offered_price_minor":price,
                                 "reference_price_minor":reference,"units":int(rng.poisson(mean)),
                                 "exposure_hours":1.,"available_units":10000,"open":True,"single_price":True})
    return rows
