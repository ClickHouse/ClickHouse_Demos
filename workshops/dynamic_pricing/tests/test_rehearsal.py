from datetime import UTC,datetime
from uuid import uuid4
import pytest
from pydantic import SecretStr
from infra.rehearsal import check_rehearsal_settings
from infra.provision import ResourceInventory
from pricing_lab.settings import Settings
from pricing_lab.contracts import WorkshopError

def owned():
    org=uuid4()
    settings=Settings(owner_id='rehearsal-release',cloud_org_id=org,region='ap-southeast-1',clickhouse_host='owned.example.test',clickhouse_secure=True,postgres_dsn='postgresql://app:fake@source.example.test/postgres?sslmode=verify-full',dataset_manifest='https://synthetic.example.test/manifest.json')
    inventory=ResourceInventory(owner_id=settings.owner_id,organization_id=org,region=settings.region,created_at=datetime.now(UTC),service_id=uuid4(),postgres_id=uuid4(),clickpipe_id=uuid4())
    return settings,inventory

def test_rehearsal_refuses_replay_and_non_dedicated_owner_before_writes():
    settings,inventory=owned()
    for changed in (settings.model_copy(update={'ingestion_mode':'local_replay'}),settings.model_copy(update={'owner_id':'learner-1'}),settings.model_copy(update={'clickhouse_host':'127.0.0.1'})):
        with pytest.raises(WorkshopError): check_rehearsal_settings(changed,inventory)

def test_rehearsal_requires_full_owned_inventory_and_remote_source():
    settings,inventory=owned()
    check_rehearsal_settings(settings,inventory)
    with pytest.raises(WorkshopError): check_rehearsal_settings(settings,inventory.model_copy(update={'clickpipe_id':None}))
    with pytest.raises(WorkshopError): check_rehearsal_settings(settings,inventory.model_copy(update={'pending_creation':'service'}))
    with pytest.raises(WorkshopError): check_rehearsal_settings(settings.model_copy(update={'postgres_dsn':SecretStr('postgresql://app:fake@localhost/postgres')}),inventory)


def test_rehearsal_admin_connection_must_match_source_and_use_separate_role():
    from infra.rehearsal import check_admin_dsn
    settings,_=owned()
    for dsn in ('postgresql://app:fake@source.example.test/postgres?sslmode=verify-full','postgresql://admin:fake@other.example.test/postgres?sslmode=verify-full','postgresql://admin:fake@source.example.test/postgres'):
        with pytest.raises(WorkshopError): check_admin_dsn(settings,dsn)
    check_admin_dsn(settings,'postgresql://admin:fake@source.example.test/postgres?sslmode=verify-full')


def test_rehearsal_wrong_pipe_name_identifies_expected_owned_pipe(monkeypatch):
    from infra.rehearsal import rehearse_cloud
    settings,inventory=owned()
    monkeypatch.setattr('infra.rehearsal.runtime_platform',lambda:'macos')
    class Runner:
        def run(self,argv):
            if argv[2]=='clickpipe':
                return {'id':str(inventory.clickpipe_id),'name':'wrong-pipe','state':'Running'}
            return {'tags':[{'key':'workshop','value':'dynamic-pricing'},{'key':'owner','value':'rehearsal-release'}],
                    'endpoints':[{'host':'owned.example.test'}],'hostname':'source.example.test'}
    with pytest.raises(WorkshopError,match='pricing-rehearsal-release-cdc'):
        rehearse_cloud(settings,inventory,admin_dsn='postgresql://admin:fake@source.example.test/postgres?sslmode=verify-full',runner=Runner())
