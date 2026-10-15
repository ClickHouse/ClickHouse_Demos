from datetime import UTC,datetime
from pathlib import Path
from uuid import UUID

import pytest
from dotenv import dotenv_values

from infra.provision import ResourceInventory
from infra.setup_cloud import destroy_cloud
from pricing_lab.contracts import WorkshopError
from pricing_lab.settings import Settings

ORG='44444444-4444-4444-8444-444444444444'
CH='11111111-1111-4111-8111-111111111111'
PG='22222222-2222-4222-8222-222222222222'
PIPE='33333333-3333-4333-8333-333333333333'


@pytest.fixture
def installation(tmp_path):
    settings=Settings(owner_id='saved-owner',cloud_org_id=ORG,resource_inventory_path=tmp_path/'resources.json',private_env_path=tmp_path/'env')
    inventory=ResourceInventory(owner_id=settings.owner_id,organization_id=ORG,region=settings.region,created_at=datetime.now(UTC),service_id=CH,postgres_id=PG,clickpipe_id=PIPE)
    settings.resource_inventory_path.write_text(inventory.model_dump_json())
    settings.private_env_path.write_text('PRICING_OWNER_ID=saved-owner\nPRICING_CLICKHOUSE_HOST=old.example.test\nPRICING_POSTGRES_DSN=old-private-dsn\nPRICING_REGION=ap-southeast-1\n')
    return settings


class Runner:
    def __init__(self,fail=None): self.calls=[];self.fail=fail
    def run(self,argv):
        self.calls.append(argv)
        kind,action=argv[2:4]
        if action=='get':
            return {'id':{'service':CH,'postgres':PG,'clickpipe':PIPE}[kind],
                    'name':'pricing-saved-owner-cdc' if kind=='clickpipe' else 'resource',
                    'tags':[{'key':'workshop','value':'dynamic-pricing'},{'key':'owner','value':'saved-owner'}]}
        if action=='delete' and kind==self.fail: raise WorkshopError('delete_failed','Retry deletion.')
        return {}


def test_destroy_preview_keeps_resources_and_private_settings(installation):
    runner=Runner();before=installation.private_env_path.read_bytes()
    plan=destroy_cloud(installation,runner,progress=lambda _:None)
    assert [item['kind'] for item in plan]==['clickpipe','postgres','service']
    assert runner.calls==[]
    assert installation.resource_inventory_path.exists()
    assert installation.private_env_path.read_bytes()==before


def test_destroy_deletes_owned_stack_and_allows_fresh_setup(installation):
    runner=Runner()
    destroy_cloud(installation,runner,execute=True,progress=lambda _:None)
    assert [call[2] for call in runner.calls if call[3]=='delete']==['clickpipe','postgres','service']
    assert not installation.resource_inventory_path.exists()
    archives=list(installation.resource_inventory_path.parent.glob('resources.deleted-*.json'))
    assert len(archives)==1
    saved=ResourceInventory.model_validate_json(archives[0].read_text())
    assert set(saved.deleted_resources)=={'clickpipe','postgres','service'}
    values=dotenv_values(installation.private_env_path)
    assert values['PRICING_OWNER_ID']=='saved-owner'
    assert values['PRICING_REGION']=='ap-southeast-1'
    assert values['PRICING_CLICKHOUSE_HOST']==values['PRICING_POSTGRES_DSN']==''
    assert values['PRICING_SESSION_SECRET']==''


def test_partial_destroy_preserves_credentials_and_resumes(installation):
    before=installation.private_env_path.read_bytes()
    with pytest.raises(WorkshopError,match='delete_failed'):
        destroy_cloud(installation,Runner(fail='service'),execute=True,progress=lambda _:None)
    assert installation.private_env_path.read_bytes()==before
    assert installation.resource_inventory_path.exists()
    runner=Runner()
    destroy_cloud(installation,runner,execute=True,progress=lambda _:None)
    assert [call[2] for call in runner.calls if call[3]=='delete']==['service']


def test_destroy_rejects_ambiguous_creation(installation):
    saved=ResourceInventory.model_validate_json(installation.resource_inventory_path.read_text())
    installation.resource_inventory_path.write_text(saved.model_copy(update={'pending_creation':'clickpipe'}).model_dump_json())
    runner=Runner()
    with pytest.raises(WorkshopError,match='creation_ack_unknown'):
        destroy_cloud(installation,runner,execute=True,progress=lambda _:None)
    assert runner.calls==[]


def test_destroy_rejects_different_owner_without_deletion(installation):
    runner=Runner()
    with pytest.raises(WorkshopError,match='owner_mismatch'):
        destroy_cloud(installation.model_copy(update={'owner_id':'another-owner'}),runner,execute=True,progress=lambda _:None)
    assert runner.calls==[]
    assert installation.resource_inventory_path.exists()


def test_destroy_cli_does_not_require_ip_detection_or_create_api(installation,monkeypatch):
    from infra import setup_cloud
    monkeypatch.setattr(setup_cloud.Settings,'from_env',classmethod(lambda cls: installation))
    runner=Runner()
    monkeypatch.setattr(setup_cloud,'CliRunner',lambda:runner)
    monkeypatch.setattr(setup_cloud,'cloud_settings',lambda *a,**kw:pytest.fail('Destroy must not detect public IP.'))
    monkeypatch.setattr(setup_cloud,'CloudApi',lambda *a,**kw:pytest.fail('Destroy must not use the create API.'))
    assert setup_cloud.main(['--destroy'])==0
    assert runner.calls==[]
