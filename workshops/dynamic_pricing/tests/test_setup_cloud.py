import json
from datetime import UTC,datetime
from pathlib import Path
from uuid import UUID
import httpx
import pytest
from pricing_lab.settings import Settings
from pricing_lab.contracts import WorkshopError

CH='11111111-1111-4111-8111-111111111111'
PIPE='33333333-3333-4333-8333-333333333333'
ORG='44444444-4444-4444-8444-444444444444'


def test_pipe_request_targets_pricing_with_replacement_and_verified_tls(tmp_path):
    from infra.setup_cloud import pipe_request
    ca=tmp_path/'ca.pem';ca.write_text('test-public-ca')
    settings=Settings(owner_id='learner-1',postgres_admin_dsn='postgresql://postgres:admin@pg.example.test/postgres?sslmode=verify-full',pg_ca_path=ca)
    request=pipe_request(settings,'test-only-cdc-secret')
    assert request['destination']=={'database':'pricing'}
    source=request['source']['postgres']
    assert source['credentials']=={'username':'pricing_cdc','password':'test-only-cdc-secret'}
    assert source['disableTls'] is False and source['skipCertVerification'] is False
    assert source['caCertificate']=='test-public-ca'
    assert source['settings']['syncIntervalSeconds']==10
    assert source['settings']['allowNullableColumns'] is True
    assert source['settings']['replicationMode']=='cdc'
    assert {row['targetTable'] for row in source['tableMappings']}=={'sales_transactions_cdc','inventory_history_cdc','price_history_cdc','cost_history_cdc'}
    assert all(row['tableEngine']=='ReplacingMergeTree' for row in source['tableMappings'])


def test_cloud_api_uses_saved_cli_credentials_and_omits_raw_errors(tmp_path,monkeypatch):
    from infra.setup_cloud import CloudApi
    monkeypatch.chdir(tmp_path)
    (tmp_path/'.clickhouse').mkdir()
    (tmp_path/'.clickhouse/credentials.json').write_text(json.dumps({'api_key':'key-only-test','api_secret':'secret-only-test'}))
    def handler(request):
        assert request.url.path==f'/v1/organizations/{ORG}/services/{CH}/clickpipes'
        assert request.headers['authorization'].startswith('Basic ')
        return httpx.Response(403,json={'error':'source-password-secret'})
    with CloudApi(ORG,transport=httpx.MockTransport(handler)) as api:
        with pytest.raises(WorkshopError) as error: api.create_pipe(CH,{})
    assert 'source-password-secret' not in str(error.value)


def test_create_pipe_records_id_before_poll_and_never_recreates(tmp_path):
    from infra.setup_cloud import ensure_pipe
    from infra.provision import ResourceInventory
    settings=Settings(cloud_org_id=ORG,resource_inventory_path=tmp_path/'resources.json')
    inv=ResourceInventory(owner_id='learner-1',organization_id=UUID(ORG),region='ap-southeast-1',created_at=datetime.now(UTC),service_id=UUID(CH))
    class Api:
        calls=0
        def create_pipe(self,service,body):
            self.calls+=1
            assert ResourceInventory.model_validate_json(settings.resource_inventory_path.read_text()).pending_creation=='clickpipe'
            return {'id':PIPE}
    api=Api()
    result=ensure_pipe(inv,settings,api,{})
    assert result.clickpipe_id==UUID(PIPE) and result.pending_creation is None
    assert ensure_pipe(result,settings,api,{}).clickpipe_id==UUID(PIPE)
    assert api.calls==1


def test_lost_pipe_response_blocks_duplicate_creation(tmp_path):
    from infra.setup_cloud import ensure_pipe
    from infra.provision import ResourceInventory
    settings=Settings(cloud_org_id=ORG,resource_inventory_path=tmp_path/'resources.json')
    inv=ResourceInventory(owner_id='learner-1',organization_id=UUID(ORG),region='ap-southeast-1',created_at=datetime.now(UTC),service_id=UUID(CH))
    class Api:
        def create_pipe(self,*args): raise WorkshopError('cloud_api_failed','No usable response.')
    with pytest.raises(WorkshopError): ensure_pipe(inv,settings,Api(),{})
    saved=ResourceInventory.model_validate_json(settings.resource_inventory_path.read_text())
    assert saved.pending_creation=='clickpipe'
    with pytest.raises(WorkshopError,match='creation_ack_unknown'): ensure_pipe(saved,settings,Api(),{})


@pytest.mark.integration
def test_setup_roles_and_app_dsn_are_idempotent_and_preserve_tls(ledger,tmp_path,postgres_dsn):
    from infra.setup_cloud import configure_roles
    from infra.provision import save_private
    settings=Settings(private_env_path=tmp_path/'private.env',postgres_admin_dsn=postgres_dsn+'?sslmode=disable')
    save_private(settings,{})
    first=configure_roles(settings,ledger)
    second=configure_roles(settings,ledger)
    assert first==second
    assert 'pricing_app:' in first
    assert first.endswith('?sslmode=disable')
    assert settings.private_env_path.stat().st_mode & 0o777==0o600
    with ledger.connect() as conn:
        assert conn.execute("SELECT rolreplication FROM pg_roles WHERE rolname='pricing_cdc'").fetchone()['rolreplication']
        assert not conn.execute("SELECT has_table_privilege('pricing_app','pricing.sales_transactions','DELETE') AS allowed").fetchone()['allowed']


def test_existing_pipe_requires_saved_replication_secret_before_role_changes(tmp_path):
    from infra.setup_cloud import configure_roles
    settings=Settings(private_env_path=tmp_path/'env')
    class Ledger:
        def connect(self): raise AssertionError('Existing pipe role must not be changed.')
    with pytest.raises(WorkshopError,match='existing_cdc_credentials_required'):
        configure_roles(settings,Ledger(),existing_pipe=True)


def test_setup_verifies_source_endpoint_before_schema_writes(tmp_path,monkeypatch):
    from infra.setup_cloud import setup_cloud
    from infra.provision import ResourceInventory
    settings=Settings(cloud_org_id=ORG,resource_inventory_path=tmp_path/'resources.json',private_env_path=tmp_path/'env',postgres_admin_dsn='postgresql://postgres:private@wrong.example.test/postgres?sslmode=verify-full')
    inv=ResourceInventory(owner_id='learner-1',organization_id=UUID(ORG),region='ap-southeast-1',created_at=datetime.now(UTC),service_id=UUID(CH),postgres_id=UUID('22222222-2222-4222-8222-222222222222'))
    monkeypatch.setattr('infra.setup_cloud._provision',lambda *args,**kwargs:inv)
    monkeypatch.setattr('infra.setup_cloud.initialize_schema',lambda *args:pytest.fail('Wrong source must not be initialized.'))
    class Runner:
        def run(self,argv): return {'hostname':'owned.example.test'}
    with pytest.raises(WorkshopError,match='source_endpoint_mismatch'):
        setup_cloud(settings,Runner(),None,progress=lambda _:None)


def test_setup_completes_views_and_rerun_reuses_same_pipe(tmp_path,monkeypatch):
    from infra.setup_cloud import setup_cloud
    from infra.provision import ResourceInventory
    settings=Settings(cloud_org_id=ORG,resource_inventory_path=tmp_path/'resources.json',private_env_path=tmp_path/'env',postgres_admin_dsn=f'postgresql://postgres:private@pg.example.test/postgres?sslmode=verify-full&sslrootcert={tmp_path}/ca.pem',pg_ca_path=tmp_path/'ca.pem')
    settings.pg_ca_path.write_text('public-ca')
    inv=ResourceInventory(owner_id='learner-1',organization_id=UUID(ORG),region='ap-southeast-1',created_at=datetime.now(UTC),service_id=UUID(CH),postgres_id=UUID('22222222-2222-4222-8222-222222222222'))
    def provision(*args,**kwargs):
        return ResourceInventory.model_validate_json(settings.resource_inventory_path.read_text()) if settings.resource_inventory_path.exists() else inv
    monkeypatch.setattr('infra.setup_cloud._provision',provision)
    class Ledger:
        def current_scope(self,*args): return None,datetime.now(UTC)
    monkeypatch.setattr('infra.setup_cloud.initialize_schema',lambda *args:Ledger())
    from infra.provision import save_private
    def roles(*args,**kwargs): save_private(settings,{'PRICING_POSTGRES_CDC_PASSWORD':'private-cdc'})
    monkeypatch.setattr('infra.setup_cloud.configure_roles',roles)
    monkeypatch.setattr('infra.setup_cloud.wait_pipe',lambda *args,**kwargs:None)
    class Analytics:
        def __init__(self,*args): pass
        def close(self): pass
    views=[]
    class Features:
        def __init__(self,*args): pass
        def configure_views(self): views.append('ready')
    monkeypatch.setattr('infra.setup_cloud.AnalyticsClient',Analytics)
    monkeypatch.setattr('infra.setup_cloud.FeatureService',Features)
    class Runner:
        def run(self,argv):
            if argv[2]=='clickpipe':
                return {'id':PIPE,'name':'pricing-learner-1-cdc','source':{'postgres':{'host':'pg.example.test','port':5432,'database':'postgres','authentication':'basic','disableTls':False,'skipCertVerification':False,'caCertificate':'public-ca',
                    'settings':{'publicationName':'pricing_workshop','replicationMode':'cdc','allowNullableColumns':True},
                    'tableMappings':[{'sourceSchemaName':'pricing','sourceTable':name,'targetTable':name+'_cdc','tableEngine':'ReplacingMergeTree'} for name in ('sales_transactions','inventory_history','price_history','cost_history')]}}}
            return {'hostname':'pg.example.test'}
    class Api:
        calls=0
        def create_pipe(self,*args): self.calls+=1;return {'id':PIPE}
    api=Api()
    assert setup_cloud(settings,Runner(),api,progress=lambda _:None).clickpipe_id==UUID(PIPE)
    assert setup_cloud(settings,Runner(),api,progress=lambda _:None).clickpipe_id==UUID(PIPE)
    assert api.calls==1 and views==['ready','ready']


def test_custom_env_path_is_also_used_for_generated_credentials(tmp_path,monkeypatch):
    path=tmp_path/'private.env';path.write_text('PRICING_OWNER_ID=separate-learner\n')
    monkeypatch.setenv('PRICING_PRIVATE_ENV_PATH',str(path))
    assert Settings.from_env().private_env_path==path


def test_saved_pipe_name_is_checked_before_source_changes(tmp_path,monkeypatch):
    from infra.setup_cloud import setup_cloud
    from infra.provision import ResourceInventory
    settings=Settings(cloud_org_id=ORG,resource_inventory_path=tmp_path/'resources.json',private_env_path=tmp_path/'env',postgres_admin_dsn='postgresql://postgres:private@pg.example.test/postgres?sslmode=verify-full')
    inv=ResourceInventory(owner_id='learner-1',organization_id=UUID(ORG),region='ap-southeast-1',created_at=datetime.now(UTC),service_id=UUID(CH),postgres_id=UUID('22222222-2222-4222-8222-222222222222'),clickpipe_id=UUID(PIPE))
    monkeypatch.setattr('infra.setup_cloud._provision',lambda *args,**kwargs:inv)
    monkeypatch.setattr('infra.setup_cloud.initialize_schema',lambda *args:pytest.fail('Unverified pipe must not change source schema.'))
    class Runner:
        def run(self,argv):
            return {'id':PIPE,'name':'wrong-owner-pipe'} if argv[2]=='clickpipe' else {'hostname':'pg.example.test'}
    with pytest.raises(WorkshopError,match='pricing-learner-1-cdc'):
        setup_cloud(settings,Runner(),None,progress=lambda _:None)


@pytest.mark.parametrize('query',['sslmode=disable','sslmode=require','sslmode=verify-full','sslmode=verify-full&sslrootcert=/wrong/ca.pem'])
def test_setup_rejects_unverified_administrator_tls(tmp_path,query):
    from infra.setup_cloud import verify_admin_tls
    ca=tmp_path/'ca.pem';ca.write_text('public-ca')
    settings=Settings(pg_ca_path=ca,postgres_admin_dsn='postgresql://postgres:private@pg.example.test/postgres?'+query)
    with pytest.raises(WorkshopError,match='source_tls_required'): verify_admin_tls(settings)


@pytest.mark.parametrize('defect',['host','database','tls','publication','mapping'])
def test_pipe_source_changes_are_rejected(tmp_path,defect):
    from infra.setup_cloud import verify_pipe_source
    ca=tmp_path/'ca.pem';ca.write_text('public-ca')
    settings=Settings(pg_ca_path=ca,postgres_admin_dsn='postgresql://postgres:private@pg.example.test/postgres')
    source={'host':'pg.example.test','port':5432,'database':'postgres','authentication':'basic','disableTls':False,'skipCertVerification':False,'caCertificate':'public-ca',
            'settings':{'publicationName':'pricing_workshop','replicationMode':'cdc','allowNullableColumns':True},
            'tableMappings':[{'sourceSchemaName':'pricing','sourceTable':name,'targetTable':name+'_cdc','tableEngine':'ReplacingMergeTree'} for name in ('sales_transactions','inventory_history','price_history','cost_history')]}
    if defect=='host': source['host']='other.example.test'
    if defect=='database': source['database']='other'
    if defect=='tls': source['skipCertVerification']=True
    if defect=='publication': source['settings']['publicationName']='other'
    if defect=='mapping': source['tableMappings'].pop()
    with pytest.raises(WorkshopError,match='clickpipe_source_mismatch'): verify_pipe_source({'source':{'postgres':source}},settings)


@pytest.mark.integration
@pytest.mark.parametrize('defect',['app_replication','cdc_write','app_delete'])
def test_setup_refuses_existing_overprivileged_roles(ledger,tmp_path,postgres_dsn,defect):
    from infra.setup_cloud import configure_roles
    settings=Settings(private_env_path=tmp_path/'private.env',postgres_admin_dsn=postgres_dsn+'?sslmode=disable')
    configure_roles(settings,ledger)
    with ledger.connect() as conn:
        if defect=='app_replication': conn.execute('ALTER ROLE pricing_app REPLICATION')
        elif defect=='cdc_write': conn.execute('GRANT UPDATE ON pricing.sales_transactions TO pricing_cdc')
        else: conn.execute('GRANT DELETE ON pricing.sales_transactions TO pricing_app')
    try:
        with pytest.raises(WorkshopError,match='source_role_privileges'): configure_roles(settings,ledger)
    finally:
        if defect=='app_replication':
            with ledger.connect() as conn: conn.execute('ALTER ROLE pricing_app NOREPLICATION')
