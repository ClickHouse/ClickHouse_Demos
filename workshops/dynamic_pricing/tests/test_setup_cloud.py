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


def test_fresh_template_has_no_local_database_or_manual_owner(tmp_path,monkeypatch):
    from pricing_lab.settings import LAB_ROOT
    env=tmp_path/'workshop.env'
    env.write_text((LAB_ROOT/'app/.env.workshop.example').read_text())
    monkeypatch.setenv('PRICING_PRIVATE_ENV_PATH',str(env))
    settings=Settings.from_env()
    assert settings.clickhouse_host==''
    assert settings.postgres_dsn.get_secret_value()==''
    assert settings.postgres_admin_dsn.get_secret_value()==''
    assert 'PRICING_OWNER_ID=' not in env.read_text()
    assert 'PRICING_CLOUD_ORG_ID=' not in env.read_text()
    assert 'PRICING_LEARNER_CIDR=' not in env.read_text()


def test_setup_generates_and_preserves_installation_identity(tmp_path,monkeypatch):
    from infra.setup_cloud import installation_settings
    from dotenv import dotenv_values
    env=tmp_path/'workshop.env';env.write_text('PRICING_CLICKHOUSE_HOST=\n')
    monkeypatch.setenv('PRICING_PRIVATE_ENV_PATH',str(env))
    monkeypatch.setenv('PRICING_RESOURCE_INVENTORY_PATH',str(tmp_path/'resources.json'))
    first=installation_settings()
    second=installation_settings()
    assert first.owner_id.startswith('pricing-')
    assert second.owner_id==first.owner_id
    assert dotenv_values(env)['PRICING_OWNER_ID']==first.owner_id
    assert env.stat().st_mode & 0o777==0o600


def test_setup_recovers_identity_from_existing_inventory(tmp_path,monkeypatch):
    from infra.setup_cloud import installation_settings
    from infra.provision import ResourceInventory
    env=tmp_path/'workshop.env';env.write_text('')
    inventory=tmp_path/'resources.json'
    inventory.write_text(ResourceInventory(owner_id='existing-installation',organization_id=ORG,region='ap-southeast-1',created_at=datetime.now(UTC)).model_dump_json())
    monkeypatch.setenv('PRICING_PRIVATE_ENV_PATH',str(env))
    monkeypatch.setenv('PRICING_RESOURCE_INVENTORY_PATH',str(inventory))
    assert installation_settings().owner_id=='existing-installation'


def test_setup_preserves_explicit_legacy_owner(tmp_path,monkeypatch):
    from infra.setup_cloud import installation_settings
    env=tmp_path/'workshop.env';env.write_text('PRICING_OWNER_ID=my-existing-owner\n')
    monkeypatch.setenv('PRICING_PRIVATE_ENV_PATH',str(env))
    monkeypatch.setenv('PRICING_RESOURCE_INVENTORY_PATH',str(tmp_path/'resources.json'))
    assert installation_settings().owner_id=='my-existing-owner'


def test_missing_cloud_endpoint_never_falls_back_to_localhost(monkeypatch):
    import pytest
    from pricing_lab.analytics import AnalyticsClient
    from pricing_lab.settings import Settings
    from pricing_lab.contracts import WorkshopError
    monkeypatch.setattr('pricing_lab.analytics.clickhouse_connect.get_client',lambda **kwargs:pytest.fail('An empty Cloud endpoint must never reach the driver.'))
    analytics=AnalyticsClient(Settings(clickhouse_host=''))
    with pytest.raises(WorkshopError,match='cloud_endpoint_missing'):
        analytics.bootstrap_database()
    with pytest.raises(WorkshopError,match='cloud_endpoint_missing'):
        analytics.query('SELECT 1')


def test_cloud_settings_detects_organization_and_ipv4_without_env_fields(tmp_path):
    from infra.setup_cloud import cloud_settings
    settings=Settings(resource_inventory_path=tmp_path/'resources.json')
    class Runner:
        def run(self,argv):
            assert argv==['clickhousectl','cloud','org','list','--json']
            return [{'id':ORG,'name':'Workshop'}]
    resolved=cloud_settings(settings,Runner(),detect_ip=lambda:'203.0.113.12')
    assert str(resolved.cloud_org_id)==ORG
    assert resolved.learner_cidr=='203.0.113.12/32'


def test_cloud_settings_requires_choice_for_multiple_organizations(tmp_path):
    from infra.setup_cloud import cloud_settings
    class Runner:
        def run(self,argv): return [{'id':CH,'name':'First'},{'id':ORG,'name':'Second'}]
    settings=Settings(resource_inventory_path=tmp_path/'resources.json')
    resolved=cloud_settings(settings,Runner(),detect_ip=lambda:'203.0.113.12',choose=lambda _: '2')
    assert str(resolved.cloud_org_id)==ORG
    with pytest.raises(WorkshopError,match='organization_selection_invalid'):
        cloud_settings(settings,Runner(),detect_ip=lambda:'203.0.113.12',choose=lambda _: '0')


def test_runtime_reads_saved_organization_for_cleanup(tmp_path,monkeypatch):
    from infra.provision import ResourceInventory
    inventory=tmp_path/'resources.json'
    inventory.write_text(ResourceInventory(owner_id='saved-owner',organization_id=ORG,region='ap-southeast-1',created_at=datetime.now(UTC)).model_dump_json())
    monkeypatch.setenv('PRICING_RESOURCE_INVENTORY_PATH',str(inventory))
    assert str(Settings.from_env().cloud_org_id)==ORG


def test_public_ipv4_detection_reports_failure_without_creating_resources(monkeypatch):
    from infra.setup_cloud import public_ipv4
    monkeypatch.setattr('infra.setup_cloud.httpx.get',lambda *a,**k: httpx.Response(200,text='::1',request=httpx.Request('GET','https://api4.ipify.org')))
    with pytest.raises(WorkshopError,match='public_ip_unavailable'): public_ipv4()


def test_clickpipe_wait_reports_progress(tmp_path,monkeypatch):
    from infra.setup_cloud import wait_pipe
    from infra.provision import ResourceInventory
    inventory=ResourceInventory(owner_id='learner-1',organization_id=ORG,region='ap-southeast-1',created_at=datetime.now(UTC),service_id=CH,clickpipe_id=PIPE)
    class Runner:
        polls=0
        def run(self,argv):
            self.polls+=1
            return {'id':PIPE,'state':'Running' if self.polls==12 else 'Starting'}
    monkeypatch.setattr('infra.setup_cloud.verify_pipe_source',lambda *args:None)
    monkeypatch.setattr('infra.setup_cloud.verify_clickpipe',lambda *args:None)
    messages=[]
    wait_pipe(inventory,Settings(),Runner(),sleep=lambda _:None,progress=messages.append)
    assert len(messages)>=4
    assert 'starting' in messages[0]
    assert 'schemas are ready' in messages[-1]


def test_runtime_settings_do_not_read_private_provisioning_inventory(tmp_path,monkeypatch):
    path=tmp_path/'resources.json';path.write_text('{"organization_id":"'+ORG+'"}')
    monkeypatch.setenv('PRICING_RESOURCE_INVENTORY_PATH',str(path))
    original=Path.read_text
    def private_read(file,*args,**kwargs):
        if file==path: raise PermissionError('Private provisioning inventory')
        return original(file,*args,**kwargs)
    monkeypatch.setattr(Path,'read_text',private_read)
    assert Settings.from_env(include_inventory=False).cloud_org_id is None
