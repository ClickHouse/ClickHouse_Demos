from datetime import UTC,datetime
import json
from uuid import UUID
import pytest
from pricing_lab.settings import Settings
from pricing_lab.contracts import WorkshopError
from infra.provision import ResourceInventory,provision,recover_password
from infra.cleanup import cleanup,OwnerMismatch

CH='11111111-1111-4111-8111-111111111111'
PG='22222222-2222-4222-8222-222222222222'
PIPE='33333333-3333-4333-8333-333333333333'
ORG='44444444-4444-4444-8444-444444444444'

@pytest.fixture
def settings(tmp_path):
    return Settings(cloud_org_id=ORG,learner_cidr='203.0.113.12/32',region='ap-southeast-1',resource_inventory_path=tmp_path/'resources.json',private_env_path=tmp_path/'.env.workshop',pg_ca_path=tmp_path/'ca.pem')

@pytest.fixture
def inventory():
    return ResourceInventory(owner_id='learner-1',organization_id=UUID(ORG),region='ap-southeast-1',created_at=datetime.now(UTC),service_id=UUID(CH),postgres_id=UUID(PG),clickpipe_id=UUID(PIPE))

class Runner:
    def __init__(self): self.calls=[];self.polls=0
    def run(self,argv):
        self.calls.append(argv)
        kind=argv[2];action=argv[3]
        if action=='create':
            if kind=='service': return {'service':{'id':CH},'password':'secret-ch'}
            return {'id':PG,'password':'secret-pg','username':'postgres','connectionString':'postgresql://postgres:secret-pg@pg.example.test:5432/postgres'}
        if action=='get':
            self.polls+=1
            row={'id':argv[4],'state':'starting' if self.polls==1 else 'running','name':'pricing-learner-1'+('-source' if kind=='postgres' else ''),'tags':[{'key':'workshop','value':'dynamic-pricing'},{'key':'owner','value':'learner-1'}]}
            if kind=='service': row['endpoints']=[{'protocol':'https','host':'ch.example.test','port':8443}]
            if kind=='postgres': row.update(hostname='pg.example.test',username='postgres')
            return row
        if action=='certs':
            from pathlib import Path
            Path(argv[argv.index('--output')+1]).write_text('test-only public certificate')
            return {}
        if action=='reset-password': return {'password':'recovered-secret'}
        return {}


def test_provision_polls_and_persists_only_private_secrets(settings):
    runner=Runner()
    result=provision(runner,settings,'setup',sleep=lambda _:None)
    assert result.service_id==UUID(CH) and result.postgres_id==UUID(PG)
    assert runner.polls>=3
    assert '--ip-allow' in runner.calls[0] and '203.0.113.12/32' in runner.calls[0]
    assert 'secret' not in json.dumps(runner.calls)
    assert 'secret' not in settings.resource_inventory_path.read_text()
    assert 'secret-ch' in settings.private_env_path.read_text()
    assert settings.private_env_path.stat().st_mode & 0o777 == 0o600
    assert 'sslmode=verify-full' in settings.private_env_path.read_text()
    creates=sum('create' in c for c in runner.calls)
    provision(runner,settings,'setup',sleep=lambda _:None)
    assert sum('create' in c for c in runner.calls)==creates


def test_cleanup_preview_never_calls_runner(inventory,settings):
    runner=Runner();plan=cleanup(inventory,runner,settings)
    assert runner.calls==[]
    assert [row['kind'] for row in plan]==['clickpipe','postgres','service']
    assert all('s3' not in str(row) and 'model' not in str(row) for row in plan)


def test_cleanup_requires_matching_owner(inventory,settings):
    changed=inventory.model_copy(update={'owner_id':'other'})
    runner=Runner()
    with pytest.raises(OwnerMismatch): cleanup(changed,runner,settings,execute=True)
    assert runner.calls==[]


def test_cleanup_verifies_tags_before_any_delete(inventory,settings):
    runner=Runner()
    original=runner.run
    def run(argv):
        result=original(argv)
        if argv[2:4]==['postgres','get']: result['tags']=[]
        return result
    runner.run=run
    with pytest.raises(OwnerMismatch): cleanup(inventory,runner,settings,execute=True)
    assert not any('delete' in call for call in runner.calls)


def test_lost_password_requires_explicit_recovery(inventory,settings):
    settings.resource_inventory_path.write_text(inventory.model_dump_json())
    runner=Runner()
    with pytest.raises(WorkshopError,match='credentials_missing'):
        provision(runner,settings,'setup',sleep=lambda _:None)
    assert not any('create' in c or 'reset-password' in c for c in runner.calls)
    recover_password(inventory,runner,settings,'postgres')
    assert 'recovered-secret' in settings.private_env_path.read_text()
    assert 'recovered-secret' not in json.dumps(runner.calls)


def test_service_failure_retains_inventory_and_creation_secret(settings):
    runner=Runner();original=runner.run
    def run(argv):
        result=original(argv)
        if argv[2:4]==['service','get']: result['state']='failed'
        return result
    runner.run=run
    with pytest.raises(WorkshopError,match='cloud_service_failed'):
        provision(runner,settings,'setup',sleep=lambda _:None)
    assert json.loads(settings.resource_inventory_path.read_text())['service_id']==CH
    assert 'secret-ch' in settings.private_env_path.read_text()


def test_literal_credentials_and_container_certificate_path(settings,monkeypatch):
    from infra.provision import save_private
    from dotenv import dotenv_values
    value="secret${SHOULD_NOT_EXPAND}'\\value"
    save_private(settings,{'PRICING_CLICKHOUSE_PASSWORD':value})
    assert dotenv_values(settings.private_env_path,interpolate=False)['PRICING_CLICKHOUSE_PASSWORD']==value
    monkeypatch.setenv('PRICING_PRIVATE_ENV_PATH',str(settings.private_env_path))
    monkeypatch.setenv('SHOULD_NOT_EXPAND','changed')
    assert Settings.from_env().clickhouse_password.get_secret_value()==value
    monkeypatch.setenv('PRICING_LAB_ROOT','/opt/pricing')
    configured=Settings(postgres_dsn='postgresql://user:password@pg.example.test/postgres?sslmode=verify-full&sslrootcert=/old/ca.pem')
    assert '%2Fopt%2Fpricing%2F.run%2Fmanaged-postgres-ca.pem' in configured.postgres_dsn.get_secret_value()


def test_interrupted_create_is_not_reissued(settings):
    runner=Runner()
    runner.run=lambda argv:(_ for _ in ()).throw(WorkshopError('cloud_command_failed','Check the console.'))
    with pytest.raises(WorkshopError): provision(runner,settings,'setup',sleep=lambda _:None)
    clean=Runner()
    with pytest.raises(WorkshopError,match='creation_ack_unknown'): provision(clean,settings,'setup',sleep=lambda _:None)
    assert clean.calls==[]

@pytest.mark.integration
def test_initialization_restart_preserves_source_state(ledger,source,scope,subject,sale_request,postgres_dsn,analytics,monkeypatch,capsys):
    from pricing_lab.cli import main
    source.create_sale(scope,sale_request)
    monkeypatch.setenv('PRICING_POSTGRES_DSN','postgresql://unusable_app_role:wrong@127.0.0.1:1/postgres')
    monkeypatch.setenv('PRICING_POSTGRES_ADMIN_DSN',postgres_dsn)
    monkeypatch.setenv('PRICING_CLICKHOUSE_HOST','127.0.0.1')
    monkeypatch.setenv('PRICING_CLICKHOUSE_PORT',str(analytics.settings.clickhouse_port))
    monkeypatch.setenv('PRICING_CLICKHOUSE_DATABASE',analytics.database)
    monkeypatch.setenv('PRICING_CLICKHOUSE_SECURE','false')
    monkeypatch.setenv('PRICING_CLICKHOUSE_PASSWORD','test-only-password')
    assert main(['initialize'])==0
    assert 'preserved_existing_state' in capsys.readouterr().out
    assert ledger.current_state(scope,subject)['stock_units']==28
    assert ledger.current_scope(scope.owner_id,scope.scenario_id)[0]==scope


@pytest.mark.integration
def test_source_grants_use_only_the_four_mapped_tables(ledger):
    from infra.configure_cdc import configure_source
    with ledger.connect() as conn:
        for name in ('pricing_app','pricing_cdc'):
            if not conn.execute('SELECT 1 FROM pg_roles WHERE rolname=%s',(name,)).fetchone():
                from psycopg import sql
                conn.execute(sql.SQL('CREATE ROLE {} LOGIN').format(sql.Identifier(name)))
    result=configure_source(ledger)
    assert result=={'publication':'pricing_workshop','tables':4}
    assert configure_source(ledger)==result
    with ledger.connect() as conn:
        assert conn.execute("SELECT rolreplication FROM pg_roles WHERE rolname='pricing_cdc'").fetchone()['rolreplication']

        assert not conn.execute("SELECT has_table_privilege('pricing_app','pricing.sales_transactions','DELETE') AS allowed").fetchone()['allowed']
        assert conn.execute("SELECT has_table_privilege(current_user,'pricing.sales_transactions','DELETE') AS allowed").fetchone()['allowed']


def test_cleanup_journals_partial_deletion_and_resumes(inventory,settings):
    class DeleteRunner(Runner):
        def run(self,argv):
            if argv[2:4]==['clickpipe','get']:
                return {'id':PIPE,'name':'pricing-learner-1-cdc'}
            if argv[2:4]==['service','delete']:
                raise WorkshopError('cloud_command_failed','Timed out stopping the service.')
            return super().run(argv)
    runner=DeleteRunner()
    with pytest.raises(WorkshopError): cleanup(inventory,runner,settings,execute=True)
    saved=ResourceInventory.model_validate_json(settings.resource_inventory_path.read_text())
    assert set(saved.deleted_resources)=={'clickpipe','postgres'}
    next_runner=Runner()
    cleanup(saved,next_runner,settings,execute=True)
    assert all(call[2]=='service' for call in next_runner.calls)
    assert set(ResourceInventory.model_validate_json(settings.resource_inventory_path.read_text()).deleted_resources)=={'clickpipe','postgres','service'}


def test_cleanup_recovers_lost_delete_ack_only_with_confirmed_absence(inventory,settings):
    inventory=inventory.model_copy(update={'clickpipe_id':None,'postgres_id':None})
    class AbsentRunner(Runner):
        def run(self,argv):
            self.calls.append(argv)
            if argv[3]=='get': raise WorkshopError('cloud_command_failed','Unavailable.')
            if argv[3]=='list': return []
            raise AssertionError('Absent service must not be deleted again.')
    runner=AbsentRunner()
    cleanup(inventory,runner,settings,execute=True)
    assert 'service' in ResourceInventory.model_validate_json(settings.resource_inventory_path.read_text()).deleted_resources
    runner.run=lambda argv: (_ for _ in ()).throw(WorkshopError('cloud_command_failed','Access denied.'))
    with pytest.raises(WorkshopError): cleanup(inventory,runner,settings,execute=True)


def test_recover_admin_password_preserves_app_connection(inventory,settings):
    from infra.provision import save_private
    from dotenv import dotenv_values
    save_private(settings,{'PRICING_POSTGRES_DSN':'postgresql://pricing_app:app-secret@pg.example.test/postgres'})
    recover_password(inventory,Runner(),settings,'postgres')
    values=dotenv_values(settings.private_env_path)
    assert 'pricing_app:app-secret' in values['PRICING_POSTGRES_DSN']
    assert 'recovered-secret' in values['PRICING_POSTGRES_ADMIN_DSN']


def test_force_service_delete_has_long_timeout(monkeypatch):
    import subprocess
    from infra.provision import CliRunner
    calls=[]
    def run(argv,**kwargs):
        calls.append(kwargs['timeout'])
        return subprocess.CompletedProcess(argv,0,stdout='{}')
    monkeypatch.setattr(subprocess,'run',run)
    runner=CliRunner()
    runner.run(['clickhousectl','cloud','service','delete',CH,'--force'])
    runner.run(['clickhousectl','cloud','postgres','get',PG])
    assert calls==[900,60]


@pytest.mark.parametrize('listing',[{},[{'id':CH}],[{}]])
def test_cleanup_does_not_infer_absence_from_unusable_or_present_inventory(inventory,settings,listing):
    inventory=inventory.model_copy(update={'clickpipe_id':None,'postgres_id':None})
    class UnverifiedRunner(Runner):
        def run(self,argv):
            self.calls.append(argv)
            if argv[3]=='get': raise WorkshopError('cloud_command_failed','Unavailable.')
            if argv[3]=='list': return listing
            raise AssertionError('Unverified resource must not be deleted.')
    runner=UnverifiedRunner()
    with pytest.raises(WorkshopError): cleanup(inventory,runner,settings,execute=True)
    assert not settings.resource_inventory_path.exists()


def test_cleanup_pipe_name_error_identifies_expected_name(inventory,settings):
    class WrongNameRunner(Runner):
        def run(self,argv):
            if argv[2:4]==['clickpipe','get']: return {'id':PIPE,'name':'wrong-pipe'}
            return super().run(argv)
    runner=WrongNameRunner()
    with pytest.raises(WorkshopError,match='pricing-learner-1-cdc'):
        cleanup(inventory,runner,settings,execute=True)
    assert not any('delete' in call for call in runner.calls)


def test_idle_clickhouse_is_ready_without_polling_to_timeout(settings):
    from infra.provision import wait_running
    runner=Runner();original=runner.run
    def run(argv):
        row=original(argv);row['state']='idle';return row
    runner.run=run
    assert wait_running(runner,settings,'service',CH,sleep=lambda _:None)['state']=='idle'
    assert runner.polls==1


@pytest.mark.parametrize('defect,error',[
    ('database','clickpipe_destination_mismatch'),
    ('engine','cdc_schema_missing'),
    ('missing_table','cdc_schema_missing'),
    ('metadata','cdc_schema_missing'),
    ('slow','clickpipe_sync_interval'),
    ('missing_interval','clickpipe_sync_interval'),
    ('name','clickpipe_name_mismatch'),
    ('none',None),
])
def test_cdc_readiness_checks_running_pipe_configuration(settings,inventory,monkeypatch,defect,error):
    from infra.provision import save_private
    settings.resource_inventory_path.write_text(inventory.model_dump_json())
    save_private(settings,{'PRICING_CLICKHOUSE_PASSWORD':'test-only', 'PRICING_POSTGRES_ADMIN_DSN':'postgresql://postgres:test-only@pg.example.test/postgres'})
    class MetadataClient:
        def __init__(self,settings): self.database=settings.clickhouse_database
        def query(self,sql,parameters):
            if 'system.tables' in sql:
                if defect=='missing_table' and parameters['table']=='cost_history_cdc': return []
                return [{'engine':'SharedMergeTree' if defect=='engine' else 'SharedReplacingMergeTree'}]
            names=['_peerdb_version','_peerdb_is_deleted','_peerdb_synced_at','owner_id','scenario_id','generation']
            if defect=='metadata': names.remove('_peerdb_synced_at')
            return [{'name':name} for name in names]
        def close(self): pass
    monkeypatch.setattr('pricing_lab.analytics.AnalyticsClient',MetadataClient)
    runner=Runner();original=runner.run
    def run(argv):
        if argv[2:4]==['clickpipe','get']:
            return {'id':PIPE,'name':'wrong' if defect=='name' else 'pricing-learner-1-cdc','state':'Running',
                    'destination':{'database':'default' if defect=='database' else 'pricing'},
                    'source':{'postgres':{'settings':{'syncIntervalSeconds':None if defect=='missing_interval' else 60 if defect=='slow' else 20}}}}
        return original(argv)
    runner.run=run
    if error:
        with pytest.raises(WorkshopError,match=error): provision(runner,settings,'cdc',sleep=lambda _:None)
    else:
        assert provision(runner,settings,'cdc',sleep=lambda _:None).clickpipe_id==UUID(PIPE)


def test_unit_settings_ignore_inherited_learner_owner():
    assert Settings().owner_id=='learner-1'
