"""One resumable learner-owned setup. Secrets never enter process arguments."""
from datetime import datetime
import fcntl
import json
import ipaddress
import os
from pathlib import Path
import secrets
import time
from urllib.parse import quote,urlsplit,urlunsplit,parse_qs
from uuid import UUID

from dotenv import dotenv_values
import httpx
from psycopg import sql
from pricing_lab.analytics import AnalyticsClient,identifier
from pricing_lab.contracts import Scope,Subject,WorkshopError
from pricing_lab.features import FeatureService
from pricing_lab.clock import Clock
from pricing_lab.ledger import Ledger
from pricing_lab.settings import Settings,LAB_ROOT
from .configure_cdc import configure_source
from .provision import CliRunner,ResourceInventory,_provision,command,save_inventory,save_private,verify_clickpipe,verify_pipe_name,report_progress

TABLES=('sales_transactions','inventory_history','price_history','cost_history')


def installation_settings():
    """Persist one installation identity before any Cloud resources are created."""
    settings=Settings.from_env()
    path=settings.private_env_path.with_suffix('.identity.lock')
    path.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(path,os.O_CREAT|os.O_RDWR,0o600)
    try:
        fcntl.flock(fd,fcntl.LOCK_EX)
        settings=Settings.from_env()
        values=dotenv_values(settings.private_env_path,interpolate=False)
        explicit=os.environ.get('PRICING_OWNER_ID') or values.get('PRICING_OWNER_ID')
        if explicit:
            owner=explicit
        elif settings.resource_inventory_path.exists():
            owner=ResourceInventory.model_validate_json(settings.resource_inventory_path.read_text()).owner_id
        else:
            owner='pricing-'+secrets.token_hex(8)
        settings=Settings(**{**settings.model_dump(),'owner_id':owner})
        save_private(settings,{'PRICING_OWNER_ID':owner})
        return settings
    finally: os.close(fd)


def public_ipv4():
    try:
        response=httpx.get('https://api4.ipify.org',timeout=15)
        response.raise_for_status()
        address=ipaddress.ip_address(response.text.strip())
        if not isinstance(address,ipaddress.IPv4Address) or not address.is_global:
            raise ValueError('Expected a public IPv4 address.')
        return str(address)
    except (httpx.HTTPError,ValueError):
        raise WorkshopError('public_ip_unavailable','Public IPv4 detection failed. Check your internet connection and rerun setup.') from None


def cloud_settings(settings,runner,*,detect_ip=public_ipv4,choose=input):
    organization=settings.cloud_org_id
    if not organization and settings.resource_inventory_path.exists():
        organization=ResourceInventory.model_validate_json(settings.resource_inventory_path.read_text()).organization_id
    if not organization:
        rows=runner.run(['clickhousectl','cloud','org','list','--json'])
        if isinstance(rows,dict): rows=rows.get('result',rows.get('organizations',[]))
        if not isinstance(rows,list) or not rows:
            raise WorkshopError('organization_unavailable','Log in with a Cloud organization API key before running setup.')
        if len(rows)==1:
            organization=rows[0]['id']
        else:
            print('Choose the Cloud organization for this workshop:')
            for i,row in enumerate(rows,1): print(f"{i}. {row.get('name',row['id'])} ({row['id']})")
            try:
                selected=int(choose('Organization number: '))
                if not 1<=selected<=len(rows): raise ValueError()
                organization=rows[selected-1]['id']
            except (ValueError,EOFError):
                raise WorkshopError('organization_selection_invalid','Rerun setup and select one of the listed organization numbers.') from None
    cidr=settings.learner_cidr or str(ipaddress.IPv4Address(detect_ip()))+'/32'
    return Settings(**{**settings.model_dump(),'cloud_org_id':organization,'learner_cidr':cidr})


class CloudApi:
    def __init__(self,organization_id,*,transport=None):
        self.organization_id=str(UUID(str(organization_id)))
        path=Path.cwd()/'.clickhouse/credentials.json'
        try:
            stored=json.loads(path.read_text()) if path.exists() else {}
            env=dotenv_values(Path.cwd()/'.env',interpolate=False)
            if stored.get('api_key') and stored.get('api_secret'):
                key,secret=stored['api_key'],stored['api_secret']
            else:
                key=os.environ.get('CLICKHOUSE_CLOUD_API_KEY') or env.get('CLICKHOUSE_CLOUD_API_KEY')
                secret=os.environ.get('CLICKHOUSE_CLOUD_API_SECRET') or env.get('CLICKHOUSE_CLOUD_API_SECRET')
        except (OSError,ValueError):
            raise WorkshopError('cloud_credentials_invalid','Check the private CLI credentials file. Its contents are omitted.') from None
        if not key or not secret:
            raise WorkshopError('cloud_api_credentials_missing','Log in with an organization API key and secret from the lab directory. OAuth cannot create CDC.')
        self.client=httpx.Client(base_url='https://api.clickhouse.cloud/v1/',auth=(key,secret),timeout=60,follow_redirects=False,transport=transport)

    def __enter__(self): return self
    def __exit__(self,*args): self.client.close()

    def create_pipe(self,service_id,body):
        try:
            response=self.client.post(f'organizations/{self.organization_id}/services/{UUID(str(service_id))}/clickpipes',json=body)
            response.raise_for_status()
            result=response.json().get('result')
            if not isinstance(result,dict) or not result.get('id'): raise ValueError('Missing ID')
            return result
        except (httpx.HTTPError,ValueError,TypeError):
            raise WorkshopError('cloud_api_failed','CDC creation did not return a usable acknowledgement. Inspect the owned service and pending inventory before retrying; raw responses are omitted.') from None


def refreshed(settings):
    values=dotenv_values(settings.private_env_path,interpolate=False)
    fields={name:values['PRICING_'+name.upper()] for name in Settings.model_fields if values.get('PRICING_'+name.upper()) is not None}
    return Settings(**{**settings.model_dump(),**fields})


def initialize_schema(settings):
    analytics=AnalyticsClient(settings)
    try:
        analytics.bootstrap_database()
        analytics.command('CREATE DATABASE IF NOT EXISTS '+identifier(settings.clickstack_database))
    finally: analytics.close()
    ledger=Ledger(settings.administrator_dsn());ledger.migrate()
    try:
        ledger.current_scope(settings.owner_id,settings.scenario_id)
    except WorkshopError as error:
        if error.code!='scenario_missing': raise
        fixture=json.loads((LAB_ROOT/'fixtures/checkpoints/lab1_baseline.json').read_text())
        ledger.initialize(Scope(owner_id=settings.owner_id,scenario_id=settings.scenario_id,generation=1),Subject.model_validate(fixture['subject']),fixture['state'],fixture['rules'],datetime.fromisoformat(fixture['scenario_cutoff']))
    return ledger


def verify_admin_tls(settings):
    parts=urlsplit(settings.administrator_dsn());query=parse_qs(parts.query)
    ca=query.get('sslrootcert',[])
    if (parts.scheme not in ('postgres','postgresql') or query.get('sslmode')!=['verify-full']
        or len(ca)!=1 or Path(ca[0]).resolve()!=settings.pg_ca_path.resolve()
        or not settings.pg_ca_path.is_file()):
        raise WorkshopError('source_tls_required','Use sslmode=verify-full and the downloaded owned-instance CA in PRICING_POSTGRES_ADMIN_DSN before setup.')


def verify_pipe_source(row,settings):
    source=(row.get('source') or {}).get('postgres') or {}
    parts=urlsplit(settings.administrator_dsn());options=source.get('settings') or {}
    mappings=source.get('tableMappings') or []
    actual={(r.get('sourceSchemaName'),r.get('sourceTable'),r.get('targetTable'),r.get('tableEngine')) for r in mappings}
    expected={('pricing',name,name+'_cdc','ReplacingMergeTree') for name in TABLES}
    # API GET deliberately omits source credentials. Validate username if returned;
    # newly created credentials are fixed to pricing_cdc in the POST request.
    username=(source.get('credentials') or {}).get('username')
    if (source.get('host')!=parts.hostname or source.get('port')!=(parts.port or 5432)
        or source.get('database')!=parts.path.lstrip('/') or source.get('authentication')!='basic'
        or source.get('disableTls') is not False or source.get('skipCertVerification') is not False
        or (source.get('caCertificate') or '').strip()!=settings.pg_ca_path.read_text().strip()
        or options.get('publicationName')!='pricing_workshop' or options.get('replicationMode')!='cdc'
        or options.get('allowNullableColumns') is not True
        or len(mappings)!=4 or actual!=expected or username not in (None,'pricing_cdc')):
        raise WorkshopError('clickpipe_source_mismatch','Use the owned Postgres source, verified TLS, pricing_workshop publication, preserved NULLs, and exactly the four pricing mappings. Inspect the saved pipe before changing source roles.')


def verify_role_attributes(conn):
    roles=conn.execute("SELECT rolname,rolsuper,rolcreatedb,rolcreaterole,rolbypassrls,rolreplication FROM pg_roles WHERE rolname IN ('pricing_app','pricing_cdc')").fetchall()
    memberships=conn.execute("SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member WHERE r.rolname IN ('pricing_app','pricing_cdc') LIMIT 1").fetchone()
    if memberships or any(row['rolsuper'] or row['rolcreatedb'] or row['rolcreaterole'] or row['rolbypassrls'] or (row['rolname']=='pricing_app' and row['rolreplication']) for row in roles):
        raise WorkshopError('source_role_privileges','Use dedicated pricing roles without administrative memberships or elevated attributes; pricing_app must not have REPLICATION. Correct existing role privileges before rerunning setup.')


def configure_roles(settings,ledger,*,existing_pipe=False):
    values=dotenv_values(settings.private_env_path,interpolate=False)
    if existing_pipe and not values.get('PRICING_POSTGRES_CDC_PASSWORD'):
        raise WorkshopError('existing_cdc_credentials_required','Save the existing pricing_cdc password as PRICING_POSTGRES_CDC_PASSWORD before adopting this ClickPipe. Its role was not changed.')
    passwords={name:values.get('PRICING_POSTGRES_'+name.upper()+'_PASSWORD') or secrets.token_urlsafe(32) for name in ('app','cdc')}
    # Persist before SQL, so a restart reuses the same credentials after an unknown commit.
    save_private(settings,{'PRICING_POSTGRES_'+name.upper()+'_PASSWORD':value for name,value in passwords.items()})
    with ledger.connect() as conn:
        for name,password in passwords.items():
            role='pricing_'+name
            if not conn.execute('SELECT 1 FROM pg_roles WHERE rolname=%s',(role,)).fetchone():
                conn.execute(sql.SQL('CREATE ROLE {} LOGIN').format(sql.Identifier(role)))
        verify_role_attributes(conn)
        for name,password in passwords.items():
            conn.execute(sql.SQL('ALTER ROLE {} WITH LOGIN PASSWORD {}').format(sql.Identifier('pricing_'+name),sql.Literal(password)))
    configure_source(ledger)
    with ledger.connect() as conn:
        verify_role_attributes(conn)
        for name in TABLES:
            table='pricing.'+name
            invalid=conn.execute("SELECT has_table_privilege('pricing_app',%s,'DELETE, TRUNCATE, REFERENCES, TRIGGER') OR has_table_privilege('pricing_cdc',%s,'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER') AS invalid",(table,table)).fetchone()['invalid']
            if invalid:
                raise WorkshopError('source_role_privileges','Remove excess or inherited app/CDC table privileges before rerunning setup. The app needs SELECT/INSERT/UPDATE; CDC needs SELECT only.')
    parts=urlsplit(settings.administrator_dsn())
    # Only user/password change; the original endpoint, database, and verified TLS remain.
    netloc=quote('pricing_app',safe='')+':'+quote(passwords['app'],safe='')+'@'+parts.netloc.rsplit('@',1)[-1]
    app_dsn=urlunsplit(parts._replace(netloc=netloc))
    save_private(settings,{'PRICING_POSTGRES_DSN':app_dsn})
    return app_dsn


def pipe_request(settings,password):
    source=urlsplit(settings.administrator_dsn())
    return {'name':'pricing-'+settings.owner_id+'-cdc','destination':{'database':settings.clickhouse_database},'source':{'postgres':{
        'type':'postgres','host':source.hostname,'port':source.port or 5432,'database':source.path.lstrip('/'),
        'authentication':'basic','credentials':{'username':'pricing_cdc','password':password},
        'disableTls':False,'skipCertVerification':False,'caCertificate':settings.pg_ca_path.read_text(),
        'settings':{'replicationMode':'cdc','publicationName':'pricing_workshop','syncIntervalSeconds':10,
                    'allowNullableColumns':True,'deleteOnMerge':False,'enableFailoverSlots':False},
        'tableMappings':[{'sourceSchemaName':'pricing','sourceTable':name,'targetTable':name+'_cdc',
                          'tableEngine':'ReplacingMergeTree','excludedColumns':[],'sortingKeys':[],
                          'useCustomSortingKey':False,'partitionByExpr':'','partitionKey':''} for name in TABLES]}}}


def ensure_pipe(inventory,settings,api,body):
    if inventory.pending_creation:
        raise WorkshopError('creation_ack_unknown','Reconcile the pending owned resource in the console before rerunning setup. Do not create another resource.')
    if inventory.clickpipe_id: return inventory
    pending=inventory.model_copy(update={'pending_creation':'clickpipe'});save_inventory(settings,pending)
    created=api.create_pipe(inventory.service_id,body)
    inventory=pending.model_copy(update={'clickpipe_id':UUID(created['id']),'pending_creation':None})
    save_inventory(settings,inventory)
    return inventory


def wait_pipe(inventory,settings,runner,*,sleep=time.sleep,progress=report_progress):
    # Initial snapshot + schema creation may take minutes, independently of CDC cadence.
    previous=None
    for attempt in range(300):
        row=runner.run(command(settings,'clickpipe','get',inventory.service_id,inventory.clickpipe_id))
        if str(row.get('id'))!=str(inventory.clickpipe_id):
            raise WorkshopError('clickpipe_id_mismatch','Verify the saved pipe ID on the owned service.')
        state=str(row.get('state','')).lower()
        if state!=previous or attempt%10==0:
            progress(f'ClickPipe {inventory.clickpipe_id}: {state or "pending"} (readiness check {attempt+1}/300)')
        previous=state
        if state in ('failed','error','deleted'):
            raise WorkshopError('clickpipe_failed','Inspect the owned pipe, source network access, and replication privileges. Its ID remains saved.')
        if state=='running':
            verify_pipe_source(row,settings)
            try:
                verify_clickpipe(row,settings)
                progress('ClickPipe is running and all four destination schemas are ready.')
                return
            except WorkshopError as error:
                if error.code!='cdc_schema_missing': raise
        sleep(2)
    raise WorkshopError('clickpipe_pending','CDC is still initializing. Rerun the same setup command to resume the saved pipe; do not create another pipe.')


def setup_cloud(settings,runner,api,*,sleep=time.sleep,progress=report_progress):
    path=settings.resource_inventory_path.with_suffix('.lock');path.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(path,os.O_CREAT|os.O_RDWR,0o600)
    try:
        try: fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            raise WorkshopError('provision_in_progress','Another setup command is active for this inventory. Wait for it to finish.') from None
        progress('Creating or verifying your ClickHouse and managed Postgres services...')
        inventory=_provision(runner,settings,'setup',sleep=sleep,progress=progress)
        settings=refreshed(settings)
        source=runner.run(command(settings,'postgres','get',inventory.postgres_id))
        parts=urlsplit(settings.administrator_dsn())
        if parts.hostname!=source.get('hostname') or parts.path.lstrip('/')!=inventory.postgres_database:
            raise WorkshopError('source_endpoint_mismatch','Match the administrator DSN to the owned Postgres inventory before initialization.')
        if inventory.clickpipe_id:
            pipe=runner.run(command(settings,'clickpipe','get',inventory.service_id,inventory.clickpipe_id))
            if str(pipe.get('id'))!=str(inventory.clickpipe_id):
                raise WorkshopError('clickpipe_id_mismatch','Verify the saved pipe ID on the owned service.')
            verify_pipe_name(pipe,settings.owner_id)
            verify_pipe_source(pipe,settings)
        verify_admin_tls(settings)
        progress('Initializing analytical databases, source schema, roles, and publication...')
        ledger=initialize_schema(settings);configure_roles(settings,ledger,existing_pipe=bool(inventory.clickpipe_id))
        settings=refreshed(settings)
        cdc_password=dotenv_values(settings.private_env_path,interpolate=False)['PRICING_POSTGRES_CDC_PASSWORD']
        progress('Creating or verifying your four-table Postgres ClickPipe...')
        inventory=ensure_pipe(inventory,settings,api,pipe_request(settings,cdc_password))
        wait_pipe(inventory,settings,runner,sleep=sleep,progress=progress)
        progress('Creating and verifying canonical ClickHouse views...')
        analytics=AnalyticsClient(settings)
        try:
            _,cutoff=ledger.current_scope(settings.owner_id,settings.scenario_id)
            FeatureService(analytics,ledger,Clock(cutoff)).configure_views()
        finally: analytics.close()
        progress('Setup ready: pricing and telemetry databases, managed Postgres, app role, and CDC. Start the local application next.')
        return inventory
    finally: os.close(fd)


def main():
    try:
        report_progress('Starting workshop Cloud setup...')
        settings=installation_settings()
        runner=CliRunner()
        report_progress('Detecting your Cloud organization and public IPv4 address...')
        settings=cloud_settings(settings,runner)
        report_progress(f'Using organization {settings.cloud_org_id}, region {settings.region}, access {settings.learner_cidr}.')
        with CloudApi(settings.cloud_org_id) as api:
            setup_cloud(settings,runner,api)
        return 0
    except WorkshopError as error: print(str(error));return 2
    except Exception:
        print('setup_failed: Check your private configuration and saved inventory. Credentials and raw errors are omitted. Rerun the same command after correcting the failed step.');return 2

if __name__=='__main__': raise SystemExit(main())
