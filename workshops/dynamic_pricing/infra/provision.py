"""Private credentials and durable resource IDs; CLI output is never echoed."""
import argparse
import fcntl
from datetime import UTC,datetime
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
from typing import Literal
from urllib.parse import parse_qsl,quote,urlsplit,urlunsplit,urlencode
from uuid import UUID,uuid4
from pydantic import BaseModel,ConfigDict,Field,field_validator
from dotenv import dotenv_values
from pricing_lab.contracts import WorkshopError
from pricing_lab.settings import Settings


class ResourceInventory(BaseModel):
    model_config=ConfigDict(extra='forbid',frozen=True)
    owner_id:str=Field(pattern=r'^[a-z0-9][a-z0-9_-]{0,63}$')
    organization_id:UUID
    region:str
    created_at:datetime
    service_id:UUID|None=None
    postgres_id:UUID|None=None
    clickpipe_id:UUID|None=None
    pricing_database:str='pricing'
    postgres_database:str='postgres'
    deleted_resources: dict[str,str] = Field(default_factory=dict)
    pending_creation:Literal['service','postgres','clickpipe']|None=None
    @field_validator('created_at')
    @classmethod
    def aware(cls,value):
        if value.tzinfo is None: raise ValueError('Record an aware UTC creation time.')
        return value.astimezone(UTC)


class CliRunner:
    def run(self,argv):
        # shell=False; no credentials, debug flags, or SQL are placed in argv.
        try:
            result=subprocess.run(argv,check=False,capture_output=True,text=True,timeout=900 if argv[2:4]==["service","delete"] else 60)
            if result.returncode: raise WorkshopError('cloud_command_failed','Check Cloud authentication, organization access, and region availability. Raw CLI output is omitted.')
            return json.loads(result.stdout) if result.stdout.strip() else {}
        except (OSError,subprocess.TimeoutExpired,json.JSONDecodeError):
            raise WorkshopError('cloud_command_failed','The Cloud command did not return a usable result. Check its state in the console before retrying creation.') from None


def atomic_write(path,text,mode=0o600):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    fd,name=tempfile.mkstemp(prefix='.'+path.name,dir=path.parent)
    try:
        os.fchmod(fd,mode)
        with os.fdopen(fd,'w') as stream:
            stream.write(text);stream.flush();os.fsync(stream.fileno())
        os.replace(name,path)
    finally:
        if os.path.exists(name): os.unlink(name)


def save_private(settings,updates):
    values=dict(dotenv_values(settings.private_env_path,interpolate=False)) if settings.private_env_path.exists() else {}
    values.update(updates)
    def quoted(value):
        value=str(value)
        if '\n' in value or '\r' in value: raise ValueError('Environment values must fit on one line.')
        return "'"+value.replace("'","\\'")+"'"
    atomic_write(settings.private_env_path,'\n'.join(f'{k}={quoted(v)}' for k,v in values.items() if v is not None)+'\n')


def save_inventory(settings,inventory):
    atomic_write(settings.resource_inventory_path,inventory.model_dump_json(indent=2)+'\n')


def command(settings,kind,action,*args):
    return ['clickhousectl','cloud',kind,action,*map(str,args),'--org-id',str(settings.cloud_org_id),'--json']


def check_inventory(inventory,settings):
    from .cleanup import OwnerMismatch
    if inventory.owner_id!=settings.owner_id or str(inventory.organization_id)!=str(settings.cloud_org_id):
        raise OwnerMismatch()
    if inventory.pricing_database!=settings.clickhouse_database or inventory.region!=settings.region:
        raise WorkshopError('inventory_configuration_changed','Use the configuration recorded for these resources or create a separate installation.')


def owned(row,owner,kind):
    tags={t.get('key'):t.get('value') for t in row.get('tags',[]) if isinstance(t,dict)}
    return tags.get('workshop')=='dynamic-pricing' and tags.get('owner')==owner


def verify_pipe_name(row,owner):
    expected='pricing-'+owner+'-cdc'
    if row.get('name')!=expected:
        raise WorkshopError('clickpipe_name_mismatch',f'Use the owned ClickPipe named {expected}. Verify its ID in the Cloud console before continuing.')


def verify_clickpipe(row,settings):
    verify_pipe_name(row,settings.owner_id)
    if str(row.get('state','')).lower()!='running':
        raise WorkshopError('clickpipe_pending','Inspect the saved ClickPipe state and wait for its initial snapshot.')
    if (row.get('destination') or {}).get('database')!=settings.clickhouse_database:
        raise WorkshopError('clickpipe_destination_mismatch',f'Select destination database {settings.clickhouse_database} in the owned ClickPipe console configuration.')
    source=(row.get('source') or {}).get('postgres') or {}
    interval=(source.get('settings') or {}).get('syncIntervalSeconds')
    if not isinstance(interval,(int,float)) or isinstance(interval,bool) or not 0 < interval <= settings.convergence_timeout_seconds:
        raise WorkshopError('clickpipe_sync_interval','Set the ClickPipe sync interval to at most 20 seconds, or raise PRICING_CONVERGENCE_TIMEOUT_SECONDS to cover it (maximum 120). Rerun readiness after the change.')
    from pricing_lab.analytics import AnalyticsClient
    from pricing_lab.features import verify_cdc_schema
    # Provisioning has just saved connection settings; reload the private file.
    values=dotenv_values(settings.private_env_path,interpolate=False)
    connection={name:values['PRICING_'+name.upper()] for name in ('clickhouse_host','clickhouse_port','clickhouse_secure','clickhouse_database','clickhouse_username','clickhouse_password') if values.get('PRICING_'+name.upper()) is not None}
    analytics=AnalyticsClient(Settings(**{**settings.model_dump(),**connection}))
    try:
        verify_cdc_schema(analytics)
    finally:
        analytics.close()


def report_progress(message):
    print(message,flush=True)


def wait_running(runner,settings,kind,resource_id,*,sleep=time.sleep,progress=report_progress):
    previous=None
    for attempt in range(300):
        row=runner.run(command(settings,kind,'get',resource_id))
        if not owned(row,settings.owner_id,kind):
            from .cleanup import OwnerMismatch
            raise OwnerMismatch()
        state=str(row.get('state','')).lower()
        if state!=previous or attempt%10==0:
            progress(f'{kind} {resource_id}: {state or "pending"} (readiness check {attempt+1}/300)')
        previous=state
        if state=='running' or (kind=='service' and state=='idle'): return row
        if state in ('failed','deleted','terminated'): raise WorkshopError('cloud_service_failed','Inspect this owned service in the Cloud console. Its ID and creation credentials remain saved.')
        sleep(2)
    raise WorkshopError('cloud_service_pending','The service is still provisioning. Retry readiness for the saved ID; do not create another service.')


def verified_dsn(value,ca_path):
    parts=urlsplit(value)
    if parts.scheme not in ('postgres','postgresql') or not parts.hostname or not parts.password:
        raise WorkshopError('credentials_missing','Save the connection string returned at creation or explicitly recover this owned service password.')
    query=dict(parse_qsl(parts.query));query.update(sslmode='verify-full',sslrootcert=str(ca_path.resolve()))
    return urlunsplit(parts._replace(query=urlencode(query)))


def provision(runner,settings,module,*,sleep=time.sleep):
    path=settings.resource_inventory_path.with_suffix('.lock')
    path.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(path,os.O_CREAT|os.O_RDWR,0o600)
    try:
        try: fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError: raise WorkshopError('provision_in_progress','Another setup command is active for this inventory. Wait for it to finish.') from None
        return _provision(runner,settings,module,sleep=sleep)
    finally: os.close(fd)


def _provision(runner,settings,module,*,sleep=time.sleep,progress=report_progress):
    if module not in ('setup','cdc','diagnostics'): raise ValueError('Choose setup, cdc, or diagnostics.')
    if not settings.cloud_org_id or not settings.learner_cidr: raise WorkshopError('cloud_configuration_missing','Set an explicit organization ID, region, and learner IPv4 /32 CIDR before provisioning.')
    if settings.resource_inventory_path.exists():
        inventory=ResourceInventory.model_validate_json(settings.resource_inventory_path.read_text())
        check_inventory(inventory,settings)
    else:
        inventory=ResourceInventory(owner_id=settings.owner_id,organization_id=settings.cloud_org_id,region=settings.region,created_at=datetime.now(UTC),pricing_database=settings.clickhouse_database)
        save_inventory(settings,inventory)
    if inventory.deleted_resources:
        raise WorkshopError('inventory_deleted','Use a separate inventory for a new installation after cleanup.')
    if inventory.pending_creation:
        raise WorkshopError('creation_ack_unknown','Inspect the owned resource in the console and record its ID in the inventory. Do not retry an ambiguous create operation.')
    private=dict(dotenv_values(settings.private_env_path,interpolate=False)) if settings.private_env_path.exists() else {}
    for kind,field in (('service','service_id'),('postgres','postgres_id')):
        resource_id=getattr(inventory,field)
        if resource_id is None:
            progress(f'Creating {kind} in {settings.region}'+(' with 1 replica...' if kind=='service' else '...'))
            inventory=inventory.model_copy(update={'pending_creation':kind});save_inventory(settings,inventory)
            args=['--name',f'pricing-{settings.owner_id}'+('-source' if kind=='postgres' else ''),'--provider','aws','--region',settings.region,'--tag','workshop=dynamic-pricing','--tag','owner='+settings.owner_id]
            if kind=='service': args+=['--ip-allow',settings.learner_cidr,'--num-replicas','1']
            else: args+=['--size',settings.postgres_size,'--pg-version','17','--ha-type','none']
            created=runner.run(command(settings,kind,'create',*args))
            row=created.get('service',{}) if kind=='service' else created
            resource_id=UUID(row['id'])
            inventory=inventory.model_copy(update={field:resource_id,'pending_creation':None});save_inventory(settings,inventory)
            if kind=='service':
                if not created.get('password'): raise WorkshopError('credentials_missing','Explicitly recover this owned service password; do not create another service.')
                save_private(settings,{'PRICING_CLICKHOUSE_PASSWORD':created['password']})
            else:
                dsn=verified_dsn(created.get('connectionString',''),settings.pg_ca_path)
                save_private(settings,{'PRICING_POSTGRES_ADMIN_DSN':dsn,'PRICING_POSTGRES_DSN':dsn})
            private=dict(dotenv_values(settings.private_env_path,interpolate=False))
            progress(f'{kind} created: {resource_id}. Credentials saved privately.')
        else:
            progress(f'Reusing saved {kind}: {resource_id}.')
        required='PRICING_CLICKHOUSE_PASSWORD' if kind=='service' else 'PRICING_POSTGRES_ADMIN_DSN'
        if not private.get(required): raise WorkshopError('credentials_missing','Explicitly recover the saved service password. Service get does not return creation credentials.')
        row=wait_running(runner,settings,kind,resource_id,sleep=sleep,progress=progress)
        if kind=='service':
            endpoint=next((e for e in row.get('endpoints',[]) if e.get('protocol')=='https'),None)
            if not endpoint: raise WorkshopError('endpoint_pending','Wait for the HTTPS endpoint of this saved service.')
            save_private(settings,{'PRICING_CLICKHOUSE_HOST':endpoint['host'],'PRICING_CLICKHOUSE_PORT':endpoint['port'],'PRICING_CLICKHOUSE_SECURE':'true','PRICING_CLICKHOUSE_DATABASE':settings.clickhouse_database,'PRICING_OWNER_ID':settings.owner_id,'PRICING_SESSION_SECRET':private.get('PRICING_SESSION_SECRET') or secrets.token_urlsafe(48)})
        else:
            progress('Downloading the Managed Postgres CA certificate...')
            settings.pg_ca_path.parent.mkdir(parents=True,exist_ok=True)
            runner.run(command(settings,'postgres','certs','get',resource_id,'--output',settings.pg_ca_path))
            if not settings.pg_ca_path.is_file() or not settings.pg_ca_path.stat().st_size: raise WorkshopError('certificate_missing','Download the saved Postgres instance CA before starting the local application.')
            settings.pg_ca_path.chmod(0o644) # Public CA bundle must be readable by the non-root container.
            save_private(settings,{'PRICING_PG_CA_PATH':str(settings.pg_ca_path.resolve())})
    if module=='cdc':
        if not inventory.clickpipe_id: raise WorkshopError('clickpipe_configuration_required','Create the owned Postgres ClickPipe in the console with destination database pricing, record its ID, then rerun cdc readiness.')
        row=runner.run(command(settings,'clickpipe','get',inventory.service_id,inventory.clickpipe_id))
        if str(row.get('id'))!=str(inventory.clickpipe_id): raise WorkshopError('clickpipe_id_mismatch','Verify the saved ClickPipe ID in the owned service.')
        verify_clickpipe(row,settings)
    return inventory


def recover_password(inventory,runner,settings,kind):
    check_inventory(inventory,settings)
    if kind not in ('service','postgres'): raise ValueError('Choose service or postgres.')
    resource_id=inventory.service_id if kind=='service' else inventory.postgres_id
    if resource_id is None: raise WorkshopError('resource_missing','Record the owned resource ID before password recovery.')
    row=runner.run(command(settings,kind,'get',resource_id))
    if not owned(row,settings.owner_id,kind):
        from .cleanup import OwnerMismatch
        raise OwnerMismatch()
    result=runner.run(command(settings,kind,'reset-password',resource_id,*(['--generate'] if kind=='postgres' else [])))
    password=result.get('password')
    if not password: raise WorkshopError('credentials_missing','The reset returned no password. Check the owned service in the console.')
    if kind=='service': save_private(settings,{'PRICING_CLICKHOUSE_PASSWORD':password})
    else:
        dsn='postgresql://'+quote(row.get('username','postgres'),safe='')+':'+quote(password,safe='')+'@'+row['hostname']+':5432/'+inventory.postgres_database
        save_private(settings,{'PRICING_POSTGRES_ADMIN_DSN':verified_dsn(dsn,settings.pg_ca_path)})


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--module',choices=['setup','cdc','diagnostics'],default='setup');parser.add_argument('--recover-password',choices=['service','postgres'])
    args=parser.parse_args();settings=Settings.from_env();runner=CliRunner()
    try:
        if args.recover_password:
            inventory=ResourceInventory.model_validate_json(settings.resource_inventory_path.read_text());recover_password(inventory,runner,settings,args.recover_password)
            print('Owned service credentials saved privately. Restart the local application.');return 0
        inventory=provision(runner,settings,args.module);print(inventory.model_dump_json(indent=2));return 0
    except WorkshopError as error: print(str(error));return 2
    except Exception: print('provision_failed: Check the private configuration and saved inventory. Raw errors are omitted.');return 2

if __name__=='__main__': raise SystemExit(main())
