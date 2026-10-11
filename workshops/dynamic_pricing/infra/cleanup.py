"""Preview owned IDs, verify all remaining resources, and journal each deletion."""
import argparse
from datetime import UTC, datetime
from pricing_lab.contracts import WorkshopError
from pricing_lab.settings import Settings
from .provision import ResourceInventory,CliRunner,check_inventory,command,owned,save_inventory,verify_pipe_name

class OwnerMismatch(WorkshopError):
    def __init__(self): super().__init__('owner_mismatch','Use the matching owner and organization. Resource ownership could not be verified; no further resources were deleted.')


def resource_row(inventory,runner,settings,kind,value):
    args=[inventory.service_id,value] if kind=='clickpipe' else [value]
    try:
        return runner.run(command(settings,kind,'get',*args))
    except WorkshopError:
        # Never interpret authentication errors or timeouts as proof of absence.
        # A successful full inventory response must independently prove it.
        listing=runner.run(command(settings,kind,'list',*([inventory.service_id] if kind=='clickpipe' else [])))
        rows=listing if isinstance(listing,list) else None
        if rows is None or any(not isinstance(row,dict) or 'id' not in row for row in rows):
            raise WorkshopError('cleanup_inventory_unusable','Check the Cloud resource inventory in the console before retrying cleanup.')
        if any(str(row['id'])==str(value) for row in rows):
            raise WorkshopError('cleanup_resource_unverified','The resource still exists but could not be verified. Check access and retry cleanup.')
        return None


def cleanup(inventory,runner,settings,execute=False):
    check_inventory(inventory,settings)
    plan=[]
    for kind,value in (('clickpipe',inventory.clickpipe_id),('postgres',inventory.postgres_id),('service',inventory.service_id)):
        if value and kind not in inventory.deleted_resources:
            plan.append({'kind':kind,'id':str(value),'organization_id':str(inventory.organization_id),'owner_id':inventory.owner_id})
    if not execute: return plan
    present={}
    for item in plan:
        kind=item['kind'];row=resource_row(inventory,runner,settings,kind,item['id'])
        present[kind]=row is not None
        if row is None: continue
        if str(row.get('id'))!=item['id']: raise OwnerMismatch()
        if kind=='clickpipe':
            verify_pipe_name(row,inventory.owner_id)
        elif not owned(row,inventory.owner_id,kind): raise OwnerMismatch()
    for item in plan:
        kind=item['kind']
        if present[kind]:
            args=[inventory.service_id,item['id']] if kind=='clickpipe' else [item['id']]
            if kind=='service': args+=['--force']
            runner.run(command(settings,kind,'delete',*args))
        inventory=inventory.model_copy(update={'deleted_resources':{**inventory.deleted_resources,kind:datetime.now(UTC).isoformat()}})
        save_inventory(settings,inventory)
    return plan


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    settings=Settings.from_env()
    try:
        inventory=ResourceInventory.model_validate_json(settings.resource_inventory_path.read_text())
        import json
        print(json.dumps({'execute':args.execute,'resources':cleanup(inventory,CliRunner(),settings,args.execute)},indent=2));return 0
    except WorkshopError as error: print(str(error));return 2
    except Exception: print('cleanup_failed: Check the saved inventory. Raw Cloud errors are omitted.');return 2

if __name__=='__main__': raise SystemExit(main())
