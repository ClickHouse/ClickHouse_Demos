#!/usr/bin/env python3
"""Record actual dedicated Cloud checks; never emit credential-bearing exceptions."""
import json
import os
from pathlib import Path
from datetime import datetime,UTC
from pricing_lab.settings import Settings,LAB_ROOT
from infra.provision import ResourceInventory
from infra.rehearsal import rehearse_cloud

def main():
    output=LAB_ROOT/'reports/exports'/('cloud-rehearsal-'+datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')+'.json')
    output.parent.mkdir(parents=True,exist_ok=True)
    try:
        settings=Settings.from_env()
        inventory=ResourceInventory.model_validate_json(settings.resource_inventory_path.read_text())
        report=rehearse_cloud(settings,inventory,admin_dsn=os.environ.get('PRICING_REHEARSAL_ADMIN_DSN',''))
    except Exception as error:
        report={'status':'failed','cloud_verified':False,'error_code':getattr(error,'code',type(error).__name__),
                'action':'Inspect dedicated ownership, live Cloud dependencies, and learner API readiness. Raw errors are omitted.'}
    output.write_text(json.dumps(report,default=str,indent=2)+'\n');output.chmod(0o600)
    print(json.dumps({'status':report['status'],'report_path':str(output)}))
    return 0 if report['status']=='automated_cloud_checks_passed' else 2

if __name__=='__main__': raise SystemExit(main())
