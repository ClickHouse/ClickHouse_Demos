"""Export scoped persisted facts. Projections and receipts remain distinct."""
import json
from pathlib import Path
from .contracts import Candidate,Receipt,WorkshopError
from .ledger import Ledger,lock_scope,scope_key,SCOPE_WHERE
from .settings import LAB_ROOT,Settings

EXPORT_ROOT=LAB_ROOT/'reports/exports'


def export_report(scope,decision_id,destination:Path,ledger=None):
    destination=Path(destination)
    if '..' in destination.parts or destination.suffix!='.json' or destination.resolve().parent!=EXPORT_ROOT.resolve():
        raise WorkshopError('report_path_invalid','Export a JSON filename inside reports/exports.')
    ledger=ledger or Ledger(Settings.from_env().postgres_dsn.get_secret_value())
    with ledger.connect() as conn:
        lock_scope(conn,scope)
        decision=conn.execute(f'SELECT * FROM pricing.decisions WHERE {SCOPE_WHERE} AND decision_id=%s',(*scope_key(scope),decision_id)).fetchone()
        if not decision: raise WorkshopError('decision_missing','Choose a reviewed decision from this scenario generation.')
        row=conn.execute(f'SELECT data FROM pricing.candidates WHERE {SCOPE_WHERE} AND candidate_id=%s',(*scope_key(scope),decision['candidate_id'])).fetchone()
        candidate=Candidate.model_validate(row['data'])
        receipt=conn.execute(f'SELECT data FROM pricing.receipts WHERE {SCOPE_WHERE} AND decision_id=%s',(*scope_key(scope),decision_id)).fetchone()
        attempts=conn.execute('SELECT status,attempted_at FROM pricing.application_attempts WHERE candidate_id=%s ORDER BY attempted_at DESC LIMIT 20',(candidate.candidate_id,)).fetchall()
        alternative_rows=conn.execute(f"SELECT data FROM pricing.candidates WHERE {SCOPE_WHERE} AND data->'snapshot'->>'snapshot_id'=%s LIMIT 20",(*scope_key(scope),str(candidate.snapshot.snapshot_id))).fetchall()
    report={'decision_id':str(decision_id),'mode':'mock','simulation_label':'projected',
            'source_label':'synthetic transactions; replication mode is recorded in the ingestion evidence',
            'scope':scope.model_dump(mode='json'),'candidate':candidate.model_dump(mode='json'),
            'alternatives':[Candidate.model_validate(r['data']).model_dump(mode='json') for r in alternative_rows],
            'objective':'receipts minus incremental selling and terminal disposal costs; procurement held constant',
            'reviewer_id':decision['reviewer_id'],'reviewed_at':decision['reviewed_at'].isoformat(),
            'review':decision['data'],'review_status':decision['status'],
            'application_status':'applied' if receipt else attempts[0]['status'] if attempts else 'not_requested',
            'receipt':Receipt.model_validate(receipt['data']).model_dump(mode='json') if receipt else None,
            'pilot_hypothesis':'Test whether fresher sales and stock inputs change feasible price choices. Synthetic projections do not establish business uplift.'}
    destination.parent.mkdir(parents=True,exist_ok=True)
    destination.write_text(json.dumps(report,indent=2,sort_keys=True)+'\n')
    p=candidate.projection
    # No arbitrary reviewer prose becomes Markdown control text or numerical evidence.
    markdown=f'''# Mock pricing decision

Decision: `{decision_id}`

Scope: `{scope.sandbox_id}`, generation {scope.generation}
Review: {decision['status']} by `{decision['reviewer_id']}` at {decision['reviewed_at'].isoformat()}
Application: {report['application_status']}

## Price and projected economics

Current price: {candidate.snapshot.current_price_minor} minor units. Proposed current-window price: {candidate.price_minor} minor units.

Net recovery: {p.get('net_recovery_minor','not scored')} minor units. Expired stock: {p.get('expired_units','not scored')} units.

These amounts are projections from synthetic inputs. The second-window plan is simulation evidence. Only an acknowledged receipt establishes an applied mock price.

## Acknowledged receipt

```json
{json.dumps(report['receipt'],indent=2)}
```

## Pilot hypothesis

{report['pilot_hypothesis']}
'''
    destination.with_suffix('.md').write_text(markdown)
    return destination
