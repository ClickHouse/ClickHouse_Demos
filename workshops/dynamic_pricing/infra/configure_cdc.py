"""Create a limited publication after the learner creates the named roles."""
from pricing_lab.settings import Settings,LAB_ROOT
from pricing_lab.ledger import Ledger
from pricing_lab.contracts import WorkshopError

def configure_source(ledger, retail=False):
    from pricing_lab.retail_cdc import RETAIL_TABLES
    names=RETAIL_TABLES if retail else ("sales_transactions","inventory_history","price_history","cost_history")
    publication="pricing_retail" if retail else "pricing_workshop"
    with ledger.connect() as conn:
        roles={row['rolname'] for row in conn.execute("SELECT rolname FROM pg_roles WHERE rolname IN ('pricing_app','pricing_cdc')").fetchall()}
        if roles!={'pricing_app','pricing_cdc'}:
            raise WorkshopError('source_roles_missing','Create pricing_app and pricing_cdc login roles in your own Postgres instance, then rerun the source grants.')
        conn.execute((LAB_ROOT/'sql/postgres/002_roles.sql').read_text())
        conn.execute('ALTER ROLE pricing_cdc REPLICATION')
        for name in names:
            conn.execute(f'GRANT SELECT ON pricing.{name} TO pricing_cdc')
        exists=conn.execute("SELECT 1 FROM pg_publication WHERE pubname=%s",(publication,)).fetchone()
        if not exists:
            conn.execute(f'CREATE PUBLICATION {publication} FOR TABLE ' + ','.join('pricing.'+name for name in names))
        tables=conn.execute("SELECT schemaname,tablename FROM pg_publication_tables WHERE pubname=%s",(publication,)).fetchall()
        expected={('pricing',name) for name in names}
        if {(row['schemaname'],row['tablename']) for row in tables}!=expected:
            raise WorkshopError('publication_mismatch','The saved publication must contain exactly the mapped source tables. Existing publications are not replaced.')
    return {'publication':publication,'tables':len(names)}

def main():
    try:
        result=configure_source(Ledger(Settings.from_env().administrator_dsn()))
        print('Source grants and four-table publication verified. Cloud connector readiness remains separate.');return 0
    except WorkshopError as error: print(str(error));return 2
    except Exception: print('source_configuration_failed: Check your administrator role and logical replication settings. Raw database errors are omitted.');return 2

if __name__=='__main__': raise SystemExit(main())
