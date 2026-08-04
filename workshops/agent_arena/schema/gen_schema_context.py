"""Apply v_* views and regenerate schema/schema_context.md from system.columns.

  source .env && python schema/gen_schema_context.py            # apply seed views + md
  source .env && python schema/gen_schema_context.py --no-apply  # md only (e.g. after CDC repoint)
"""
import argparse
from arena.config import load_config
from agents.chclient import make_admin_client

VIEWS = ["v_customers", "v_products", "v_orders", "v_order_items", "v_events"]

SEMANTICS = {
    "v_orders.status": "one of placed|paid|shipped|delivered|cancelled|returned",
    "v_orders.channel": "one of web|ios|android|partner",
    "v_customers.country": "ISO-ish: SG|VN|TH|ID|AU|IN|TW|JP",
    "v_customers.segment": "consumer|smb|enterprise",
    "v_products.category": "electronics|home|apparel|grocery|beauty",
    "v_events.event_type": "view|search|add_to_cart|checkout|purchase",
    "v_order_items.unit_price": "price at time of sale",
}


def _identifier(value: str) -> str:
    if not value or not value.replace("_", "").isalnum():
        raise ValueError(f"unsafe ClickHouse identifier: {value!r}")
    return f"`{value}`"


def apply_views_and_grants(admin, ch) -> None:
    with open("schema/clickhouse_views.sql") as f:
        for stmt in f.read().split(";"):
            if stmt.strip():
                admin.command(stmt)
    database = _identifier(ch.database)
    user = _identifier(ch.ro_user)
    # The account is dedicated to the learner agent. Clear legacy database-wide
    # grants from older workshop revisions before granting only the safe views.
    admin.command(f"REVOKE SELECT ON *.* FROM {user}")
    for view in VIEWS:
        admin.command(f"GRANT SELECT ON {database}.{_identifier(view)} TO {user}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-apply", action="store_true",
                    help="skip applying seed-based views; just generate the md "
                         "from existing v_* (use after a CDC repoint)")
    args = ap.parse_args()

    cfg = load_config()
    db = cfg.clickhouse.database
    admin = make_admin_client(cfg.clickhouse)
    if not args.no_apply:
        apply_views_and_grants(admin, cfg.clickhouse)

    lines = ["# Schema context (agent-facing)\n",
             "Query ONLY these views. ClickHouse SQL dialect. "
             "Revenue = quantity*unit_price - discount.\n"]
    for v in VIEWS:
        cols = admin.query(
            "SELECT name, type FROM system.columns "
            f"WHERE database = '{db}' AND table = '{v}' ORDER BY position"
        ).result_rows
        lines.append(f"\n## {v}")
        for name, typ in cols:
            sem = SEMANTICS.get(f"{v}.{name}", "")
            sem = f" — {sem}" if sem else ""
            lines.append(f"- `{name}` {typ}{sem}")
    with open("schema/schema_context.md", "w") as f:
        f.write("\n".join(lines) + "\n")
    print("wrote schema/schema_context.md")


if __name__ == "__main__":
    main()
