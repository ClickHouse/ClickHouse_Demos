"""Create the arena database and a least-privilege read-only user.
Run once after credentials are in place. Idempotent.
Usage: source .env && python scripts/setup_clickhouse.py
"""
from arena.config import load_config
from agents.chclient import make_admin_client


def _identifier(value: str) -> str:
    if not value or not value.replace("_", "").isalnum():
        raise ValueError(f"unsafe ClickHouse identifier: {value!r}")
    return f"`{value}`"


def _literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def configure_database(admin, ch) -> None:
    database = _identifier(ch.database)
    user = _identifier(ch.ro_user)
    admin.command(f"CREATE DATABASE IF NOT EXISTS {database}")
    admin.command(
        f"CREATE USER IF NOT EXISTS {user} "
        f"IDENTIFIED WITH sha256_password BY {_literal(ch.ro_password)}"
    )
    # CREATE USER IF NOT EXISTS does not update a reused learner account.
    admin.command(
        f"ALTER USER {user} IDENTIFIED WITH sha256_password BY {_literal(ch.ro_password)}"
    )
    lim = ch.query_limits
    admin.command(
        f"ALTER USER {user} SETTINGS "
        f"max_execution_time = {lim.max_execution_time}, "
        f"max_result_rows = {lim.max_result_rows}, "
        f"max_memory_usage = {lim.max_memory_usage}, "
        f"max_rows_to_read = {lim.max_rows_to_read}, "
        f"max_bytes_to_read = {lim.max_bytes_to_read}, "
        f"readonly = 1"
    )


def main() -> None:
    cfg = load_config()
    ch = cfg.clickhouse
    # Bootstrap via the always-present 'default' database (arena db may not exist yet).
    admin = make_admin_client(ch, database="default")

    configure_database(admin, ch)
    print(f"OK: database {ch.database} and user {ch.ro_user} ready.")


if __name__ == "__main__":
    main()
