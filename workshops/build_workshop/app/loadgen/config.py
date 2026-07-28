from __future__ import annotations

import os
from dataclasses import dataclass


LOCAL_DATABASE_HOSTS = {
    "postgres",
    "localhost",
    "0.0.0.0",
    "::1",
    "host.docker.internal",
}


def env(name: str, default: str) -> str:
    value = os.getenv(name)
    return default if value is None or value == "" else value


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(
            f"{name} is required. Configure the ClickHouse-managed Postgres "
            "connection from Module 03; local database fallbacks are disabled."
        )
    return value


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    sslmode: str
    publication: str


def load_postgres_config() -> PostgresConfig:
    host = required_env("PGHOST")
    normalized_host = host.lower().rstrip(".")
    if normalized_host in LOCAL_DATABASE_HOSTS or normalized_host.startswith("127."):
        raise RuntimeError(
            f"PGHOST={host} points to a local database. Use the managed Postgres "
            "hostname returned by clickhousectl in Module 03."
        )

    sslmode = env("PGSSLMODE", "require").lower()
    if sslmode != "require":
        raise RuntimeError(
            "PGSSLMODE=require is mandatory for ClickHouse-managed Postgres."
        )

    return PostgresConfig(
        host=host,
        port=int(env("PGPORT", "5432")),
        database=env("PGDATABASE", "postgres"),
        user=env("PGUSER", "postgres"),
        password=required_env("PGPASSWORD"),
        sslmode=sslmode,
        publication=env("PG_PUBLICATION", "pub_taxi"),
    )
