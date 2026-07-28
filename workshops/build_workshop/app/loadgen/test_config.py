from __future__ import annotations

import pytest

from config import load_postgres_config


def set_managed_postgres(monkeypatch) -> None:
    monkeypatch.setenv("PGHOST", "pg.example.clickhouse.cloud")
    monkeypatch.setenv("PGPASSWORD", "test-password")
    monkeypatch.setenv("PGSSLMODE", "require")


@pytest.mark.parametrize(
    "host",
    [
        "postgres",
        "localhost",
        "127.0.0.1",
        "127.42.0.7",
        "0.0.0.0",
        "::1",
        "host.docker.internal",
    ],
)
def test_rejects_local_postgres_hosts(monkeypatch, host: str) -> None:
    set_managed_postgres(monkeypatch)
    monkeypatch.setenv("PGHOST", host)

    with pytest.raises(RuntimeError, match="points to a local database"):
        load_postgres_config()


def test_requires_tls(monkeypatch) -> None:
    set_managed_postgres(monkeypatch)
    monkeypatch.setenv("PGSSLMODE", "disable")

    with pytest.raises(RuntimeError, match="PGSSLMODE=require"):
        load_postgres_config()


def test_accepts_managed_postgres(monkeypatch) -> None:
    set_managed_postgres(monkeypatch)

    config = load_postgres_config()

    assert config.host == "pg.example.clickhouse.cloud"
    assert config.port == 5432
    assert config.database == "postgres"
    assert config.user == "postgres"
    assert config.sslmode == "require"
