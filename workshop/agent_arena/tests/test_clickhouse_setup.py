from types import SimpleNamespace

import datagen.generator as generator
from scripts.setup_clickhouse import configure_database
from schema.gen_schema_context import apply_views_and_grants, VIEWS


class FakeAdmin:
    def __init__(self):
        self.commands = []
        self.inserts = []

    def command(self, sql):
        self.commands.append(" ".join(sql.split()))

    def insert(self, table, rows, column_names):
        self.inserts.append((table, rows, column_names))


def _clickhouse():
    limits = SimpleNamespace(max_execution_time=15, max_result_rows=100000,
                             max_memory_usage=4000, max_rows_to_read=200000,
                             max_bytes_to_read=500000, result_limit=200)
    return SimpleNamespace(database="custom_arena", ro_user="arena_ro",
                           ro_password="password-with-'quote", query_limits=limits)


def test_setup_resets_user_and_does_not_grant_raw_tables():
    admin = FakeAdmin()
    configure_database(admin, _clickhouse())
    commands = "\n".join(admin.commands)
    assert "CREATE DATABASE IF NOT EXISTS `custom_arena`" in commands
    assert "ALTER USER `arena_ro` IDENTIFIED" in commands
    assert "password-with-\\'quote" in commands
    assert "max_bytes_to_read = 500000" in commands
    assert "GRANT SELECT ON `custom_arena`.*" not in commands


def test_generator_uses_configured_database_and_reseeds(monkeypatch):
    admin = FakeAdmin()
    monkeypatch.setattr(generator, "load_config",
                        lambda: SimpleNamespace(clickhouse=_clickhouse()))
    monkeypatch.setattr(generator, "make_admin_client", lambda cfg: admin)

    generator.write_clickhouse({"customers": [[1, "A", "a@example.com", "SG", "smb",
                                                "2026-01-01", "2026-01-01", "2026-01-01"]]})

    commands = "\n".join(admin.commands)
    assert "arena_house" not in commands
    for table in generator.BUSINESS_COLUMNS:
        assert f"TRUNCATE TABLE IF EXISTS `custom_arena`.`{table}`" in commands
    assert admin.inserts[0][0] == "custom_arena.customers"


def test_views_are_replaced_and_only_views_are_granted():
    admin = FakeAdmin()
    apply_views_and_grants(admin, _clickhouse())
    commands = "\n".join(admin.commands)
    assert "arena_house" not in commands
    assert commands.count("CREATE OR REPLACE VIEW") == len(VIEWS)
    assert "REVOKE SELECT ON *.* FROM `arena_ro`" in commands
    for view in VIEWS:
        assert f"GRANT SELECT ON `custom_arena`.`{view}` TO `arena_ro`" in commands
