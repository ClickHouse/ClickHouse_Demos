from types import SimpleNamespace

import agents.chclient as chclient
from arena.config import ClickHouseCfg, QueryLimits


def _cfg():
    return ClickHouseCfg(
        host="example", port=8443, secure=True, database="custom_arena",
        admin_user="admin", admin_password="admin", ro_user="arena_ro",
        ro_password="ro",
        query_limits=QueryLimits(max_execution_time=15, max_result_rows=100000,
                                 max_memory_usage=1000, max_rows_to_read=2000,
                                 max_bytes_to_read=3000, result_limit=200),
    )


def test_read_only_client_caps_returned_rows(monkeypatch):
    seen = []
    fake = SimpleNamespace(query=lambda sql: (
        seen.append(sql) or SimpleNamespace(result_rows=[(1,)], column_names=["x"])))
    monkeypatch.setattr(chclient.clickhouse_connect, "get_client", lambda **kwargs: fake)
    client = chclient.ROClickHouseClient(_cfg())

    assert client.query("SELECT 1;").rows == [(1,)]
    assert seen == ["SELECT * FROM (SELECT 1) LIMIT 200"]
