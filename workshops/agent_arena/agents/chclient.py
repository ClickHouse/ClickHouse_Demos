from dataclasses import dataclass
import clickhouse_connect
from arena.config import ClickHouseCfg


def make_admin_client(cfg: ClickHouseCfg, database: str | None = None):
    """Full-privilege client for setup + writing results. Never used to run agent SQL.

    Pass database='default' during bootstrap before the arena database exists.
    """
    return clickhouse_connect.get_client(
        host=cfg.host, port=cfg.port, secure=cfg.secure,
        username=cfg.admin_user, password=cfg.admin_password,
        database=database or cfg.database,
    )


@dataclass
class QueryResult:
    rows: list[tuple]
    cols: list[str]


class ROClickHouseClient:
    """Read-only client used to execute agent-generated SQL. Enforces server-side limits."""

    def __init__(self, cfg: ClickHouseCfg):
        self._cfg = cfg
        # Resource limits + readonly are enforced as the arena_ro user's own
        # server-side settings (see scripts/setup_clickhouse.py). A readonly=1
        # user cannot override settings at query time, so we pass none here.
        self._client = clickhouse_connect.get_client(
            host=cfg.host, port=cfg.port, secure=cfg.secure,
            username=cfg.ro_user, password=cfg.ro_password,
            database=cfg.database,
        )

    def query(self, sql: str) -> QueryResult:
        cleaned = sql.strip().rstrip(";").strip()
        bounded = f"SELECT * FROM ({cleaned}) LIMIT {self._cfg.query_limits.result_limit}"
        res = self._client.query(bounded)
        rows = [tuple(r) for r in res.result_rows]
        return QueryResult(rows=rows, cols=list(res.column_names))
