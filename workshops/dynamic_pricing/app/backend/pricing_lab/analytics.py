"""Bounded ClickHouse access. Values are bound; generated identifiers are validated."""
from .observability import traced
import re
from datetime import UTC, datetime
from threading import Lock

import clickhouse_connect

from .settings import Settings
from .contracts import WorkshopError


def identifier(value: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", value):
        raise ValueError("Invalid generated database or table identifier.")
    return value


class AnalyticsClient:
    def __init__(self, settings: Settings, request_timeout_seconds: int = 1800):
        self.settings = settings
        if not 1 <= request_timeout_seconds <= 1800:
            raise ValueError("Use a request timeout between 1 and 1,800 seconds.")
        self.request_timeout_seconds = request_timeout_seconds
        self.database = identifier(settings.clickhouse_database)
        self._client = None
        self._connect_lock = Lock()

    def bootstrap_database(self):
        """Create the validated workshop database before its first connection."""
        self.require_host()
        client = clickhouse_connect.get_client(
            host=self.settings.clickhouse_host, port=self.settings.clickhouse_port,
            secure=self.settings.clickhouse_secure, database='default',
            username=self.settings.clickhouse_username,
            password=self.settings.clickhouse_password.get_secret_value(),
            autogenerate_session_id=False, connect_timeout=10,
            send_receive_timeout=self.request_timeout_seconds)
        try:
            client.command(f'CREATE DATABASE IF NOT EXISTS {self.database}')
        finally:
            client.close()

    @property
    def client(self):
        with self._connect_lock:
            if self._client is None:
                self.require_host()
                self._client = clickhouse_connect.get_client(
                    host=self.settings.clickhouse_host, port=self.settings.clickhouse_port,
                    secure=self.settings.clickhouse_secure, database=self.database,
                    username=self.settings.clickhouse_username,
                    password=self.settings.clickhouse_password.get_secret_value(),
                    autogenerate_session_id=False, connect_timeout=10, send_receive_timeout=self.request_timeout_seconds,
                )
        return self._client

    def require_host(self):
        if not self.settings.clickhouse_host:
            raise WorkshopError('cloud_endpoint_missing','Run bash scripts/setup-cloud.sh to save your ClickHouse Cloud endpoint before querying data.')

    @traced('pricing.sql.query')
    def query(self, sql: str, parameters: dict | None = None) -> list[dict]:
        # The pinned driver otherwise returns naive datetimes for pytz.UTC.
        date_columns = ("effective_at","event_time","emitted_at","commit_acknowledged_at","acknowledged_at",
                        "inventory_expires_at","_peerdb_synced_at","captured_at","scenario_cutoff","hour",
                        "occurred_at","query_start_time","query_start_time_microseconds","expires_at","cutoff","logical_end")
        result = self.client.query(sql, parameters=parameters or {},query_tz=UTC,
                                   column_tzs={name:UTC for name in date_columns},
                                   settings={"max_execution_time": 60, "max_result_rows": 10000, "result_overflow_mode": "throw"})
        return [{k:(v.replace(tzinfo=UTC) if v.tzinfo is None else v.astimezone(UTC))
                 if isinstance(v,datetime) else v for k,v in row.items()} for row in result.named_results()]

    @traced('pricing.sql.measure')
    def measured_query(self,sql,parameters):
        import time
        started=time.monotonic()
        result=self.client.query(sql,parameters=parameters,settings={'use_query_cache':0,'max_execution_time':60,'max_result_rows':10000,'result_overflow_mode':'throw'})
        duration=time.monotonic()-started
        def integer(key):
            value=result.summary.get(key)
            return int(value) if value is not None else None
        return {'rows':list(result.named_results()),'query_id':result.query_id,'client_seconds':duration,'read_rows':integer('read_rows'),'read_bytes':integer('read_bytes')}

    @traced('pricing.sql.command')
    def command(self, sql: str, parameters: dict | None = None):
        return self.client.command(sql, parameters=parameters or {}, settings={"max_execution_time": 1800})

    def close(self):
        if self._client is not None:
            self._client.close()
            self._client = None

    def live_current(self, scope, subject) -> list[dict]:
        return self.query(f"""SELECT * FROM {self.database}.sales_live_current
          WHERE owner_id={{owner:String}} AND scenario_id={{scenario:String}} AND generation={{generation:Int32}}
          AND sku={{sku:Int32}} AND store_id={{store:Int32}} AND channel='store' ORDER BY event_id""",
          {"owner":scope.owner_id,"scenario":scope.scenario_id,"generation":scope.generation,"sku":subject.sku,"store":subject.store_id})
