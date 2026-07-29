from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from typing import Any

import clickhouse_connect

from .config import Settings


MARKET_COLUMNS = [
    "market_id",
    "condition_id",
    "token_id",
    "outcome",
    "question",
    "slug",
    "active",
    "accepting_orders",
    "volume_24h",
    "observed_at",
]
TICK_COLUMNS = [
    "event_id",
    "condition_id",
    "token_id",
    "event_at",
    "observed_at",
    "event_kind",
    "source",
    "price",
    "size",
    "side",
    "best_bid",
    "best_ask",
    "midpoint",
    "source_hash",
    "raw_payload",
]
TRADE_COLUMNS = [
    "trade_id",
    "condition_id",
    "token_id",
    "event_at",
    "observed_at",
    "proxy_wallet",
    "side",
    "price",
    "size",
    "outcome",
    "transaction_hash",
    "title",
]


def _text(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


class ClickHouseStorage:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
            secure=settings.clickhouse_secure,
            connect_timeout=10,
            send_receive_timeout=15,
        )

    async def ping(self) -> None:
        await asyncio.to_thread(self.client.command, "SELECT 1")

    async def assert_schema(self) -> None:
        expected = {
            "markets",
            "price_ticks",
            "trades",
            "trades_clean",
            "market_midpoints_1m",
            "market_midpoints_1m_mv",
        }

        def fetch() -> set[str]:
            result = self.client.query(
                "SELECT name FROM system.tables WHERE database = {db:String}",
                parameters={"db": self.settings.clickhouse_database},
            )
            return {str(row[0]) for row in result.result_rows}

        actual = await asyncio.to_thread(fetch)
        missing = sorted(expected - actual)
        if missing:
            raise RuntimeError(
                "missing ClickHouse tables: " + ", ".join(missing) + "; run Module 02 SQL"
            )

    async def insert(
        self, table: str, rows: list[dict[str, Any]], columns: list[str], token: str
    ) -> None:
        if not rows:
            return
        data = [[row[column] for column in columns] for row in rows]
        await asyncio.to_thread(
            self.client.insert,
            f"{self.settings.clickhouse_database}.{table}",
            data,
            column_names=columns,
            settings={
                "async_insert": 1,
                "async_insert_deduplicate": 1,
                "wait_for_async_insert": 1,
                "insert_deduplication_token": token,
            },
        )

    async def recent_ids(self, minutes: int) -> list[str]:
        since = datetime.now(UTC) - timedelta(minutes=minutes)

        def fetch() -> list[str]:
            rows = self.client.query(
                """
                SELECT id
                FROM
                (
                    SELECT event_id AS id, observed_at
                    FROM price_ticks
                    WHERE observed_at >= {since:DateTime64(3)}
                    UNION ALL
                    SELECT trade_id AS id, observed_at
                    FROM trades FINAL
                    WHERE observed_at >= {since:DateTime64(3)}
                )
                ORDER BY observed_at DESC
                LIMIT 50000
                """,
                parameters={"since": since},
            ).result_rows
            return [_text(row[0]) for row in reversed(rows)]

        return await asyncio.to_thread(fetch)

    async def trade_checkpoints(
        self, condition_ids: Iterable[str]
    ) -> dict[str, datetime]:
        ids = list(condition_ids)
        if not ids:
            return {}

        def fetch() -> dict[str, datetime]:
            result = self.client.query(
                """
                SELECT condition_id, max(event_at)
                FROM trades FINAL
                WHERE condition_id IN {ids:Array(String)}
                GROUP BY condition_id
                """,
                parameters={"ids": ids},
            )
            return {
                _text(row[0]): row[1].replace(tzinfo=UTC)
                for row in result.result_rows
            }

        return await asyncio.to_thread(fetch)

    async def close(self) -> None:
        await asyncio.to_thread(self.client.close)
