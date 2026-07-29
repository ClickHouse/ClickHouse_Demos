import asyncio
import os
from datetime import UTC, datetime

import pytest

from collector.config import Settings
from collector.models import build_tick, normalize_trade, stable_batch_token
from collector.storage import MARKET_COLUMNS, TICK_COLUMNS, TRADE_COLUMNS, ClickHouseStorage


pytestmark = pytest.mark.skipif(
    os.getenv("POLYMARKET_CLICKHOUSE_INTEGRATION") != "1",
    reason="requires the disposable ClickHouse integration container",
)


@pytest.mark.asyncio
async def test_clickhouse_storage_round_trip_and_retry_deduplication():
    settings = Settings(
        clickhouse_host=os.environ["CLICKHOUSE_HOST"],
        clickhouse_port=int(os.environ["CLICKHOUSE_PORT"]),
        clickhouse_user="default",
        clickhouse_password=os.environ["CLICKHOUSE_PASSWORD"],
        clickhouse_database="polymarket",
        clickhouse_secure=False,
        mode="fixture",
        market_count=1,
        reconcile_seconds=10,
        book_fallback_seconds=30,
        stall_seconds=30,
        dedupe_minutes=15,
        initial_lookback_minutes=10,
        health_port=8090,
    )
    storage = ClickHouseStorage(settings)
    now = datetime.now(UTC)
    condition_id = "0x" + "a" * 64
    token_id = 123456789
    market = {
        "market_id": 1,
        "condition_id": condition_id,
        "token_id": token_id,
        "outcome": "Yes",
        "question": "Will the adapter integration pass?",
        "slug": "adapter-integration",
        "active": True,
        "accepting_orders": False,
        "volume_24h": "100",
        "observed_at": now,
    }
    tick = build_tick(
        event_kind="best_bid_ask",
        source="FIXTURE",
        condition_id=condition_id,
        token_id=token_id,
        timestamp=now.isoformat(),
        best_bid="0.48",
        best_ask="0.52",
        source_hash="adapter-integration",
        raw={"fixture": True},
    )
    trade = normalize_trade(
        {
            "transactionHash": "0x" + "b" * 64,
            "asset": str(token_id),
            "proxyWallet": "0x" + "c" * 40,
            "side": "BUY",
            "price": "0.5",
            "size": "2",
            "timestamp": int(now.timestamp()),
            "conditionId": condition_id,
            "outcome": "Yes",
            "title": market["question"],
        }
    )

    try:
        await storage.ping()
        await storage.assert_schema()
        await storage.insert("markets", [market], MARKET_COLUMNS, "market-adapter")
        tick_token = stable_batch_token([tick["event_id"]])
        await storage.insert("price_ticks", [tick], TICK_COLUMNS, tick_token)
        await storage.insert("price_ticks", [tick], TICK_COLUMNS, tick_token)
        await storage.insert(
            "trades", [trade], TRADE_COLUMNS, stable_batch_token([trade["trade_id"]])
        )

        recent_ids = await storage.recent_ids(15)
        checkpoints = await storage.trade_checkpoints([condition_id])

        def counts():
            return storage.client.query(
                """
                SELECT
                    (
                        SELECT count()
                        FROM polymarket.price_ticks
                        WHERE event_id = {tick_id:String}
                    ),
                    (
                        SELECT count()
                        FROM polymarket.trades_clean
                        WHERE trade_id = {trade_id:String}
                    ),
                    (
                        SELECT countMerge(updates)
                        FROM polymarket.market_midpoints_1m
                        WHERE token_id = {token_id:UInt256}
                    )
                """,
                parameters={
                    "tick_id": tick["event_id"],
                    "trade_id": trade["trade_id"],
                    "token_id": token_id,
                },
            ).first_row

        tick_count, trade_count, candle_count = await asyncio.to_thread(counts)
        # The standalone MergeTree test server has no Keeper-backed insert
        # deduplication. ClickHouse Cloud's SharedMergeTree applies the token;
        # this check proves the real adapter accepts and retries that setting.
        assert tick_count == 2
        assert trade_count == 1
        assert candle_count == 2
        assert {tick["event_id"], trade["trade_id"]} <= set(recent_ids)
        assert checkpoints[condition_id].replace(microsecond=0) == now.replace(
            microsecond=0
        )
    finally:
        await storage.close()
