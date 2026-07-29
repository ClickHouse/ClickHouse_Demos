import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import aiohttp
import pytest

from collector.config import Settings
from collector.health import HealthState
from collector.service import CollectorService, DedupeWindow, fixture_markets, fixture_ticks


class FakeAPI:
    async def discover(self, count):
        return fixture_markets(count)


class FakeStorage:
    def __init__(self, failures=0):
        self.failures = failures
        self.calls = []
        self.successful_write = asyncio.Event()

    async def ping(self):
        return None

    async def assert_schema(self):
        return None

    async def recent_ids(self, _minutes):
        return {"already-seen"}

    async def trade_checkpoints(self, _conditions):
        return {}

    async def insert(self, table, rows, columns, token):
        self.calls.append((table, rows, columns, token))
        if self.failures:
            self.failures -= 1
            raise RuntimeError("temporary write failure")
        self.successful_write.set()


def settings(mode="fixture"):
    return Settings(
        clickhouse_host="example.clickhouse.cloud",
        clickhouse_port=8443,
        clickhouse_user="default",
        clickhouse_password="secret",
        clickhouse_database="polymarket",
        clickhouse_secure=True,
        mode=mode,
        market_count=2,
        reconcile_seconds=1,
        book_fallback_seconds=1,
        stall_seconds=1,
        dedupe_minutes=15,
        initial_lookback_minutes=10,
        health_port=8090,
    )


def test_dedupe_window_reserves_commits_releases_and_evicts():
    window = DedupeWindow(capacity=2)
    window.hydrate({"old"})

    assert window.reserve(["old", "new"]) == ["new"]
    assert window.reserve(["new"]) == []
    window.release(["new"])
    assert window.reserve(["new"]) == ["new"]
    window.commit(["new"])
    window.commit(["third"])

    assert "old" not in window.committed
    assert set(window.committed) == {"new", "third"}


def test_dedupe_hydration_keeps_newest_ids_at_the_lru_tail():
    window = DedupeWindow(capacity=2)
    window.hydrate(["older", "newer"])

    window.commit(["newest"])

    assert list(window.committed) == ["newer", "newest"]


@pytest.mark.asyncio
async def test_prepare_hydrates_dedupe_and_persists_fixture_markets():
    storage = FakeStorage()
    health = HealthState()
    service = CollectorService(settings(), FakeAPI(), storage, health)

    await service.prepare()

    assert "already-seen" in service.dedupe.committed
    assert storage.calls[0][0] == "markets"
    assert health.status == "fixture"
    assert health.watched_markets == 2


@pytest.mark.asyncio
async def test_enqueue_filters_committed_and_pending_ids():
    storage = FakeStorage()
    service = CollectorService(settings(), FakeAPI(), storage, HealthState())
    service.dedupe.hydrate({"seen"})
    rows = [
        {"event_id": "seen", "value": 1},
        {"event_id": "fresh", "value": 2},
        {"event_id": "fresh", "value": 2},
    ]

    await service.enqueue("price_ticks", rows, ["event_id", "value"], "event_id")

    batch = service.queue.get_nowait()
    assert [row["event_id"] for row in batch.rows] == ["fresh"]
    assert batch.ids == ["fresh"]


@pytest.mark.asyncio
async def test_writer_retries_identical_batch_before_committing_id():
    storage = FakeStorage(failures=1)
    service = CollectorService(
        settings(), FakeAPI(), storage, HealthState(), sleep=lambda _: asyncio.sleep(0)
    )
    rows = fixture_ticks(fixture_markets(1), 1, datetime.now(UTC))
    await service.enqueue("price_ticks", rows, list(rows[0]), "event_id")

    task = asyncio.create_task(service.writer_loop())
    await asyncio.wait_for(storage.successful_write.wait(), timeout=1)
    await asyncio.wait_for(service.queue.join(), timeout=1)
    service.stop.set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert len(storage.calls) == 2
    assert storage.calls[0][3] == storage.calls[1][3]
    assert rows[0]["event_id"] in service.dedupe.committed
    assert service.health.last_clickhouse_write_at is not None


@pytest.mark.asyncio
async def test_writer_coalesces_compatible_small_batches():
    storage = FakeStorage()
    service = CollectorService(settings(), FakeAPI(), storage, HealthState())
    await service.enqueue(
        "price_ticks", [{"event_id": "one"}], ["event_id"], "event_id"
    )
    await service.enqueue(
        "price_ticks", [{"event_id": "two"}], ["event_id"], "event_id"
    )

    task = asyncio.create_task(service.writer_loop())
    await asyncio.wait_for(storage.successful_write.wait(), timeout=1)
    await asyncio.wait_for(service.queue.join(), timeout=1)
    service.stop.set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert len(storage.calls) == 1
    assert [row["event_id"] for row in storage.calls[0][1]] == ["one", "two"]


@pytest.mark.asyncio
async def test_queue_capacity_counts_rows_and_applies_backpressure():
    constrained = replace(settings(), queue_capacity=2)
    service = CollectorService(constrained, FakeAPI(), FakeStorage(), HealthState())
    first_rows = [
        {"event_id": "first", "value": 1},
        {"event_id": "second", "value": 2},
    ]
    await service.enqueue(
        "price_ticks", first_rows, ["event_id", "value"], "event_id"
    )

    blocked = asyncio.create_task(
        service.enqueue(
            "price_ticks",
            [{"event_id": "third", "value": 3}],
            ["event_id", "value"],
            "event_id",
        )
    )
    await asyncio.sleep(0)

    assert service.health.queue_depth == 2
    assert service.health.status == "unhealthy"
    assert blocked.done() is False

    first_batch = service.queue.get_nowait()
    await service._release_capacity(len(first_batch.rows))
    await blocked

    assert service.health.queue_depth == 1
    assert service.queue.get_nowait().ids == ["third"]


@pytest.mark.asyncio
async def test_reconciled_checkpoint_advances_when_every_row_is_duplicate():
    service = CollectorService(settings(), FakeAPI(), FakeStorage(), HealthState())
    checkpoint = datetime.now(UTC)
    service.dedupe.hydrate({"seen"})

    await service.enqueue(
        "trades",
        [{"trade_id": "seen"}],
        ["trade_id"],
        "trade_id",
        {"condition": checkpoint},
    )

    assert service.queue.empty()
    assert service.checkpoints["condition"] == checkpoint


@pytest.mark.asyncio
async def test_empty_reconciliation_advances_checkpoint_without_a_write():
    service = CollectorService(settings(), FakeAPI(), FakeStorage(), HealthState())
    checkpoint = datetime.now(UTC)

    await service.enqueue(
        "trades", [], ["trade_id"], "trade_id", {"condition": checkpoint}
    )

    assert service.queue.empty()
    assert service.checkpoints["condition"] == checkpoint


def test_source_rows_must_match_the_discovered_token_condition_pair():
    health = HealthState()
    service = CollectorService(settings(), FakeAPI(), FakeStorage(), health)
    service.tokens = fixture_markets(1)
    token = service.tokens[0]
    valid = {"token_id": token.token_id, "condition_id": token.condition_id}
    wrong_condition = {**valid, "condition_id": "0x" + "9" * 64}
    unknown_token = {**valid, "token_id": token.token_id + 99}

    accepted = service._filter_watched(
        [valid, wrong_condition, unknown_token], "test_source"
    )

    assert accepted == [valid]
    assert health.source_parse_errors_total == 2


def test_non_midpoint_events_do_not_claim_quote_freshness():
    service = CollectorService(settings("live"), FakeAPI(), FakeStorage(), HealthState())
    service.tokens = fixture_markets(1)
    token = service.tokens[0]

    accepted = service._filter_watched(
        [
            {
                "token_id": token.token_id,
                "condition_id": token.condition_id,
                "midpoint": "0",
            }
        ],
        "websocket",
    )

    assert len(accepted) == 1
    assert service._token_price_at == {}


def test_source_health_recovers_and_clickhouse_failure_has_precedence():
    now = datetime.now(UTC)
    health = HealthState(started_at=now - timedelta(minutes=2))
    service = CollectorService(settings("live"), FakeAPI(), FakeStorage(), health)
    service.tokens = fixture_markets(1)

    service._refresh_health(now)
    assert health.status == "unhealthy"
    assert health.reason == "all_sources_stale"

    service._ws_healthy = True
    health.last_websocket_event_at = now
    health.last_trade_reconcile_at = now
    for token in service.tokens:
        service._token_price_at[token.token_id] = now
        service._token_websocket_at[token.token_id] = now
    health.watched_tokens = len(service.tokens)
    service._refresh_health(now)
    assert health.status == "live"

    service._write_failure_started_at = now - timedelta(seconds=61)
    service._refresh_health(now)
    assert health.status == "unhealthy"
    assert health.reason == "clickhouse_write_stalled"


@pytest.mark.asyncio
async def test_websocket_loop_retries_transport_failure_and_enqueues_data():
    class ReconnectingAPI:
        def __init__(self):
            self.calls = 0

        async def websocket_messages(self, _token_ids, _stall_seconds):
            self.calls += 1
            if self.calls == 1:
                raise aiohttp.ClientConnectionError("handshake failed")
            token = fixture_markets(1)[0]
            yield {
                "event_type": "best_bid_ask",
                "market": token.condition_id,
                "asset_id": str(token.token_id),
                "timestamp": "1782753357257",
                "best_bid": "0.4",
                "best_ask": "0.6",
            }
            await asyncio.sleep(3600)

    api = ReconnectingAPI()
    service = CollectorService(
        settings("live"), api, FakeStorage(), HealthState(), sleep=lambda _: asyncio.sleep(0)
    )
    service.tokens = fixture_markets(1)
    task = asyncio.create_task(service.websocket_loop())

    batch = await asyncio.wait_for(service.queue.get(), timeout=1)
    service.stop.set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert api.calls >= 2
    assert batch.rows[0]["source"] == "WEBSOCKET"
    assert service.health.source_errors_total == 1


@pytest.mark.asyncio
async def test_book_fallback_keeps_ticks_flowing_while_websocket_is_down():
    class BookAPI:
        async def book(self, token_id):
            return {
                "asset_id": str(token_id),
                "timestamp": "1782753357257",
                "bids": [{"price": "0.4"}],
                "asks": [{"price": "0.6"}],
            }

    service = CollectorService(
        settings("live"),
        BookAPI(),
        FakeStorage(),
        HealthState(),
        sleep=lambda _: asyncio.sleep(0.001),
    )
    service.tokens = fixture_markets(1)
    task = asyncio.create_task(service.book_fallback_loop())

    batch = await asyncio.wait_for(service.queue.get(), timeout=1)
    service.stop.set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert all(row["source"] == "CLOB_REST" for row in batch.rows)
    assert service.health.last_book_fallback_at is not None


@pytest.mark.asyncio
async def test_prepare_no_markets_sets_degraded(monkeypatch):
    class EmptyAPI:
        async def discover(self, _count):
            return []

    storage = FakeStorage()
    health = HealthState()
    service = CollectorService(settings("live"), EmptyAPI(), storage, health)

    await service.prepare()

    assert health.status == "degraded"
    assert health.reason == "no_active_markets"
    assert storage.calls == []


@pytest.mark.asyncio
async def test_run_surfaces_an_unexpected_background_task_failure():
    service = CollectorService(settings(), FakeAPI(), FakeStorage(), HealthState())

    async def fail_fixture_loop():
        raise RuntimeError("fixture task crashed")

    service.fixture_loop = fail_fixture_loop

    with pytest.raises(RuntimeError, match="fixture task crashed"):
        await service.run()


@pytest.mark.asyncio
async def test_all_malformed_trade_page_does_not_advance_checkpoint():
    class MalformedAPI:
        async def trades(self, _condition_id, _start, _end):
            return [{"conditionId": "bad"}]

    service = CollectorService(settings("live"), MalformedAPI(), FakeStorage(), HealthState())
    service.tokens = fixture_markets(1)
    condition_id = service.tokens[0].condition_id

    succeeded = await service._reconcile_condition(condition_id, datetime.now(UTC))

    assert succeeded is False
    assert condition_id not in service.checkpoints


@pytest.mark.asyncio
async def test_trade_reconciliation_rejects_only_the_malformed_item():
    class MixedAPI:
        async def trades(self, _condition_id, _start, _end):
            return [
                {"conditionId": "missing-required-fields"},
                {
                        "conditionId": "0x" + f"{1:064x}",
                    "asset": "10000000",
                    "timestamp": 1_700_000_000,
                    "transactionHash": "0x" + "2" * 64,
                    "proxyWallet": "0x" + "3" * 40,
                    "side": "BUY",
                    "price": "0.5",
                    "size": "2",
                },
            ]

    service = CollectorService(settings("live"), MixedAPI(), FakeStorage(), HealthState())
    service.tokens = fixture_markets(1)

    task = asyncio.create_task(service.trade_reconcile_loop())
    batch = await asyncio.wait_for(service.queue.get(), timeout=1)
    service.stop.set()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    assert len(batch.rows) == 1
    assert service.health.source_parse_errors_total == 1
