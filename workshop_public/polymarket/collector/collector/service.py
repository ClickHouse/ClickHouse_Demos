from __future__ import annotations

import asyncio
import json
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import aiohttp

from .api import PolymarketAPI, RateLimited, SourceError
from .config import Settings
from .health import HealthState
from .models import (
    MarketToken,
    build_tick,
    normalize_book,
    normalize_trade,
    normalize_ws_message,
    stable_batch_token,
)
from .storage import MARKET_COLUMNS, TICK_COLUMNS, TRADE_COLUMNS, ClickHouseStorage

WRITE_BATCH_ROWS = 1_000
WRITE_COALESCE_SECONDS = 0.05
SOURCE_CONCURRENCY = 5
MARKET_REFRESH_SECONDS = 300
RECONCILE_DEADLINE_SECONDS = 30


def log(event: str, **fields: Any) -> None:
    print(
        json.dumps(
            {"timestamp": datetime.now(UTC).isoformat(), "event": event, **fields},
            default=str,
            sort_keys=True,
        ),
        flush=True,
    )


class DedupeWindow:
    def __init__(self, capacity: int = 50_000):
        self.capacity = capacity
        self.committed: OrderedDict[str, None] = OrderedDict()
        self.pending: set[str] = set()

    def hydrate(self, ids: list[str] | set[str]) -> None:
        for item_id in ids:
            self._commit_one(item_id)

    def reserve(self, ids: list[str]) -> list[str]:
        fresh = []
        reserved = set(self.pending)
        for item_id in ids:
            if item_id in self.committed or item_id in reserved:
                continue
            fresh.append(item_id)
            reserved.add(item_id)
        self.pending.update(fresh)
        return fresh

    def commit(self, ids: list[str]) -> None:
        for item_id in ids:
            self.pending.discard(item_id)
            self._commit_one(item_id)

    def release(self, ids: list[str]) -> None:
        self.pending.difference_update(ids)

    def _commit_one(self, item_id: str) -> None:
        self.committed[item_id] = None
        self.committed.move_to_end(item_id)
        while len(self.committed) > self.capacity:
            self.committed.popitem(last=False)


@dataclass
class Batch:
    table: str
    rows: list[dict[str, Any]]
    columns: list[str]
    ids: list[str]
    token: str
    checkpoints: dict[str, datetime] = field(default_factory=dict)


class CollectorService:
    def __init__(
        self,
        settings: Settings,
        api: PolymarketAPI,
        storage: ClickHouseStorage,
        health: HealthState,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        self.settings = settings
        self.api = api
        self.storage = storage
        self.health = health
        self.sleep = sleep
        self.queue: asyncio.Queue[Batch] = asyncio.Queue()
        self._capacity_changed = asyncio.Condition()
        self._source_slots = asyncio.Semaphore(SOURCE_CONCURRENCY)
        self._writer_stop = asyncio.Event()
        self._queued_rows = 0
        self._write_failure_started_at: datetime | None = None
        self._last_tick_enqueued_at: datetime | None = None
        self._token_price_at: dict[int, datetime] = {}
        self._token_websocket_at: dict[int, datetime] = {}
        self._quiet_notice_at: datetime | None = None
        self.dedupe = DedupeWindow()
        self.tokens: list[MarketToken] = []
        self.checkpoints: dict[str, datetime] = {}
        self.stop = asyncio.Event()
        self._ws_healthy = False

    async def prepare(self) -> None:
        await self.storage.ping()
        await self.storage.assert_schema()
        self.dedupe.hydrate(await self.storage.recent_ids(self.settings.dedupe_minutes))
        if self.settings.mode == "fixture":
            self.tokens = fixture_markets(self.settings.market_count)
        else:
            self.tokens = await self.api.discover(self.settings.market_count)
        if not self.tokens:
            self.health.status = "degraded"
            self.health.reason = "no_active_markets"
            log(
                "no_active_markets",
                action="Set POLYMARKET_MODE=fixture and restart the collector",
            )
            return
        self.health.watched_markets = len({token.condition_id for token in self.tokens})
        self.health.watched_tokens = len(self.tokens)
        self.checkpoints = await self.storage.trade_checkpoints(
            {token.condition_id for token in self.tokens}
        )
        now = datetime.now(UTC)
        market_rows = [
            {
                "market_id": token.market_id,
                "condition_id": token.condition_id,
                "token_id": token.token_id,
                "outcome": token.outcome,
                "question": token.question,
                "slug": token.slug,
                "active": token.active,
                "accepting_orders": token.accepting_orders,
                "volume_24h": token.volume_24h,
                "observed_at": now,
            }
            for token in self.tokens
        ]
        ids = [f"market:{row['condition_id']}:{row['token_id']}:{now.isoformat()}" for row in market_rows]
        await self.storage.insert("markets", market_rows, MARKET_COLUMNS, stable_batch_token(ids))
        self.health.last_clickhouse_write_at = datetime.now(UTC)
        self.health.status = "live" if self.settings.mode == "live" else "fixture"
        if self.settings.mode == "fixture":
            self.health.websocket = "fixture"
        self.health.reason = "ready"
        log("collector_ready", mode=self.settings.mode, markets=self.health.watched_markets)

    async def run(self) -> None:
        await self.prepare()
        writer = asyncio.create_task(self.writer_loop(), name="clickhouse-writer")
        producers = [asyncio.create_task(self.health_loop(), name="health-monitor")]
        if not self.tokens:
            producers.append(
                asyncio.create_task(
                    self.discovery_retry_loop(), name="market-discovery-retry"
                )
            )
        elif self.settings.mode == "fixture":
            producers.append(asyncio.create_task(self.fixture_loop(), name="fixture-feed"))
        else:
            producers.extend(
                [
                    asyncio.create_task(self.websocket_loop(), name="market-websocket"),
                    asyncio.create_task(
                        self.trade_reconcile_loop(), name="trade-reconciliation"
                    ),
                    asyncio.create_task(self.book_fallback_loop(), name="book-fallback"),
                    asyncio.create_task(
                        self.market_refresh_loop(), name="market-refresh"
                    ),
                ]
            )
        stop_waiter = asyncio.create_task(self.stop.wait(), name="shutdown-signal")
        failure: BaseException | None = None
        try:
            done, _ = await asyncio.wait(
                [stop_waiter, writer, *producers],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                if task is stop_waiter:
                    continue
                exception = task.exception()
                if exception is not None:
                    failure = exception
                    break
                if not self.stop.is_set():
                    failure = RuntimeError(
                        f"collector task {task.get_name()} stopped unexpectedly"
                    )
                    break
        finally:
            self.stop.set()
            for task in producers:
                task.cancel()
            await asyncio.gather(*producers, return_exceptions=True)
            drained = False
            if not writer.done():
                try:
                    await asyncio.wait_for(asyncio.shield(self.queue.join()), timeout=30)
                    drained = True
                except asyncio.TimeoutError:
                    log(
                        "shutdown_drain_timeout",
                        queued_rows=self._queued_rows,
                        action="REST reconciliation will recover trades after restart",
                    )
            self._writer_stop.set()
            if not writer.done():
                if drained:
                    writer.cancel()
                else:
                    try:
                        await asyncio.wait_for(writer, timeout=20)
                    except asyncio.TimeoutError:
                        writer.cancel()
                await asyncio.gather(writer, return_exceptions=True)
            stop_waiter.cancel()
            await asyncio.gather(stop_waiter, return_exceptions=True)
        if failure is not None:
            raise failure

    async def discovery_retry_loop(self) -> None:
        while not self.stop.is_set():
            await self.sleep(60)
            try:
                self.tokens = await self.api.discover(self.settings.market_count)
            except Exception as exc:  # noqa: BLE001 - loop must survive source failure
                self._source_error("gamma_discovery", exc)
                continue
            if self.tokens:
                log("markets_discovered", action="restart collector to subscribe")
                self.stop.set()

    async def market_refresh_loop(self) -> None:
        current = {(token.condition_id, token.token_id) for token in self.tokens}
        while not self.stop.is_set():
            await self.sleep(MARKET_REFRESH_SECONDS)
            try:
                refreshed = await self.api.discover(self.settings.market_count)
            except (
                SourceError,
                aiohttp.ClientError,
                OSError,
                asyncio.TimeoutError,
            ) as exc:
                self._source_error("gamma_refresh", exc)
                continue
            selected = {(token.condition_id, token.token_id) for token in refreshed}
            if selected and selected != current:
                log(
                    "watched_markets_changed",
                    action="restart collector with the refreshed market set",
                )
                self.stop.set()
                return

    async def enqueue(
        self,
        table: str,
        rows: list[dict[str, Any]],
        columns: list[str],
        id_field: str,
        checkpoints: dict[str, datetime] | None = None,
    ) -> None:
        if not rows:
            self.checkpoints.update(checkpoints or {})
            return
        ids = [str(row[id_field]) for row in rows]
        fresh_ids = self.dedupe.reserve(ids)
        if not fresh_ids:
            self.checkpoints.update(checkpoints or {})
            return
        allowed = set(fresh_ids)
        included: set[str] = set()
        fresh_rows = []
        for row in rows:
            row_id = str(row[id_field])
            if row_id in allowed and row_id not in included:
                fresh_rows.append(row)
                included.add(row_id)
        chunks = [
            fresh_rows[index : index + self.settings.queue_capacity]
            for index in range(0, len(fresh_rows), self.settings.queue_capacity)
        ]
        queued_ids: set[str] = set()
        try:
            for index, chunk in enumerate(chunks):
                chunk_ids = [str(row[id_field]) for row in chunk]
                if not await self._reserve_capacity(len(chunk)):
                    break
                batch = Batch(
                    table=table,
                    rows=chunk,
                    columns=columns,
                    ids=chunk_ids,
                    token=stable_batch_token(chunk_ids),
                    checkpoints=(checkpoints or {}) if index == len(chunks) - 1 else {},
                )
                await self.queue.put(batch)
                queued_ids.update(chunk_ids)
                if table == "price_ticks":
                    self._last_tick_enqueued_at = datetime.now(UTC)
        finally:
            self.dedupe.release([item_id for item_id in fresh_ids if item_id not in queued_ids])

    async def _reserve_capacity(self, rows: int) -> bool:
        async with self._capacity_changed:
            while (
                self._queued_rows + rows > self.settings.queue_capacity
                and not self.stop.is_set()
            ):
                self.health.status = "unhealthy"
                self.health.reason = "queue_full"
                await self._capacity_changed.wait()
            if self.stop.is_set():
                return False
            self._queued_rows += rows
            self.health.queue_depth = self._queued_rows
            if self._queued_rows >= min(8_000, self.settings.queue_capacity):
                self.health.status = "unhealthy"
                self.health.reason = "queue_near_capacity"
            return True

    async def _release_capacity(self, rows: int) -> None:
        async with self._capacity_changed:
            self._queued_rows = max(0, self._queued_rows - rows)
            self.health.queue_depth = self._queued_rows
            self._capacity_changed.notify_all()

    async def writer_loop(self) -> None:
        deferred: Batch | None = None
        while not self._writer_stop.is_set() or not self.queue.empty() or deferred:
            first = deferred if deferred is not None else await self.queue.get()
            deferred = None
            batches = [first]
            rows = len(first.rows)
            await self.sleep(WRITE_COALESCE_SECONDS)
            while rows < WRITE_BATCH_ROWS:
                try:
                    candidate = self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if (
                    candidate.table != first.table
                    or candidate.columns != first.columns
                    or rows + len(candidate.rows) > WRITE_BATCH_ROWS
                ):
                    deferred = candidate
                    break
                batches.append(candidate)
                rows += len(candidate.rows)
            batch = self._combine_batches(batches)
            delay = 1
            while not self._writer_stop.is_set():
                try:
                    await self.storage.insert(
                        batch.table, batch.rows, batch.columns, batch.token
                    )
                except Exception as exc:  # noqa: BLE001 - retries preserve batch
                    self.health.clickhouse_errors_total += 1
                    now = datetime.now(UTC)
                    self._write_failure_started_at = self._write_failure_started_at or now
                    failed_for = (now - self._write_failure_started_at).total_seconds()
                    self.health.status = "unhealthy" if failed_for >= 60 else "degraded"
                    self.health.reason = (
                        "clickhouse_write_stalled"
                        if failed_for >= 60
                        else "clickhouse_write_retrying"
                    )
                    log(
                        "clickhouse_write_failed",
                        error=str(exc),
                        table=batch.table,
                        retry_seconds=delay,
                    )
                    await self.sleep(delay)
                    delay = min(30, delay * 2)
                    continue
                self.dedupe.commit(batch.ids)
                self.checkpoints.update(batch.checkpoints)
                self.health.last_clickhouse_write_at = datetime.now(UTC)
                self._write_failure_started_at = None
                await self._release_capacity(len(batch.rows))
                self._refresh_health(datetime.now(UTC))
                break
            for _ in batches:
                self.queue.task_done()

    @staticmethod
    def _combine_batches(batches: list[Batch]) -> Batch:
        if len(batches) == 1:
            return batches[0]
        rows = [row for batch in batches for row in batch.rows]
        ids = [item_id for batch in batches for item_id in batch.ids]
        checkpoints: dict[str, datetime] = {}
        for batch in batches:
            checkpoints.update(batch.checkpoints)
        return Batch(
            table=batches[0].table,
            rows=rows,
            columns=batches[0].columns,
            ids=ids,
            token=stable_batch_token(ids),
            checkpoints=checkpoints,
        )

    async def health_loop(self) -> None:
        while not self.stop.is_set():
            await self.sleep(5)
            now = datetime.now(UTC)
            self._refresh_health(now)
            if self.settings.mode != "live":
                continue

            startup_age = (now - self.health.started_at).total_seconds()
            tick_age = (
                (now - self._last_tick_enqueued_at).total_seconds()
                if self._last_tick_enqueued_at
                else startup_age
            )
            notice_age = (
                (now - self._quiet_notice_at).total_seconds()
                if self._quiet_notice_at
                else 61
            )
            if tick_age >= 60 and notice_age >= 60:
                self._quiet_notice_at = now
                log(
                    "quiet_feed",
                    action=(
                        "Set POLYMARKET_MODE=fixture in .env.polymarket and run "
                        "docker compose --env-file .env.polymarket up -d --force-recreate collector"
                    ),
                )

    def _refresh_health(self, now: datetime) -> None:
        if self._write_failure_started_at is not None:
            failed_for = (now - self._write_failure_started_at).total_seconds()
            self.health.status = "unhealthy" if failed_for >= 60 else "degraded"
            self.health.reason = (
                "clickhouse_write_stalled"
                if failed_for >= 60
                else "clickhouse_write_retrying"
            )
            return
        if self._queued_rows >= min(8_000, self.settings.queue_capacity):
            self.health.status = "unhealthy"
            self.health.reason = "queue_near_capacity"
            return
        if self.settings.mode == "fixture":
            self.health.status = "fixture"
            self.health.reason = "ready"
            return
        if not self.tokens:
            self.health.status = "degraded"
            self.health.reason = "no_active_markets"
            return

        startup_age = (now - self.health.started_at).total_seconds()
        trade_age = (
            (now - self.health.last_trade_reconcile_at).total_seconds()
            if self.health.last_trade_reconcile_at
            else startup_age
        )
        price_limit = max(60, self.settings.book_fallback_seconds * 3)
        websocket_limit = max(60, self.settings.stall_seconds * 2)
        self.health.fresh_tokens = sum(
            (now - observed_at).total_seconds() <= price_limit
            for observed_at in self._token_price_at.values()
        )
        self.health.websocket_fresh_tokens = sum(
            (now - observed_at).total_seconds() <= websocket_limit
            for observed_at in self._token_websocket_at.values()
        )
        price_fresh = (
            self.health.watched_tokens > 0
            and self.health.fresh_tokens == self.health.watched_tokens
        )
        websocket_fresh = (
            self._ws_healthy
            and self.health.websocket_fresh_tokens == self.health.watched_tokens
        )
        trades_fresh = trade_age <= max(60, self.settings.reconcile_seconds * 3)
        if not price_fresh and not trades_fresh:
            self.health.status = "unhealthy"
            self.health.reason = "all_sources_stale"
        elif not price_fresh:
            self.health.status = "degraded"
            self.health.reason = "price_sources_stale"
        elif not trades_fresh:
            self.health.status = "degraded"
            self.health.reason = "trade_reconciliation_stale"
        elif not websocket_fresh:
            self.health.status = "degraded"
            self.health.reason = "websocket_stale_rest_active"
        else:
            self.health.status = "live"
            self.health.reason = "sources_fresh"

    async def websocket_loop(self) -> None:
        delay = 1
        token_ids = [token.token_id for token in self.tokens]
        while not self.stop.is_set():
            self.health.websocket = "connecting"
            try:
                async for message in self.api.websocket_messages(
                    token_ids, self.settings.stall_seconds
                ):
                    self._ws_healthy = True
                    self.health.websocket = "connected"
                    self.health.last_websocket_event_at = datetime.now(UTC)
                    self._refresh_health(datetime.now(UTC))
                    try:
                        rows = normalize_ws_message(message)
                    except (KeyError, TypeError, ValueError) as exc:
                        self._parse_error("websocket", message, exc)
                        continue
                    rows = self._filter_watched(rows, "websocket")
                    await self.enqueue("price_ticks", rows, TICK_COLUMNS, "event_id")
                    delay = 1
            except (
                SourceError,
                aiohttp.ClientError,
                OSError,
                asyncio.TimeoutError,
            ) as exc:
                self._ws_healthy = False
                self.health.websocket = "retrying"
                self._refresh_health(datetime.now(UTC))
                self._source_error("websocket", exc)
                await self.sleep(delay)
                delay = min(30, delay * 2)

    async def trade_reconcile_loop(self) -> None:
        conditions = sorted({token.condition_id for token in self.tokens})
        while not self.stop.is_set():
            now = datetime.now(UTC)
            results = await asyncio.gather(
                *(self._reconcile_condition(condition_id, now) for condition_id in conditions)
            )
            if results and all(results):
                self.health.last_trade_reconcile_at = datetime.now(UTC)
                self._refresh_health(datetime.now(UTC))
            await self.sleep(self.settings.reconcile_seconds)

    async def _reconcile_condition(
        self, condition_id: str, now: datetime
    ) -> bool:
        checkpoint = self.checkpoints.get(
            condition_id,
            now - timedelta(minutes=self.settings.initial_lookback_minutes),
        )
        start = int(checkpoint.timestamp()) - 5
        try:
            async with self._source_slots:
                async with asyncio.timeout(
                    max(RECONCILE_DEADLINE_SECONDS, self.settings.reconcile_seconds)
                ):
                    payloads = await self.api.trades(
                        condition_id, start, int(now.timestamp())
                    )
        except RateLimited as exc:
            self._source_error("data_api", exc)
            await self.sleep(exc.retry_after)
            return False
        except (
            KeyError,
            TypeError,
            ValueError,
            SourceError,
            aiohttp.ClientError,
            OSError,
            asyncio.TimeoutError,
        ) as exc:
            self._source_error("data_api", exc)
            return False

        rows = []
        for payload in payloads:
            try:
                row = normalize_trade(payload)
            except (KeyError, TypeError, ValueError) as exc:
                self._parse_error("data_api", payload, exc)
                continue
            rows.extend(self._filter_watched([row], "data_api"))
        if payloads and not rows:
            return False
        rows.sort(key=lambda row: row["event_at"])
        next_checkpoint = max((row["event_at"] for row in rows), default=now)
        await self.enqueue(
            "trades",
            rows,
            TRADE_COLUMNS,
            "trade_id",
            {condition_id: next_checkpoint},
        )
        return True

    async def book_fallback_loop(self) -> None:
        token_conditions = {token.token_id: token.condition_id for token in self.tokens}
        while not self.stop.is_set():
            await self.sleep(self.settings.book_fallback_seconds)
            now = datetime.now(UTC)
            stale_tokens = {
                token_id: condition_id
                for token_id, condition_id in token_conditions.items()
                if token_id not in self._token_price_at
                or (now - self._token_price_at[token_id]).total_seconds()
                >= self.settings.book_fallback_seconds
            }
            if not stale_tokens:
                continue
            fetched = await asyncio.gather(
                *(
                    self._fetch_book(token_id, condition_id)
                    for token_id, condition_id in stale_tokens.items()
                )
            )
            rows = [row for row in fetched if row is not None]
            await self.enqueue("price_ticks", rows, TICK_COLUMNS, "event_id")
            if rows:
                self.health.last_book_fallback_at = datetime.now(UTC)
                self._refresh_health(datetime.now(UTC))

    async def _fetch_book(
        self, token_id: int, condition_id: str
    ) -> dict[str, Any] | None:
        try:
            async with self._source_slots:
                async with asyncio.timeout(max(10, self.settings.book_fallback_seconds)):
                    payload = await self.api.book(token_id)
            row = normalize_book(payload, condition_id)
            accepted = self._filter_watched([row], "clob_book")
            if accepted and Decimal(str(accepted[0]["midpoint"])) > 0:
                return accepted[0]
            if accepted:
                self._parse_error(
                    "clob_book",
                    accepted[0],
                    ValueError("book has no usable bid/ask midpoint"),
                )
            return None
        except RateLimited as exc:
            self._source_error("clob_book", exc)
            await self.sleep(exc.retry_after)
        except (
            KeyError,
            TypeError,
            ValueError,
            SourceError,
            aiohttp.ClientError,
            OSError,
            asyncio.TimeoutError,
        ) as exc:
            self._source_error("clob_book", exc)
        return None

    async def fixture_loop(self) -> None:
        sequence = 0
        while not self.stop.is_set():
            now = datetime.now(UTC)
            rows = fixture_ticks(self.tokens, sequence, now)
            trades = fixture_trades(self.tokens, sequence, now)
            await self.enqueue("price_ticks", rows, TICK_COLUMNS, "event_id")
            await self.enqueue("trades", trades, TRADE_COLUMNS, "trade_id")
            self.health.last_websocket_event_at = now
            self.health.last_trade_reconcile_at = now
            self.health.fresh_tokens = len(self.tokens)
            sequence += 1
            await self.sleep(5)

    def _source_error(self, source: str, exc: Exception) -> None:
        self.health.source_errors_total += 1
        log("source_error", source=source, error=str(exc)[:200])

    def _filter_watched(
        self, rows: list[dict[str, Any]], source: str
    ) -> list[dict[str, Any]]:
        token_conditions = {
            token.token_id: token.condition_id for token in self.tokens
        }
        accepted = []
        for row in rows:
            condition_id = token_conditions.get(row.get("token_id"))
            if condition_id == row.get("condition_id"):
                accepted.append(row)
                continue
            self._parse_error(
                source,
                row,
                ValueError("event identifiers are outside the watched market set"),
            )
        if source in {"websocket", "clob_book"}:
            observed_at = datetime.now(UTC)
            for row in accepted:
                if Decimal(str(row.get("midpoint", "0"))) <= 0:
                    continue
                token_id = row["token_id"]
                self._token_price_at[token_id] = observed_at
                if source == "websocket":
                    self._token_websocket_at[token_id] = observed_at
        return accepted

    def _parse_error(self, source: str, payload: Any, exc: Exception) -> None:
        self.health.source_parse_errors_total += 1
        preview = json.dumps(payload, default=str)[:200]
        log("source_parse_error", source=source, error=str(exc), preview=preview)


def fixture_markets(count: int) -> list[MarketToken]:
    rows = []
    for index in range(count):
        condition = "0x" + f"{index + 1:064x}"
        for outcome_index, outcome in enumerate(("Yes", "No")):
            rows.append(
                MarketToken(
                    market_id=9_000_000 + index,
                    condition_id=condition,
                    token_id=10_000_000 + index * 2 + outcome_index,
                    outcome=outcome,
                    question=f"Fixture market {index + 1}: will the signal move?",
                    slug=f"fixture-market-{index + 1}",
                    active=True,
                    accepting_orders=False,
                    volume_24h="1000",
                )
            )
    return rows


def fixture_ticks(
    tokens: list[MarketToken], sequence: int, now: datetime
) -> list[dict[str, Any]]:
    rows = []
    for token in tokens:
        base = 40 + ((sequence + token.market_id) % 20)
        bid = f"0.{base:02d}"
        ask = f"0.{base + 2:02d}"
        rows.append(
            build_tick(
                event_kind="best_bid_ask",
                source="FIXTURE",
                condition_id=token.condition_id,
                token_id=token.token_id,
                timestamp=now.isoformat(),
                best_bid=bid,
                best_ask=ask,
                source_hash=f"fixture-{sequence}-{token.token_id}",
                raw={"fixture": True, "sequence": sequence},
            )
        )
    return rows


def fixture_trades(
    tokens: list[MarketToken], sequence: int, now: datetime
) -> list[dict[str, Any]]:
    rows = []
    for token in tokens[::2]:
        price = 0.4 + ((sequence + token.market_id) % 20) / 100
        rows.append(
            normalize_trade(
                {
                    "transactionHash": "0x" + f"{sequence * 100 + token.market_id:064x}"[-64:],
                    "asset": str(token.token_id),
                    "proxyWallet": "0x" + "1" * 40,
                    "side": "BUY" if sequence % 2 == 0 else "SELL",
                    "price": str(price),
                    "size": str(10 + sequence % 5),
                    "timestamp": int(now.timestamp()),
                    "conditionId": token.condition_id,
                    "outcome": token.outcome,
                    "title": token.question,
                }
            )
        )
    return rows
