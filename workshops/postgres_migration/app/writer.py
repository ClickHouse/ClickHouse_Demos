"""Continuous OLTP order writer.

Emits one JSON object per line on stdout, one line per report interval, so the
benchmark harness can read throughput out of a pipe instead of out of a shared
database -- if the harness had to query a counter table it would be adding load to
the very thing it is measuring. Rate is a target, not a guarantee: when the
database cannot keep up, TPS falls, which is the entire point of the measurement.

Every order is one transaction -- one `orders` row, then one to four `order_items`
rows. That shape is what makes writer TPS a meaningful contention measurement: a
single-row insert would rarely wait on the dashboard's scans, and a bulk COPY would
not resemble a checkout path at all.

Output contract, relied on by `bench/run.py`:
  - interval lines: {"committed": N, "failed": N, "tps": F, "p95_ms": F}, flushed
    immediately, counts and latencies scoped to that window
  - one final line on SIGTERM or SIGINT, after in-flight transactions drain:
    {"final": true, "committed": N, "failed": N, "p95_ms": F, "elapsed_seconds": F},
    where "committed" and "failed" are lifetime counts but "p95_ms" covers only the
    final partial window, since every earlier window's latencies were drained by the
    line that reported them. It deliberately carries no "tps" key, because the harness
    treats every line that has one as a throughput sample to be taken into a median.
Anything that is not part of that contract goes to stderr.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import signal
import sys
import time

INSERT_ORDER = """
INSERT INTO orders (customer_id, status, placed_at, updated_at)
VALUES ($1, 'placed', now(), now())
RETURNING order_id
"""

INSERT_ITEM = """
INSERT INTO order_items (order_id, product_id, quantity, line_total, placed_at)
VALUES ($1, $2, $3, $4, now())
"""


class Stats:
    """Lifetime counters plus the latency samples of the window in progress.

    Exactly one instance lives for the whole run and is never replaced. The reporter
    takes a window out of it by differencing the totals and draining the latency
    list. Replacing the object instead would silently drop every order that was
    in flight at report time: those coroutines hold a reference to the old object,
    so their increments would land on something nobody ever prints.
    """

    def __init__(self) -> None:
        self.committed = 0
        self.failed = 0
        self.latencies: list[float] = []
        self._reported_committed = 0
        self._reported_failed = 0

    def record_commit(self, latency_ms: float) -> None:
        self.committed += 1
        self.latencies.append(latency_ms)

    def record_failure(self) -> None:
        self.failed += 1

    def take_window(self) -> tuple[int, int, list[float]]:
        """Return (committed, failed, latencies) since the previous call.

        Safe without a lock: there is no await between reading the counters and
        resetting the cursor, so the event loop cannot interleave a worker. A
        transaction that commits after this returns is credited to the next window
        rather than lost.
        """
        latencies = self.latencies
        self.latencies = []
        window = (
            self.committed - self._reported_committed,
            self.failed - self._reported_failed,
            latencies,
        )
        self._reported_committed = self.committed
        self._reported_failed = self.failed
        return window


class Pacer:
    """Hands out order start slots against one monotonic schedule.

    Every worker claims from the same schedule, so `--rate` is the aggregate offered
    load rather than a per-worker limit, and the interval between starts does not
    drift with how long each transaction took. Claiming needs no lock because
    `claim` never awaits: the loop cannot interleave a second worker between reading
    the clock and advancing `next_at`.

    When the database stalls, the schedule falls behind. Firing the whole backlog on
    recovery would offer far more than `--rate` and turn the measurement into a
    max-throughput run, so a schedule more than `max_lag_seconds` behind is restarted
    from now and the backlog is discarded.
    """

    def __init__(self, rate: int, max_lag_seconds: float = 1.0) -> None:
        self.interval = 1.0 / rate
        self.max_lag_seconds = max_lag_seconds
        self.next_at: float | None = None
        self.restarts = 0

    def claim(self, now: float) -> float:
        """Reserve the next start slot and return how long to wait for it."""
        if self.next_at is None:
            self.next_at = now
        elif self.next_at < now - self.max_lag_seconds:
            self.restarts += 1
            self.next_at = now
        delay = self.next_at - now
        self.next_at += self.interval
        return delay if delay > 0.0 else 0.0


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(len(ordered) * fraction))
    return ordered[index]


async def sleep_or_stop(stopping: asyncio.Event, delay: float) -> bool:
    """Wait `delay` seconds. Return True if the stop signal arrived first."""
    if delay <= 0.0:
        return stopping.is_set()
    try:
        await asyncio.wait_for(stopping.wait(), delay)
    except (asyncio.TimeoutError, TimeoutError):
        return False
    return True


async def place_order(pool, max_customer: int, max_product: int, stats: Stats, errors: list[str]) -> None:
    """One order, one transaction: the `orders` row plus its one to four items."""
    started = time.perf_counter()
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                order_id = await conn.fetchval(INSERT_ORDER, random.randint(1, max_customer))
                for _ in range(random.randint(1, 4)):
                    await conn.execute(
                        INSERT_ITEM,
                        order_id,
                        random.randint(1, max_product),
                        random.randint(1, 5),
                        round(random.uniform(1, 500), 2),
                    )
    except Exception as exc:  # a failed order is data, not a crash: it is what "failed" counts
        stats.record_failure()
        if not errors:
            # Report the first failure once, so a bad DSN or a missing grant is visible
            # instead of showing up only as a rising "failed" count.
            errors.append(f"{type(exc).__name__}: {exc}")
            print(f"writer: first failure: {errors[0]}", file=sys.stderr, flush=True)
        return
    stats.record_commit((time.perf_counter() - started) * 1000.0)


async def worker(
    pool,
    pacer: Pacer,
    stats: Stats,
    stopping: asyncio.Event,
    max_customer: int,
    max_product: int,
    errors: list[str],
) -> None:
    """One long-lived worker. `--concurrency` of these bound the in-flight orders."""
    while not stopping.is_set():
        if await sleep_or_stop(stopping, pacer.claim(time.perf_counter())):
            return
        await place_order(pool, max_customer, max_product, stats, errors)


async def report(stats: Stats, stopping: asyncio.Event, report_seconds: int) -> None:
    window_started = time.perf_counter()
    while not stopping.is_set():
        if await sleep_or_stop(stopping, report_seconds):
            return
        now = time.perf_counter()
        elapsed = now - window_started
        window_started = now
        committed, failed, latencies = stats.take_window()
        print(
            json.dumps(
                {
                    "committed": committed,
                    "failed": failed,
                    "tps": round(committed / elapsed, 1) if elapsed > 0 else 0.0,
                    "p95_ms": round(percentile(latencies, 0.95), 1),
                }
            ),
            flush=True,
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Continuous OLTP order writer. Prints one JSON line per report interval."
    )
    parser.add_argument("--dsn", required=True, help="postgres DSN for the shop_writer role")
    parser.add_argument("--rate", type=int, default=200, help="target orders per second (default: 200)")
    parser.add_argument(
        "--concurrency", type=int, default=16, help="in-flight transactions and pool size (default: 16)"
    )
    parser.add_argument(
        "--report-seconds", type=int, default=10, help="seconds between JSON lines (default: 10)"
    )
    args = parser.parse_args(argv)
    for name in ("rate", "concurrency", "report_seconds"):
        if getattr(args, name) < 1:
            parser.error(f"--{name.replace('_', '-')} must be at least 1")
    return args


def install_stop_handlers(stopping: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stopping.set)
        except (NotImplementedError, AttributeError):
            # Native Windows has no add_signal_handler. WSL, which is what the
            # workshop asks Windows learners to use, takes the branch above.
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stopping.set))


async def run(args: argparse.Namespace) -> int:
    try:
        import asyncpg
    except ModuleNotFoundError:
        print(
            "writer: asyncpg is not installed. Run: pip install -r requirements.txt",
            file=sys.stderr,
        )
        return 2

    pool = await asyncpg.create_pool(args.dsn, min_size=args.concurrency, max_size=args.concurrency)
    try:
        max_customer = await pool.fetchval("SELECT max(customer_id) FROM customers")
        max_product = await pool.fetchval("SELECT max(product_id) FROM products")
        if max_customer is None or max_product is None:
            print(
                "writer: customers or products is empty. Load sql/02_seed.sql first.",
                file=sys.stderr,
            )
            return 2

        stopping = asyncio.Event()
        install_stop_handlers(stopping)

        stats = Stats()
        pacer = Pacer(args.rate)
        errors: list[str] = []
        started = time.perf_counter()

        reporter = asyncio.create_task(report(stats, stopping, args.report_seconds))
        workers = [
            asyncio.create_task(
                worker(pool, pacer, stats, stopping, max_customer, max_product, errors)
            )
            for _ in range(args.concurrency)
        ]

        # The workers are the drain: each finishes the transaction it is inside and
        # then sees `stopping`, so nothing is cancelled mid-transaction and every
        # commit is counted before the final line is printed.
        await asyncio.gather(*workers)
        reporter.cancel()
        try:
            await reporter
        except asyncio.CancelledError:
            pass

        _, _, tail_latencies = stats.take_window()
        if pacer.restarts:
            print(
                f"writer: pacing schedule restarted {pacer.restarts} times; "
                "the database fell more than a second behind --rate",
                file=sys.stderr,
            )
        print(
            json.dumps(
                {
                    "final": True,
                    "committed": stats.committed,
                    "failed": stats.failed,
                    "p95_ms": round(percentile(tail_latencies, 0.95), 1),
                    "elapsed_seconds": round(time.perf_counter() - started, 1),
                }
            ),
            flush=True,
        )
        return 0
    finally:
        await pool.close()


def main() -> int:
    args = parse_args()
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    sys.exit(main())
