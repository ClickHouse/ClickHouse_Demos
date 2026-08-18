"""Offline unit tests for writer.py.

Everything here runs with no database, no network and no real sleeping. The two
pieces of logic that can be silently wrong are the rate pacer and the window
accounting, so both are driven with explicit fake timestamps rather than
time.perf_counter(): a test that reads the real clock cannot assert an exact
schedule, which is the only thing worth asserting about a pacer.

Rates are chosen so the interval is exactly representable in binary (rate 4 ->
0.25) wherever a test asserts an exact delay. Where a rate like 100 is used for
realism, the assertions go through pytest.approx.
"""

from __future__ import annotations

import asyncio
import json
import time

import pytest

import writer
from writer import Pacer, Stats, parse_args, percentile, sleep_or_stop


# --------------------------------------------------------------------------
# percentile
# --------------------------------------------------------------------------


def test_percentile_of_empty_list_is_zero() -> None:
    # An empty window is normal: it is what a report prints before the first
    # commit lands, so this must be a number and not an IndexError.
    assert percentile([], 0.95) == 0.0
    assert percentile([], 0.5) == 0.0


def test_percentile_of_known_list() -> None:
    values = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]

    # index = min(n - 1, int(n * fraction)): int(10 * 0.95) == 9 -> ordered[9],
    # int(10 * 0.5) == 5 -> ordered[5]. This is nearest-rank rounding up, not
    # interpolation, which is what a load generator wants: every reported value
    # is a latency that was actually measured.
    assert percentile(values, 0.95) == 100.0
    assert percentile(values, 0.5) == 60.0


def test_percentile_sorts_before_indexing() -> None:
    shuffled = [50.0, 10.0, 100.0, 30.0, 90.0, 20.0, 80.0, 40.0, 70.0, 60.0]

    assert percentile(shuffled, 0.95) == 100.0
    assert percentile(shuffled, 0.5) == 60.0


def test_percentile_of_single_element_list() -> None:
    assert percentile([42.5], 0.95) == 42.5
    assert percentile([42.5], 0.0) == 42.5
    assert percentile([42.5], 1.0) == 42.5


def test_percentile_clamps_the_top_index() -> None:
    # fraction 1.0 would index one past the end without the min().
    assert percentile([1.0, 2.0, 3.0], 1.0) == 3.0


def test_percentile_index_on_an_exact_multiple() -> None:
    # A known edge, pinned so that changing it is a decision and not a surprise:
    # when n * fraction is a whole number, int() lands one sample ABOVE the
    # nearest-rank value -- p50 of four samples returns the 3rd, not the 2nd, and
    # p95 of twenty returns the max. Harmless for a ten-second window holding
    # thousands of samples, and the reported number is always a latency that was
    # really measured, but do not "fix" it casually: bench/run.py compares p95
    # across runs, so redefining it silently makes old and new runs
    # incomparable.
    assert percentile([1.0, 2.0, 3.0, 4.0], 0.5) == 3.0
    assert percentile([float(n) for n in range(1, 21)], 0.95) == 20.0


def test_percentile_does_not_mutate_its_input() -> None:
    # percentile is called on the list take_window() just drained, and in the
    # final line on the tail window. Sorting in place would reorder samples the
    # caller may still hold a reference to.
    values = [30.0, 10.0, 20.0]
    original = list(values)

    percentile(values, 0.95)

    assert values == original


# --------------------------------------------------------------------------
# Pacer
# --------------------------------------------------------------------------


def test_pacer_interval_is_the_reciprocal_of_rate() -> None:
    assert Pacer(4).interval == 0.25
    assert Pacer(200).interval == pytest.approx(0.005)


def test_pacer_first_claim_fires_immediately() -> None:
    pacer = Pacer(4)

    assert pacer.claim(100.0) == 0.0
    assert pacer.restarts == 0


def test_pacer_throttles_a_caller_that_never_waits() -> None:
    # A caller that ignores the returned delay (or a burst of workers claiming in
    # the same event-loop turn) must be handed a growing delay, not zero. If this
    # ever returns 0.0 twice at the same timestamp, --rate has stopped meaning
    # anything and the workers spin.
    pacer = Pacer(4)

    delays = [pacer.claim(100.0) for _ in range(5)]

    assert delays == [0.0, 0.25, 0.5, 0.75, 1.0]
    assert pacer.restarts == 0


def test_pacer_holds_the_target_rate_for_an_instant_worker() -> None:
    # Simulate a worker whose transaction takes no time: it sleeps exactly the
    # delay it was told and immediately claims again. After N claims the clock
    # must have advanced (N - 1) intervals, i.e. exactly `rate` claims per second
    # with no drift.
    pacer = Pacer(4)
    clock = 0.0

    for _ in range(9):
        clock += pacer.claim(clock)

    assert clock == pytest.approx(2.0)  # 8 intervals of 0.25
    assert pacer.restarts == 0


def test_pacer_schedule_does_not_drift_with_slow_work() -> None:
    # The schedule is absolute, so a transaction that overruns its slot does not
    # push every later slot back: the 3rd slot is still at 0.50 even though the
    # 2nd claim happened at 0.40.
    pacer = Pacer(4)

    assert pacer.claim(0.0) == 0.0  # slot at 0.00
    assert pacer.claim(0.40) == 0.0  # slot at 0.25, already overdue
    assert pacer.claim(0.40) == pytest.approx(0.10)  # slot at 0.50


def test_pacer_credit_is_bounded_by_the_lag_it_absorbs() -> None:
    # Within max_lag the backlog is honoured, which is what keeps the offered
    # load at --rate on average. But it is bounded: a 1.0 s lag at rate 4 buys
    # exactly 4 catch-up slots, and then the caller is throttled again. This is
    # the property that stops a slow caller accumulating unbounded credit.
    pacer = Pacer(4, max_lag_seconds=1.0)
    pacer.claim(0.0)

    delays = [pacer.claim(1.0) for _ in range(6)]

    assert delays == [0.0, 0.0, 0.0, 0.0, 0.25, 0.5]
    assert pacer.restarts == 0


def test_pacer_discards_the_backlog_after_a_long_stall() -> None:
    # A 10 s stall at rate 100 is a 1000-transaction backlog. Firing it would
    # turn a rate-limited run into a max-throughput run, so the schedule restarts
    # from now: exactly one immediate slot, then the normal interval.
    pacer = Pacer(100, max_lag_seconds=0.5)
    pacer.claim(0.0)

    delays = [pacer.claim(10.0) for _ in range(4)]

    assert delays[0] == 0.0
    assert delays[1:] == pytest.approx([0.01, 0.02, 0.03])
    assert pacer.restarts == 1


def test_pacer_restart_is_counted_once_per_stall() -> None:
    pacer = Pacer(100, max_lag_seconds=0.5)
    pacer.claim(0.0)

    pacer.claim(10.0)  # first stall
    for _ in range(20):
        pacer.claim(10.0)  # caught up now, no further restarts
    assert pacer.restarts == 1

    pacer.claim(30.0)  # a second stall
    assert pacer.restarts == 2


def test_pacer_restart_threshold_is_strict() -> None:
    # `next_at < now - max_lag_seconds`: a schedule exactly max_lag behind is
    # still absorbed rather than restarted, so the counter reported on stderr
    # means "the database really fell behind", not "a slot landed on the
    # boundary".
    on_boundary = Pacer(1, max_lag_seconds=1.0)
    on_boundary.claim(0.0)  # next_at = 1.0
    on_boundary.claim(2.0)  # 1.0 < 1.0 is False
    assert on_boundary.restarts == 0

    past_boundary = Pacer(1, max_lag_seconds=1.0)
    past_boundary.claim(0.0)  # next_at = 1.0
    past_boundary.claim(2.5)  # 1.0 < 1.5 is True
    assert past_boundary.restarts == 1


def test_pacer_never_returns_a_negative_delay() -> None:
    # The delay is passed straight to sleep_or_stop, where a negative value would
    # mean "check the stop flag and go", so the clamp is what keeps an overdue
    # slot from being confused with a stop signal.
    pacer = Pacer(4)
    pacer.claim(0.0)

    assert pacer.claim(100.0) == 0.0


# --------------------------------------------------------------------------
# Stats.take_window
# --------------------------------------------------------------------------


def test_take_window_of_an_untouched_stats_is_empty() -> None:
    assert Stats().take_window() == (0, 0, [])


def test_consecutive_windows_report_disjoint_counts() -> None:
    stats = Stats()
    stats.record_commit(1.0)
    stats.record_commit(2.0)
    stats.record_failure()

    first = stats.take_window()

    stats.record_commit(3.0)
    stats.record_failure()
    stats.record_failure()

    second = stats.take_window()

    assert first == (2, 1, [1.0, 2.0])
    assert second == (1, 2, [3.0])


def test_a_commit_between_windows_is_credited_to_the_second() -> None:
    # This is the case the take_window() docstring promises: a transaction that
    # commits after the reporter has taken its window must show up in the next
    # window, never be dropped.
    stats = Stats()
    stats.take_window()

    stats.record_commit(7.5)

    assert stats.take_window() == (1, 0, [7.5])


def test_an_empty_window_between_two_busy_ones_loses_nothing() -> None:
    stats = Stats()
    stats.record_commit(1.0)

    assert stats.take_window() == (1, 0, [1.0])
    assert stats.take_window() == (0, 0, [])

    stats.record_commit(2.0)

    assert stats.take_window() == (1, 0, [2.0])
    assert stats.committed == 2


def test_latencies_are_drained_per_window() -> None:
    stats = Stats()
    stats.record_commit(1.0)

    drained = stats.take_window()[2]

    # The reporter keeps the returned list while workers keep recording; the two
    # must not be the same object, or the next window's samples would land in a
    # list that has already been percentiled.
    assert drained is not stats.latencies
    assert stats.latencies == []

    stats.record_commit(2.0)

    assert drained == [1.0]
    assert stats.latencies == [2.0]


def test_failures_are_counted_separately_from_commits() -> None:
    stats = Stats()
    stats.record_failure()
    stats.record_failure()

    committed, failed, latencies = stats.take_window()

    assert (committed, failed) == (0, 2)
    # A failed order has no latency sample: the transaction did not complete, so
    # timing it would drag p95 towards whatever the error path costs.
    assert latencies == []


def test_lifetime_counters_survive_windowing() -> None:
    # The final line prints stats.committed/stats.failed, so taking a window must
    # not reset them -- only the reporting cursor moves.
    stats = Stats()
    for _ in range(3):
        stats.record_commit(1.0)
        stats.record_failure()
    stats.take_window()
    stats.record_commit(1.0)
    stats.take_window()

    assert stats.committed == 4
    assert stats.failed == 3


# --------------------------------------------------------------------------
# sleep_or_stop
# --------------------------------------------------------------------------


def test_sleep_or_stop_with_no_delay_reports_the_flag() -> None:
    async def scenario() -> tuple[bool, bool]:
        running = asyncio.Event()
        stopped = asyncio.Event()
        stopped.set()
        return (
            await sleep_or_stop(running, 0.0),
            await sleep_or_stop(stopped, 0.0),
        )

    assert asyncio.run(scenario()) == (False, True)


def test_sleep_or_stop_returns_at_once_when_already_stopping() -> None:
    async def scenario() -> bool:
        stopping = asyncio.Event()
        stopping.set()
        return await sleep_or_stop(stopping, 30.0)

    started = time.monotonic()
    assert asyncio.run(scenario()) is True
    # A 30 s delay must not be waited out when the stop flag is already set.
    assert time.monotonic() - started < 1.0


def test_sleep_or_stop_returns_false_on_timeout() -> None:
    # The one place a real wait happens, deliberately at 10 ms: this is the
    # normal path in production (the interval elapsed, nobody signalled) and the
    # only way to cover the TimeoutError branch.
    async def scenario() -> bool:
        return await sleep_or_stop(asyncio.Event(), 0.01)

    assert asyncio.run(scenario()) is False


# --------------------------------------------------------------------------
# report(): the interval-line output contract bench/run.py parses
# --------------------------------------------------------------------------


def test_report_prints_the_documented_interval_line(monkeypatch, capsys) -> None:
    stats = Stats()
    stats.record_commit(10.0)
    stats.record_commit(20.0)
    stats.record_failure()

    slept: list[float] = []

    async def fake_sleep_or_stop(stopping: asyncio.Event, delay: float) -> bool:
        # Stands in for the interval wait, so no test spends real seconds. A
        # commit is recorded during the second wait to prove the second printed
        # window is the one that credits it.
        slept.append(delay)
        await asyncio.sleep(0)
        if len(slept) == 2:
            stats.record_commit(30.0)
        return len(slept) > 2

    monkeypatch.setattr(writer, "sleep_or_stop", fake_sleep_or_stop)

    asyncio.run(writer.report(stats, asyncio.Event(), 10))

    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]

    assert slept == [10.0, 10.0, 10.0]  # --report-seconds is what it waits on
    assert len(lines) == 2
    # The harness reads exactly these keys, and treats any line carrying "tps" as
    # a throughput sample.
    for line in lines:
        assert set(line) == {"committed", "failed", "tps", "p95_ms"}
    assert (lines[0]["committed"], lines[0]["failed"]) == (2, 1)
    assert lines[0]["p95_ms"] == 20.0
    assert (lines[1]["committed"], lines[1]["failed"]) == (1, 0)
    assert lines[1]["p95_ms"] == 30.0


def test_report_stops_without_printing_a_partial_window(monkeypatch, capsys) -> None:
    # On the stop signal the reporter returns and lets run() print the final
    # line. Printing a truncated window here would give the harness a low
    # throughput sample to take into its median.
    stats = Stats()
    stats.record_commit(5.0)

    async def stop_immediately(stopping: asyncio.Event, delay: float) -> bool:
        return True

    monkeypatch.setattr(writer, "sleep_or_stop", stop_immediately)

    asyncio.run(writer.report(stats, asyncio.Event(), 10))

    assert capsys.readouterr().out == ""
    # Nothing was consumed, so the final line still sees the sample.
    assert stats.take_window() == (1, 0, [5.0])


# --------------------------------------------------------------------------
# parse_args: the CLI contract bench/run.py builds command lines against
# --------------------------------------------------------------------------


def test_parse_args_defaults() -> None:
    args = parse_args(["--dsn", "postgres://shop_writer@localhost/shop"])

    assert args.dsn == "postgres://shop_writer@localhost/shop"
    assert args.rate == 200
    assert args.concurrency == 16
    assert args.report_seconds == 10


def test_parse_args_accepts_every_documented_flag() -> None:
    args = parse_args(
        [
            "--dsn",
            "postgres://shop_writer@localhost/shop",
            "--rate",
            "500",
            "--concurrency",
            "32",
            "--report-seconds",
            "5",
        ]
    )

    assert (args.rate, args.concurrency, args.report_seconds) == (500, 32, 5)


def test_parse_args_requires_a_dsn() -> None:
    with pytest.raises(SystemExit):
        parse_args([])


@pytest.mark.parametrize("flag", ["--rate", "--concurrency", "--report-seconds"])
@pytest.mark.parametrize("value", ["0", "-1"])
def test_parse_args_rejects_non_positive_tuning(flag: str, value: str) -> None:
    # rate 0 would be a ZeroDivisionError in Pacer, concurrency 0 an empty pool,
    # report-seconds 0 a reporter that never yields.
    with pytest.raises(SystemExit):
        parse_args(["--dsn", "postgres://x/y", flag, value])
