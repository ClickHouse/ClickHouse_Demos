"""Offline unit tests for run.py.

No database, no subprocess, no network, no real sleeping. Every timestamp is supplied
explicitly, because the two things that can be silently wrong here are the parsing of the
writer's stdout contract and which samples the headline TPS is a median of -- and a test
that reads the real clock cannot assert either.

The three pieces under test are the ones the workshop's conclusion depends on:
percentile (every latency figure), WriterMonitor (the headline TPS), and
build_result/render_markdown (whether a query that failed on one target is visible or
hidden).
"""

from __future__ import annotations

import asyncio
import json

import pytest

import run
from run import (
    DashboardStats,
    WriterMonitor,
    build_result,
    load_queries,
    parse_args,
    percentile,
    render_markdown,
)


# --------------------------------------------------------------------------
# percentile
# --------------------------------------------------------------------------


def test_percentile_of_empty_list_is_zero() -> None:
    # A query that never succeeded has no samples, and its row still has to render as a
    # number rather than raising IndexError halfway through writing the results file.
    assert percentile([], 0.5) == 0.0
    assert percentile([], 0.95) == 0.0


def test_percentile_is_nearest_rank_not_interpolated() -> None:
    values = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]

    # index = min(n - 1, int(n * fraction)): int(10 * 0.5) == 5 -> ordered[5],
    # int(10 * 0.95) == 9 -> ordered[9]. Every reported value is a latency that was
    # actually measured, and it matches app/writer.py's rule so the two agree.
    assert percentile(values, 0.50) == 60.0
    assert percentile(values, 0.95) == 100.0


def test_percentile_sorts_before_indexing() -> None:
    shuffled = [90.0, 10.0, 50.0, 100.0, 20.0, 70.0, 30.0, 80.0, 40.0, 60.0]
    assert percentile(shuffled, 0.95) == 100.0
    assert percentile(shuffled, 0.50) == 60.0


def test_percentile_clamps_fraction_of_one() -> None:
    # int(3 * 1.0) == 3 would be off the end of a three-element list.
    assert percentile([1.0, 2.0, 3.0], 1.0) == 3.0


def test_percentile_of_single_value() -> None:
    assert percentile([7.5], 0.95) == 7.5


# --------------------------------------------------------------------------
# WriterMonitor: the writer's stdout contract
# --------------------------------------------------------------------------


def interval_line(tps: float, committed: int = 100, failed: int = 0, p95_ms: float = 12.0) -> str:
    return json.dumps({"committed": committed, "failed": failed, "tps": tps, "p95_ms": p95_ms})


FINAL_LINE = json.dumps(
    {"final": True, "committed": 6000, "failed": 3, "p95_ms": 22.5, "elapsed_seconds": 310.4}
)


def test_interval_line_becomes_a_sample() -> None:
    monitor = WriterMonitor(report_seconds=10)
    monitor.feed(interval_line(118.4, committed=1184, failed=2, p95_ms=14.5), at=100.0)

    assert len(monitor.samples) == 1
    sample = monitor.samples[0]
    assert sample.at == 100.0
    assert sample.tps == 118.4
    assert sample.committed == 1184
    assert sample.failed == 2
    assert sample.p95_ms == 14.5
    assert monitor.malformed == 0
    assert monitor.final is None


def test_trailing_newline_and_blank_lines_are_tolerated() -> None:
    monitor = WriterMonitor()
    monitor.feed(interval_line(50.0) + "\n", at=10.0)
    monitor.feed("\n", at=11.0)
    monitor.feed("   ", at=12.0)

    assert len(monitor.samples) == 1
    assert monitor.malformed == 0


def test_malformed_line_is_counted_not_raised() -> None:
    # The writer's stderr is inherited, not piped, so a stray stdout line means something
    # broke the contract. Counting it puts a number in the results file; letting json.loads
    # raise would take down the reader thread and silently end sampling mid-run.
    monitor = WriterMonitor()
    monitor.feed("writer: connection reset by peer", at=5.0)
    monitor.feed('{"tps": ', at=6.0)
    monitor.feed("[1, 2, 3]", at=7.0)

    assert monitor.samples == []
    assert monitor.malformed == 3
    assert monitor.malformed_lines[0] == "writer: connection reset by peer"


def test_malformed_examples_are_capped() -> None:
    monitor = WriterMonitor()
    for index in range(10):
        monitor.feed(f"not json {index}", at=float(index))

    assert monitor.malformed == 10
    assert len(monitor.malformed_lines) == run.MAX_RECORDED_MALFORMED_LINES


def test_line_without_tps_is_not_a_throughput_sample() -> None:
    # writer.py's contract is explicit that every line carrying "tps" is a sample to be
    # taken into a median. A line without one must not be invented into a zero -- a zero
    # would drag the headline number down.
    monitor = WriterMonitor()
    monitor.feed(json.dumps({"committed": 40, "failed": 0, "p95_ms": 9.0}), at=20.0)

    assert monitor.samples == []
    assert monitor.unrecognized == 1
    assert monitor.malformed == 0
    assert monitor.final is None


def test_final_line_is_captured_and_is_not_a_sample() -> None:
    monitor = WriterMonitor(report_seconds=10)
    monitor.feed(interval_line(100.0), at=20.0)
    monitor.feed(FINAL_LINE, at=30.0)

    assert len(monitor.samples) == 1
    assert monitor.final is not None
    assert monitor.final["committed"] == 6000
    assert monitor.final["failed"] == 3
    assert monitor.final["elapsed_seconds"] == 310.4
    assert monitor.unrecognized == 0
    assert monitor.malformed == 0


def test_final_line_lifetime_counts_reach_the_summary() -> None:
    monitor = WriterMonitor(report_seconds=10)
    monitor.feed(interval_line(100.0), at=20.0)
    monitor.feed(FINAL_LINE, at=30.0)

    summary = monitor.summary(start=0.0, end=30.0)
    assert summary["final_line_seen"] is True
    assert summary["committed_lifetime"] == 6000
    assert summary["failed_lifetime"] == 3
    assert summary["elapsed_seconds"] == 310.4


def test_sample_with_non_numeric_tps_is_malformed() -> None:
    monitor = WriterMonitor()
    monitor.feed(json.dumps({"tps": "fast", "committed": 1}), at=1.0)

    assert monitor.samples == []
    assert monitor.malformed == 1


# --------------------------------------------------------------------------
# WriterMonitor: which samples the headline TPS is a median of
# --------------------------------------------------------------------------


def test_only_windows_fully_inside_the_measurement_are_eligible() -> None:
    # A line read at T covers (T - 10, T]. Measurement runs [30, 90], so the line at 35
    # covers warm-up where the writer had the database to itself, and the line at 95 covers
    # time after the dashboard load stopped. Including either overstates writer throughput,
    # which is precisely the number the workshop leads with.
    monitor = WriterMonitor(report_seconds=10)
    for at, tps in ((25.0, 200.0), (35.0, 190.0), (40.0, 100.0), (50.0, 110.0), (95.0, 195.0)):
        monitor.feed(interval_line(tps), at=at)

    eligible = monitor.samples_in_window(start=30.0, end=90.0)
    assert [s.tps for s in eligible] == [100.0, 110.0]


def test_headline_tps_is_the_median_of_eligible_samples() -> None:
    monitor = WriterMonitor(report_seconds=10)
    for at, tps in ((5.0, 500.0), (20.0, 100.0), (30.0, 120.0), (40.0, 110.0)):
        monitor.feed(interval_line(tps), at=at)

    summary = monitor.summary(start=10.0, end=40.0)
    assert summary["tps"] == 110.0
    assert summary["tps_basis"] == "measurement-window"
    assert summary["tps_samples"] == 3
    assert summary["tps_min"] == 100.0
    assert summary["tps_max"] == 120.0
    assert summary["samples_total"] == 4


def test_summary_falls_back_to_every_sample_when_no_window_fits() -> None:
    # A calibration run shorter than one reporting interval still gets a number, but the
    # basis travels with it into the results file and onto the printed table so nobody
    # quotes it as the same measurement.
    monitor = WriterMonitor(report_seconds=10)
    monitor.feed(interval_line(90.0), at=2.0)
    monitor.feed(interval_line(70.0), at=4.0)

    summary = monitor.summary(start=0.0, end=5.0)
    assert summary["tps"] == 80.0
    assert summary["tps_basis"] == "all-samples-fallback"
    assert summary["tps_samples"] == 2


def test_summary_with_no_samples_reports_no_number() -> None:
    monitor = WriterMonitor(report_seconds=10)

    summary = monitor.summary(start=0.0, end=300.0)
    assert summary["tps"] is None
    assert summary["tps_basis"] == "no-samples"
    assert summary["tps_samples"] == 0
    assert summary["final_line_seen"] is False
    assert summary["committed_lifetime"] is None


def test_summary_p95_covers_the_eligible_windows_only() -> None:
    monitor = WriterMonitor(report_seconds=10)
    monitor.feed(interval_line(100.0, p95_ms=999.0), at=5.0)
    monitor.feed(interval_line(100.0, p95_ms=20.0), at=20.0)
    monitor.feed(interval_line(100.0, p95_ms=30.0), at=30.0)

    summary = monitor.summary(start=10.0, end=30.0)
    assert summary["p95_ms"] == 30.0


# --------------------------------------------------------------------------
# The pipe pump, without a subprocess
# --------------------------------------------------------------------------


def test_pump_stamps_each_line_as_it_arrives() -> None:
    # The pump is what keeps the writer's pipe drained for the whole run, and it is also
    # where the timestamps used by the window filter come from. Exercised with a list and a
    # fake clock: no subprocess, and the arrival times are exact rather than approximate.
    monitor = WriterMonitor(report_seconds=10)
    ticks = iter([10.0, 20.0, 30.0])
    lines = [interval_line(100.0) + "\n", interval_line(120.0) + "\n", FINAL_LINE + "\n"]

    run.WriterProcess._pump(iter(lines), monitor, lambda: next(ticks))

    assert [(s.at, s.tps) for s in monitor.samples] == [(10.0, 100.0), (20.0, 120.0)]
    assert monitor.final is not None


def test_pump_survives_a_broken_pipe_without_raising() -> None:
    # A reader thread that dies on an exception would end sampling silently, and the run
    # would report the median of however many samples arrived before the pipe broke.
    monitor = WriterMonitor()

    def exploding():
        yield interval_line(100.0)
        raise OSError("pipe closed")

    run.WriterProcess._pump(exploding(), monitor, lambda: 1.0)

    assert len(monitor.samples) == 1


def test_pump_tolerates_a_missing_stream() -> None:
    monitor = WriterMonitor()
    run.WriterProcess._pump(None, monitor, lambda: 0.0)
    assert monitor.samples == []


# --------------------------------------------------------------------------
# build_result: a query that fails on one target must not hide
# --------------------------------------------------------------------------


def config_for(queries: int = 2) -> dict:
    return {
        "dashboard_concurrency": 8,
        "duration_seconds": 300,
        "warmup_seconds": 30,
        "queries": queries,
        "writer": True,
        "writer_rate": 200,
    }


def writer_summary(**overrides) -> dict:
    summary = {
        "tps": 118.4,
        "tps_basis": "measurement-window",
        "tps_samples": 29,
        "tps_min": 110.0,
        "tps_max": 125.0,
        "report_seconds": 10,
        "p95_ms": 22.0,
        "committed_lifetime": 36000,
        "failed_lifetime": 0,
        "elapsed_seconds": 330.0,
        "final_line_seen": True,
        "samples_total": 33,
        "malformed_lines": 0,
        "malformed_examples": [],
        "unrecognized_lines": 0,
        "exit_code": 0,
        "died_before_stop": False,
    }
    summary.update(overrides)
    return summary


def test_result_has_a_row_for_every_query_even_one_that_never_ran() -> None:
    # This is the failure the harness exists to make visible: q6 works on Postgres and
    # errors through pg_clickhouse, so an "after" table built only from queries that
    # succeeded would have seven rows and would still look like a comparison.
    stats = DashboardStats()
    stats.record_timing("q1_revenue_by_hour", 10.0)
    stats.record_timing("q1_revenue_by_hour", 30.0)
    stats.record_error("q6_category_mix", RuntimeError("relation does not exist"))

    result = build_result(
        label="after",
        config=config_for(),
        query_names=["q1_revenue_by_hour", "q6_category_mix"],
        stats=stats,
        elapsed_seconds=100.0,
        writer_summary=writer_summary(),
    )

    assert [row["name"] for row in result["queries"]] == ["q1_revenue_by_hour", "q6_category_mix"]
    failed_row = result["queries"][1]
    assert failed_row["runs"] == 0
    assert failed_row["errors"] == 1
    assert result["queries_without_results"] == ["q6_category_mix"]
    assert result["queries_with_errors"] == ["q6_category_mix"]
    assert result["comparable"] is False
    assert "relation does not exist" in result["query_errors"]["q6_category_mix"][0]
    assert any("NOT comparable" in warning for warning in result["warnings"])


def test_partial_failures_are_reported_without_voiding_the_comparison() -> None:
    stats = DashboardStats()
    stats.record_timing("q3_revenue_by_region", 50.0)
    stats.record_error("q3_revenue_by_region", TimeoutError("statement timeout"))

    result = build_result(
        label="before",
        config=config_for(1),
        query_names=["q3_revenue_by_region"],
        stats=stats,
        elapsed_seconds=100.0,
        writer_summary=writer_summary(),
    )

    assert result["comparable"] is True
    assert result["queries"][0]["runs"] == 1
    assert result["queries"][0]["errors"] == 1
    assert any("excluded from p50/p95" in warning for warning in result["warnings"])


def test_dashboard_qps_is_completions_over_measured_seconds() -> None:
    stats = DashboardStats()
    for _ in range(400):
        stats.record_timing("q1_revenue_by_hour", 5.0)

    result = build_result(
        label="before",
        config=config_for(1),
        query_names=["q1_revenue_by_hour"],
        stats=stats,
        elapsed_seconds=100.0,
        writer_summary=writer_summary(),
    )

    assert result["dashboard_completed"] == 400
    assert result["dashboard_qps"] == 4.0
    assert result["measured_seconds"] == 100.0


def test_zero_elapsed_does_not_divide_by_zero() -> None:
    result = build_result(
        label="before",
        config=config_for(1),
        query_names=["q1_revenue_by_hour"],
        stats=DashboardStats(),
        elapsed_seconds=0.0,
        writer_summary=writer_summary(),
    )

    assert result["dashboard_qps"] == 0.0


def test_a_run_without_a_writer_says_it_has_no_headline_number() -> None:
    result = build_result(
        label="before",
        config=config_for(1),
        query_names=["q1_revenue_by_hour"],
        stats=DashboardStats(),
        elapsed_seconds=10.0,
        writer_summary=None,
    )

    assert result["writer_tps"] is None
    assert result["writer"] is None
    assert any("headline number" in warning for warning in result["warnings"])


def test_a_writer_that_died_mid_run_voids_the_measurement_loudly() -> None:
    # The one output worse than a wrong number is an uncontended dashboard measurement
    # labelled as a contended one, which is what a writer that fell over halfway produces.
    result = build_result(
        label="before",
        config=config_for(1),
        query_names=["q1_revenue_by_hour"],
        stats=DashboardStats(),
        elapsed_seconds=100.0,
        writer_summary=writer_summary(died_before_stop=True, exit_code=1),
    )

    assert result["writer_died"] is True
    assert any("NOT a contended measurement" in warning for warning in result["warnings"])
    assert "| Writer held the load for the whole run | NO |" in render_markdown(result)


def test_a_healthy_writer_says_it_held_the_load() -> None:
    result = build_result(
        label="before",
        config=config_for(1),
        query_names=["q1_revenue_by_hour"],
        stats=DashboardStats(),
        elapsed_seconds=100.0,
        writer_summary=writer_summary(),
    )

    assert result["writer_died"] is False
    assert "| Writer held the load for the whole run | yes |" in render_markdown(result)


def test_writer_failures_and_fallback_basis_are_surfaced() -> None:
    result = build_result(
        label="after",
        config=config_for(1),
        query_names=["q1_revenue_by_hour"],
        stats=DashboardStats(),
        elapsed_seconds=10.0,
        writer_summary=writer_summary(
            tps_basis="all-samples-fallback",
            failed_lifetime=17,
            final_line_seen=False,
            malformed_lines=2,
        ),
    )

    joined = " ".join(result["warnings"])
    assert "fell back to every sample" in joined
    assert "17 failed transactions" in joined
    assert "no final line" in joined
    assert "unparseable lines" in joined


def test_error_messages_are_deduped_and_capped() -> None:
    stats = DashboardStats()
    for _ in range(5):
        stats.record_error("q6_category_mix", RuntimeError("same message"))
    for index in range(5):
        stats.record_error("q6_category_mix", RuntimeError(f"different {index}"))

    assert stats.error_counts["q6_category_mix"] == 10
    assert len(stats.error_messages["q6_category_mix"]) == run.MAX_RECORDED_ERRORS_PER_QUERY


# --------------------------------------------------------------------------
# render_markdown
# --------------------------------------------------------------------------


def sample_result() -> dict:
    stats = DashboardStats()
    stats.record_timing("q1_revenue_by_hour", 10.0)
    stats.record_timing("q1_revenue_by_hour", 30.0)
    stats.record_error("q6_category_mix", RuntimeError("function not supported"))
    return build_result(
        label="after",
        config=config_for(),
        query_names=["q1_revenue_by_hour", "q6_category_mix"],
        stats=stats,
        elapsed_seconds=100.0,
        writer_summary=writer_summary(),
    )


def test_markdown_prints_the_config_block_alongside_the_numbers() -> None:
    # A benchmark without its configuration is not a result: dashboard QPS at concurrency 8
    # and at concurrency 32 are different quantities with the same name.
    rendered = render_markdown(sample_result())

    assert "| Config | Value |" in rendered
    assert "| dashboard_concurrency | 8 |" in rendered
    assert "| duration_seconds | 300 |" in rendered
    assert "| warmup_seconds | 30 |" in rendered
    assert "| writer_rate | 200 |" in rendered
    assert "| queries | 2 |" in rendered


def test_markdown_leads_with_qps_and_writer_tps() -> None:
    rendered = render_markdown(sample_result())

    assert "## Dashboard benchmark: after" in rendered
    assert "| Sustained dashboard QPS | 0.02 |" in rendered
    assert "| Writer TPS during dashboard load | 118.4 (measurement-window, 29 samples) |" in rendered


def test_markdown_query_table_is_one_row_per_query_with_error_counts() -> None:
    rendered = render_markdown(sample_result())
    lines = rendered.splitlines()

    assert "| Query | runs | errors | p50 ms | p95 ms |" in lines
    assert "| q1_revenue_by_hour | 2 | 0 | 30.0 | 30.0 |" in lines
    assert "| q6_category_mix | 0 | 1 | 0.0 | 0.0 |" in lines


def test_markdown_shows_warnings_and_the_error_text() -> None:
    rendered = render_markdown(sample_result())

    assert "WARNINGS" in rendered
    assert "| Comparable query set | NO |" in rendered
    assert "q6_category_mix: RuntimeError: function not supported" in rendered


def test_markdown_renders_a_missing_writer_as_a_dash() -> None:
    config = config_for(1)
    config["writer"] = False
    config["writer_rate"] = None
    result = build_result(
        label="before",
        config=config,
        query_names=["q1_revenue_by_hour"],
        stats=DashboardStats(),
        elapsed_seconds=10.0,
        writer_summary=None,
    )
    rendered = render_markdown(result)

    assert "| Writer TPS during dashboard load | - (no-writer, 0 samples) |" in rendered
    assert "| writer_rate | - |" in rendered


def test_markdown_is_a_string_with_no_emoji() -> None:
    rendered = render_markdown(sample_result())

    assert isinstance(rendered, str)
    assert rendered.isascii()


# --------------------------------------------------------------------------
# dashboard_worker: one connection per session, warm-up discarded
# --------------------------------------------------------------------------


class FakeConnection:
    def __init__(self) -> None:
        self.fetched: list[str] = []

    async def fetch(self, sql: str) -> list:
        self.fetched.append(sql)
        return []


class FakeAcquire:
    def __init__(self, pool: "FakePool") -> None:
        self.pool = pool

    async def __aenter__(self) -> FakeConnection:
        return self.pool.connection

    async def __aexit__(self, *exc_info) -> bool:
        return False


class FakePool:
    """Just enough of asyncpg.Pool to prove the acquire happens once per worker."""

    def __init__(self) -> None:
        self.connection = FakeConnection()
        self.acquires = 0

    def acquire(self) -> FakeAcquire:
        self.acquires += 1
        return FakeAcquire(self)


def stepping_clock(values: list[float]):
    """A clock that returns the next scripted value, then stays at the last one."""
    remaining = list(values)

    def clock() -> float:
        return remaining.pop(0) if len(remaining) > 1 else remaining[0]

    return clock


def test_worker_acquires_one_connection_for_its_whole_session() -> None:
    # The connection is acquired outside the timing loop on purpose: timing pool.acquire()
    # as part of the query folds the harness's own pool sizing into panel latency, and one
    # long-lived session is the truer model of an open dashboard panel anyway.
    queries = [("q1", "SELECT 1"), ("q2", "SELECT 2")]
    pool = FakePool()
    stats = DashboardStats()
    # loop checks: 0 (run), 0 (record), 1 (run), 1 (record), 2 (stop at deadline)
    clock = stepping_clock([0.0, 0.0, 1.0, 1.0, 99.0])

    asyncio.run(
        run.dashboard_worker(pool, queries, 0, measure_start=0.0, deadline=10.0, stats=stats, clock=clock)
    )

    assert pool.acquires == 1
    assert pool.connection.fetched == ["SELECT 1", "SELECT 2"]
    assert stats.completed == 2


def test_worker_discards_everything_before_measurement_starts() -> None:
    queries = [("q1", "SELECT 1")]
    pool = FakePool()
    stats = DashboardStats()
    # Two passes land at t=0 and t=1, both before measure_start=5, so neither is timed;
    # the third check ends the loop. Without this, the first pass over eight cold queries
    # lands entirely in the p95.
    clock = stepping_clock([0.0, 0.0, 1.0, 1.0, 99.0])

    asyncio.run(
        run.dashboard_worker(pool, queries, 0, measure_start=5.0, deadline=10.0, stats=stats, clock=clock)
    )

    assert pool.connection.fetched == ["SELECT 1", "SELECT 1"]
    assert stats.timings == {}
    assert stats.completed == 0


def test_worker_start_index_staggers_the_panel_it_begins_with() -> None:
    # All workers starting at q1 would run the eight panels in lock-step, which changes
    # both the contention shape and how evenly the runs are spread across queries.
    queries = [("q1", "SELECT 1"), ("q2", "SELECT 2"), ("q3", "SELECT 3")]
    pool = FakePool()
    clock = stepping_clock([0.0, 0.0, 99.0])

    asyncio.run(
        run.dashboard_worker(
            pool, queries, 2, measure_start=0.0, deadline=10.0, stats=DashboardStats(), clock=clock
        )
    )

    assert pool.connection.fetched == ["SELECT 3"]


# --------------------------------------------------------------------------
# CLI surface and pinned defaults
# --------------------------------------------------------------------------


def test_defaults_are_pinned_in_the_file() -> None:
    args = parse_args(["--dsn", "postgres:///shop", "--label", "before", "--out", "results/before.json"])

    assert args.dashboard_concurrency == run.DASHBOARD_CONCURRENCY == 8
    assert args.duration_seconds == run.DURATION_SECONDS == 300
    assert args.warmup_seconds == run.WARMUP_SECONDS == 30
    assert args.writer_rate == run.WRITER_RATE == 200
    assert args.writer_concurrency == run.WRITER_CONCURRENCY == 16
    assert run.WRITER_REPORT_SECONDS == 10


def test_missing_required_flags_exit_two() -> None:
    with pytest.raises(SystemExit) as excinfo:
        parse_args(["--dsn", "postgres:///shop"])
    assert excinfo.value.code == 2


@pytest.mark.parametrize(
    "flag,value",
    [
        ("--dashboard-concurrency", "0"),
        ("--duration-seconds", "0"),
        ("--warmup-seconds", "-1"),
        ("--writer-rate", "0"),
        ("--writer-concurrency", "0"),
    ],
)
def test_nonsense_load_parameters_are_rejected(flag: str, value: str) -> None:
    with pytest.raises(SystemExit):
        parse_args(
            ["--dsn", "postgres:///shop", "--label", "before", "--out", "out.json", flag, value]
        )


def test_config_block_records_that_the_writer_was_absent() -> None:
    args = parse_args(["--dsn", "postgres:///shop", "--label", "before", "--out", "out.json"])
    config = run.build_config(args, query_count=8)

    assert config["writer"] is False
    assert config["writer_rate"] is None
    assert config["writer_concurrency"] is None
    assert config["writer_report_seconds"] is None
    assert config["latency_excludes_pool_acquire"] is True


def test_config_block_records_the_writer_settings_when_present() -> None:
    args = parse_args(
        [
            "--dsn",
            "postgres:///shop",
            "--label",
            "after",
            "--out",
            "out.json",
            "--writer-dsn",
            "postgres:///shop_writer",
        ]
    )
    config = run.build_config(args, query_count=8)

    assert config["writer"] is True
    assert config["writer_rate"] == 200
    assert config["writer_concurrency"] == 16
    assert config["writer_report_seconds"] == 10


# --------------------------------------------------------------------------
# The queries the harness replays
# --------------------------------------------------------------------------


def test_load_queries_reads_the_eight_panel_queries_in_filename_order() -> None:
    queries = load_queries()
    names = [name for name, _ in queries]

    assert len(queries) == 8
    assert names == sorted(names)
    assert names[0] == "q1_revenue_by_hour"
    assert all(sql.strip() for _, sql in queries)


def test_load_queries_accepts_a_directory(tmp_path) -> None:
    (tmp_path / "q2_b.sql").write_text("SELECT 2\n")
    (tmp_path / "q1_a.sql").write_text("  SELECT 1  \n")

    assert load_queries(tmp_path) == [("q1_a", "SELECT 1"), ("q2_b", "SELECT 2")]


def test_writer_path_points_at_the_order_writer() -> None:
    # The harness spawns it by path, so a directory move must fail here rather than at
    # minute one of a five-minute measurement.
    assert run.WRITER_PATH.name == "writer.py"
    assert run.WRITER_PATH.exists()
