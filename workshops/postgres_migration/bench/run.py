"""Replays the dashboard's panel queries under concurrent OLTP write load.

Two DSNs, one harness. The only difference between the "before" and "after" runs is which
database the dashboard queries go to; the writer keeps running against the OLTP primary in
both, because the number that matters is what the dashboard costs the checkout path.

Three numbers come out, and the third is the headline:

  - per-query p50 and p95, over the measurement window only
  - sustained dashboard QPS
  - **writer TPS measured during the dashboard load** -- the measurable form of "dashboards
    steal throughput from checkout", and the number the whole migration is justified by

Every load parameter is pinned as a module constant below and only overridable by flag for
the author's calibration run. Participants must not be choosing the load level: two runs at
different concurrency are two different experiments, not a before and an after. Whatever the
values were, they are written into the results file and printed above the table, because a
benchmark without its configuration is not a result.

How the writer is driven, and why it is not the obvious way:

  `app/writer.py` is spawned as a subprocess with its stdout on a pipe, and a **dedicated
  thread drains that pipe for the whole run**. Leaving the pipe unread until the end -- the
  obvious shape -- risks filling the OS pipe buffer (64 KB is typical, and a long run at a
  short report interval gets there), at which point the writer's own `print` blocks inside
  its event loop and its throughput collapses. That would silently corrupt the exact number
  this harness leads with, and it would look like contention. The thread also means
  `terminate()` is followed by a bounded `wait()` rather than by an unbounded read: iterating
  the pipe after terminate() hangs forever if the writer does not exit, and the `wait(timeout=)`
  placed after such a loop never gets a chance to fire.

  Samples are timestamped as they arrive and only those whose whole reporting window falls
  inside the measurement window are taken into the median. Without that filter, the samples
  printed while the dashboard pool was still warming up -- when the writer has the database
  to itself -- inflate the "before" TPS and understate the contention the workshop is about.

Nothing here imports asyncpg at module scope: the tests, `--help`, and a `py_compile` check
all have to work on a machine with no database driver installed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

BENCH_DIR = Path(__file__).resolve().parent
QUERY_DIR = BENCH_DIR / "queries"
WRITER_PATH = BENCH_DIR.parent / "app" / "writer.py"

# ---------------------------------------------------------------------------
# Pinned load parameters. Change these in the file, for everyone, or no two runs
# compare. They are echoed into the results file and printed above the table.
# ---------------------------------------------------------------------------
DASHBOARD_CONCURRENCY = 8
DURATION_SECONDS = 300
WARMUP_SECONDS = 30
WRITER_RATE = 200
WRITER_CONCURRENCY = 16

# Not a flag, deliberately: the reporting interval sets the granularity of the headline
# TPS median and how many samples the window filter can keep. 10 s over a 300 s
# measurement leaves 29 fully-enclosed windows.
WRITER_REPORT_SECONDS = 10

# A failing query returns instantly, so an unguarded retry loop turns one broken panel
# into thousands of error lines and pins a core.
ERROR_BACKOFF_SECONDS = 0.25
MAX_RECORDED_ERRORS_PER_QUERY = 3
MAX_ERROR_CHARS = 240
MAX_RECORDED_MALFORMED_LINES = 3
WRITER_STOP_TIMEOUT_SECONDS = 30
WRITER_KILL_TIMEOUT_SECONDS = 5
WRITER_CHECK_SECONDS = 5


def percentile(values: Iterable[float], fraction: float) -> float:
    """Nearest-rank percentile: every value reported is a latency actually measured.

    Same rule as `app/writer.py` on purpose -- a p95 from the harness and a p95 from the
    writer are then computed the same way and can sit in the same sentence. Interpolating
    would invent a latency no query ever had.
    """
    ordered = sorted(values)
    if not ordered:
        return 0.0
    index = min(len(ordered) - 1, int(len(ordered) * fraction))
    return ordered[index]


def load_queries(directory: Path | None = None) -> list[tuple[str, str]]:
    """Every `*.sql` file in `queries/`, in filename order, as (name, sql).

    Filename order is the replay order, which is why the files are `q1_`..`q8_`: the panel
    mix must be identical between the before and the after run.
    """
    query_dir = QUERY_DIR if directory is None else directory
    return [(path.stem, path.read_text().strip()) for path in sorted(query_dir.glob("*.sql"))]


# ---------------------------------------------------------------------------
# Writer output
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WriterSample:
    """One interval line from the writer, stamped with when the harness read it."""

    at: float
    tps: float
    committed: int
    failed: int
    p95_ms: float


class WriterMonitor:
    """Parses `app/writer.py`'s stdout contract, one line at a time.

    Split out from the thread that reads the pipe so it can be tested with explicit
    timestamps and hand-written lines: the parsing is where a silent wrong number would
    come from, and it must not need a subprocess to exercise.

    The contract it implements, from writer.py's own docstring:
      - interval line: has "tps" -- a throughput sample
      - final line: has "final": true and deliberately NO "tps", because every line with
        one is taken into the median; it carries lifetime committed/failed and elapsed
      - anything else on stdout is a contract violation and is counted, not ignored
    """

    def __init__(self, report_seconds: int = WRITER_REPORT_SECONDS) -> None:
        self.report_seconds = report_seconds
        self.samples: list[WriterSample] = []
        self.final: dict[str, Any] | None = None
        self.malformed = 0
        self.unrecognized = 0
        self.malformed_lines: list[str] = []

    def feed(self, line: str, at: float) -> None:
        text = line.strip()
        if not text:
            return
        try:
            payload = json.loads(text)
        except ValueError:
            self._record_malformed(text)
            return
        if not isinstance(payload, dict):
            self._record_malformed(text)
            return
        if payload.get("final"):
            # Lifetime totals. Kept whole rather than picked apart, so the results file
            # can carry what the writer said even if writer.py grows a key.
            self.final = payload
            return
        if "tps" not in payload:
            # Not a throughput sample and not the final line. Counted so a writer whose
            # contract drifted shows up as a number in the results rather than as silence.
            self.unrecognized += 1
            return
        try:
            self.samples.append(
                WriterSample(
                    at=at,
                    tps=float(payload["tps"]),
                    committed=int(payload.get("committed", 0)),
                    failed=int(payload.get("failed", 0)),
                    p95_ms=float(payload.get("p95_ms", 0.0)),
                )
            )
        except (TypeError, ValueError):
            self._record_malformed(text)

    def _record_malformed(self, text: str) -> None:
        self.malformed += 1
        if len(self.malformed_lines) < MAX_RECORDED_MALFORMED_LINES:
            self.malformed_lines.append(text[:MAX_ERROR_CHARS])

    def samples_in_window(self, start: float, end: float) -> list[WriterSample]:
        """Samples whose entire reporting window lies inside [start, end].

        A line read at time T reports the window (T - report_seconds, T], so the first
        eligible line is the one at or after `start + report_seconds`. This is the filter
        that keeps warm-up throughput -- measured with no dashboard load at all -- out of
        the headline number.
        """
        first_eligible = start + self.report_seconds
        return [s for s in self.samples if first_eligible <= s.at <= end]

    def summary(self, start: float, end: float) -> dict[str, Any]:
        """The writer block of the results file, including how the TPS was arrived at."""
        in_window = self.samples_in_window(start, end)
        basis = "measurement-window"
        used = in_window
        if not used and self.samples:
            # A run too short to enclose a whole reporting window still deserves a number,
            # but not one that pretends to be the same measurement. The basis travels with
            # it into the results file and onto the printed table.
            basis = "all-samples-fallback"
            used = list(self.samples)
        tps_values = [s.tps for s in used]
        final = self.final or {}
        return {
            "tps": round(statistics.median(tps_values), 1) if tps_values else None,
            "tps_basis": basis if tps_values else "no-samples",
            "tps_samples": len(used),
            "tps_min": round(min(tps_values), 1) if tps_values else None,
            "tps_max": round(max(tps_values), 1) if tps_values else None,
            "report_seconds": self.report_seconds,
            "p95_ms": round(percentile([s.p95_ms for s in used], 0.95), 1) if used else None,
            "committed_lifetime": final.get("committed"),
            "failed_lifetime": final.get("failed"),
            "elapsed_seconds": final.get("elapsed_seconds"),
            "final_line_seen": self.final is not None,
            "samples_total": len(self.samples),
            "malformed_lines": self.malformed,
            "malformed_examples": list(self.malformed_lines),
            "unrecognized_lines": self.unrecognized,
        }


class WriterProcess:
    """The writer subprocess plus the thread that keeps its pipe drained.

    The thread is the point. See the module docstring: an unread pipe can block the
    writer mid-benchmark, and a read-after-terminate can block the harness forever.
    """

    def __init__(self, proc: subprocess.Popen, monitor: WriterMonitor, reader: threading.Thread) -> None:
        self.proc = proc
        self.monitor = monitor
        self.reader = reader
        self.died_before_stop = False

    @classmethod
    def start(
        cls,
        *,
        dsn: str,
        rate: int,
        concurrency: int,
        report_seconds: int | None = None,
        writer_path: Path | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> "WriterProcess":
        # Resolved here rather than as default arguments: a default binds the module
        # constant at import time, so a caller (or a test) that reassigns WRITER_PATH or
        # WRITER_REPORT_SECONDS would silently keep spawning the old one and stamp the new
        # value into the config block, making the results file disagree with itself.
        report_seconds = WRITER_REPORT_SECONDS if report_seconds is None else report_seconds
        writer_path = WRITER_PATH if writer_path is None else writer_path
        proc = subprocess.Popen(
            [
                sys.executable,
                str(writer_path),
                "--dsn",
                dsn,
                "--rate",
                str(rate),
                "--concurrency",
                str(concurrency),
                "--report-seconds",
                str(report_seconds),
            ],
            stdout=subprocess.PIPE,
            # stderr is deliberately inherited: writer.py puts its first-failure message and
            # its pacing-restart warning there, and those are how a run that produced a
            # beautiful TPS out of nothing but failures gets caught.
            text=True,
            bufsize=1,
        )
        monitor = WriterMonitor(report_seconds)
        reader = threading.Thread(
            target=cls._pump,
            args=(proc.stdout, monitor, clock),
            name="writer-stdout",
            daemon=True,
        )
        reader.start()
        return cls(proc, monitor, reader)

    @staticmethod
    def _pump(stream: Any, monitor: WriterMonitor, clock: Callable[[], float]) -> None:
        if stream is None:
            return
        try:
            for line in stream:
                monitor.feed(line, clock())
        except Exception as exc:  # a dead pipe must not take the benchmark with it
            print(f"bench: writer stdout reader stopped: {type(exc).__name__}: {exc}", file=sys.stderr)

    def exited_early(self) -> int | None:
        """The exit code if the writer is already gone, else None.

        writer.py exits 2 without printing anything on stdout when asyncpg is missing or
        the seed tables are empty, so "no samples" has a cause worth surfacing early
        rather than at the end of a five-minute run.
        """
        return self.proc.poll()

    def stop(self) -> None:
        """SIGTERM, bounded wait, SIGKILL if it comes to that; then join the reader.

        writer.py drains its in-flight transactions and prints its final line on SIGTERM,
        and the reader thread is what picks that line up. Joining after the process exits
        means EOF has already arrived, so the final line cannot be lost to a race.
        """
        if self.proc.poll() is None:
            self.proc.terminate()
        else:
            # It was already gone before we asked it to stop, so some part of the run had
            # no OLTP load at all. That has to reach the results file: an uncontended
            # dashboard number labelled as a contended one is the worst output this
            # harness could produce.
            self.died_before_stop = True
        try:
            self.proc.wait(timeout=WRITER_STOP_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            print(
                f"bench: writer did not exit within {WRITER_STOP_TIMEOUT_SECONDS}s; killing it",
                file=sys.stderr,
            )
            self.proc.kill()
            try:
                self.proc.wait(timeout=WRITER_KILL_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                print("bench: writer ignored SIGKILL", file=sys.stderr)
        self.reader.join(timeout=WRITER_KILL_TIMEOUT_SECONDS)
        if self.proc.stdout is not None:
            try:
                self.proc.stdout.close()
            except Exception:
                pass

    def summary(self, start: float, end: float) -> dict[str, Any]:
        summary = self.monitor.summary(start, end)
        summary["exit_code"] = self.proc.poll()
        summary["died_before_stop"] = self.died_before_stop
        return summary


async def watch_writer(
    writer: WriterProcess,
    workers: list[asyncio.Task],
    deadline: float,
    clock: Callable[[], float] = time.monotonic,
) -> int | None:
    """Abort the run if the writer dies, rather than finishing a measurement that has none.

    Without this, a writer that fails a minute in leaves the dashboard running alone for
    the rest of the window and the harness reports an uncontended latency as a contended
    one -- with no OLTP load, the "before" numbers look like the "after" numbers and the
    workshop's whole comparison evaporates. Losing the run is the correct outcome; the
    writer's stderr is already on the terminal saying why it died.
    """
    while clock() < deadline:
        await asyncio.sleep(WRITER_CHECK_SECONDS)
        code = writer.exited_early()
        if code is not None:
            print(
                f"bench: the writer exited with code {code} mid-run; aborting so this does "
                "not get reported as a measurement taken under OLTP load",
                file=sys.stderr,
                flush=True,
            )
            for task in workers:
                task.cancel()
            return code
    return None


# ---------------------------------------------------------------------------
# Dashboard load
# ---------------------------------------------------------------------------


@dataclass
class DashboardStats:
    """Per-query latencies and per-query failures, kept side by side on purpose.

    Failures are counted, named and sampled rather than printed and forgotten. A query
    that errors on one target and not the other silently changes the query set being
    compared, which is the one way this harness could produce two tables that look
    comparable and are not.
    """

    timings: dict[str, list[float]] = field(default_factory=dict)
    error_counts: dict[str, int] = field(default_factory=dict)
    error_messages: dict[str, list[str]] = field(default_factory=dict)

    def record_timing(self, name: str, elapsed_ms: float) -> None:
        self.timings.setdefault(name, []).append(elapsed_ms)

    def record_error(self, name: str, exc: BaseException) -> None:
        self.error_counts[name] = self.error_counts.get(name, 0) + 1
        message = f"{type(exc).__name__}: {exc}"[:MAX_ERROR_CHARS]
        seen = self.error_messages.setdefault(name, [])
        if message not in seen and len(seen) < MAX_RECORDED_ERRORS_PER_QUERY:
            seen.append(message)
            print(f"bench: query {name} failed: {message}", file=sys.stderr, flush=True)

    @property
    def completed(self) -> int:
        return sum(len(values) for values in self.timings.values())


async def dashboard_worker(
    pool: Any,
    queries: list[tuple[str, str]],
    start_index: int,
    measure_start: float,
    deadline: float,
    stats: DashboardStats,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """One dashboard session: acquires a connection once, then replays panels until the deadline.

    The connection is acquired **outside** the timing loop, and only `conn.fetch` is timed.
    Timing `pool.acquire()` as part of the query would fold pool waits into panel latency,
    and the two do not move for the same reason: acquisition time is a property of the
    harness's own pool sizing, while the workshop's claim is about what the query costs on
    each engine. Holding one connection for the worker's life is also the truer model of a
    dashboard, where each open panel keeps a session.

    Work before `measure_start` is warm-up and is discarded -- otherwise the first pass
    over eight cold queries lands entirely in the p95.
    """
    async with pool.acquire() as conn:
        # Staggered so the eight panels are in flight against each other rather than all
        # workers hammering q1 in lock-step, and so every query gets a comparable number
        # of runs.
        index = start_index
        while clock() < deadline:
            name, sql = queries[index % len(queries)]
            index += 1
            started = time.perf_counter()
            try:
                await conn.fetch(sql)
            except Exception as exc:
                stats.record_error(name, exc)
                await asyncio.sleep(ERROR_BACKOFF_SECONDS)
                continue
            if clock() >= measure_start:
                stats.record_timing(name, (time.perf_counter() - started) * 1000.0)


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------


def build_result(
    *,
    label: str,
    config: dict[str, Any],
    query_names: list[str],
    stats: DashboardStats,
    elapsed_seconds: float,
    writer_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    """Assemble the results file. Pure, so the visibility of a failure can be tested.

    Every query in `query_names` gets a row even if it never succeeded once, so a target
    where a panel does not run produces a row reading `runs 0` instead of quietly producing
    a shorter table than the run it is being compared against.
    """
    rows = []
    for name in query_names:
        values = stats.timings.get(name, [])
        rows.append(
            {
                "name": name,
                "runs": len(values),
                "errors": stats.error_counts.get(name, 0),
                "p50_ms": round(percentile(values, 0.50), 1),
                "p95_ms": round(percentile(values, 0.95), 1),
            }
        )

    failed_names = sorted(stats.error_counts)
    never_succeeded = [row["name"] for row in rows if row["runs"] == 0]
    writer_tps = writer_summary.get("tps") if writer_summary else None

    warnings: list[str] = []
    if never_succeeded:
        warnings.append(
            "these queries never returned a row on this target, so this table is NOT "
            f"comparable query-for-query: {', '.join(never_succeeded)}"
        )
    elif failed_names:
        warnings.append(
            "some runs of these queries failed and were excluded from p50/p95: "
            f"{', '.join(failed_names)}"
        )
    if writer_summary is None:
        warnings.append(
            "no --writer-dsn, so there was no OLTP load and no writer TPS: this run does "
            "not produce the headline number"
        )
    else:
        if writer_summary.get("died_before_stop"):
            warnings.append(
                "the writer exited on its own during the run (exit code "
                f"{writer_summary.get('exit_code')}), so part or all of this measurement had "
                "no OLTP load: it is NOT a contended measurement and must not be quoted as one"
            )
        if writer_tps is None:
            warnings.append("the writer produced no throughput samples; writer TPS is unavailable")
        if writer_summary.get("tps_basis") == "all-samples-fallback":
            warnings.append(
                "writer TPS fell back to every sample, including warm-up: no reporting "
                "window fitted inside the measurement window"
            )
        if not writer_summary.get("final_line_seen"):
            warnings.append("the writer printed no final line, so its lifetime totals are unknown")
        if writer_summary.get("failed_lifetime"):
            warnings.append(
                f"the writer recorded {writer_summary['failed_lifetime']} failed transactions"
            )
        if writer_summary.get("malformed_lines"):
            warnings.append(
                f"{writer_summary['malformed_lines']} unparseable lines on the writer's stdout"
            )

    return {
        "label": label,
        "config": config,
        "queries": rows,
        "dashboard_qps": round(stats.completed / elapsed_seconds, 2) if elapsed_seconds > 0 else 0.0,
        "dashboard_completed": stats.completed,
        "measured_seconds": round(elapsed_seconds, 1),
        "writer_tps": writer_tps,
        "writer": writer_summary,
        "writer_died": bool(writer_summary and writer_summary.get("died_before_stop")),
        "query_errors": {name: stats.error_messages.get(name, []) for name in failed_names},
        "queries_with_errors": failed_names,
        "queries_without_results": never_succeeded,
        "comparable": not never_succeeded,
        "warnings": warnings,
    }


def render_markdown(result: dict[str, Any]) -> str:
    """The printed report: config first, then the numbers, then the caveats.

    The config table is not optional decoration. Any number quoted out of this harness has
    to be quoted with the block above it, because dashboard QPS at concurrency 8 and at
    concurrency 32 are different quantities with the same name.
    """
    writer = result.get("writer") or {}
    lines: list[str] = []
    lines.append(f"## Dashboard benchmark: {result['label']}")
    lines.append("")
    lines.append("| Config | Value |")
    lines.append("| --- | --- |")
    for key, value in result["config"].items():
        lines.append(f"| {key} | {'-' if value is None else value} |")
    lines.append("")
    lines.append("| Metric | Value |")
    lines.append("| --- | --- |")
    lines.append(f"| Sustained dashboard QPS | {result['dashboard_qps']} |")
    writer_tps = result.get("writer_tps")
    basis = writer.get("tps_basis", "no-writer")
    lines.append(
        f"| Writer TPS during dashboard load | {'-' if writer_tps is None else writer_tps} "
        f"({basis}, {writer.get('tps_samples', 0)} samples) |"
    )
    lines.append(f"| Queries completed | {result['dashboard_completed']} |")
    lines.append(f"| Measured seconds | {result['measured_seconds']} |")
    lines.append(f"| Comparable query set | {'yes' if result['comparable'] else 'NO'} |")
    if result.get("writer") is not None:
        lines.append(f"| Writer held the load for the whole run | {'NO' if result.get('writer_died') else 'yes'} |")
    lines.append("")
    lines.append("| Query | runs | errors | p50 ms | p95 ms |")
    lines.append("| --- | --- | --- | --- | --- |")
    for row in result["queries"]:
        lines.append(
            f"| {row['name']} | {row['runs']} | {row['errors']} | {row['p50_ms']} | {row['p95_ms']} |"
        )
    if result["warnings"]:
        lines.append("")
        lines.append("WARNINGS")
        for warning in result["warnings"]:
            lines.append(f"- {warning}")
    for name, messages in result["query_errors"].items():
        for message in messages:
            lines.append(f"- {name}: {message}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Replays the eight dashboard panel queries under concurrent OLTP write load and "
            "reports panel p50/p95, dashboard QPS and writer TPS during the load. Load "
            "parameters are pinned in the file: override them only to recalibrate, never "
            "per participant, or no two runs compare."
        )
    )
    parser.add_argument("--dsn", required=True, help="where the dashboard queries go")
    parser.add_argument("--label", required=True, help="run name, e.g. before or after")
    parser.add_argument("--out", required=True, help="path of the JSON results file to write")
    parser.add_argument(
        "--dashboard-concurrency",
        type=int,
        default=DASHBOARD_CONCURRENCY,
        help=f"concurrent dashboard sessions (pinned default: {DASHBOARD_CONCURRENCY})",
    )
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=DURATION_SECONDS,
        help=f"measured seconds, excluding warm-up (pinned default: {DURATION_SECONDS})",
    )
    parser.add_argument(
        "--warmup-seconds",
        type=int,
        default=WARMUP_SECONDS,
        help=f"discarded seconds before measurement starts (pinned default: {WARMUP_SECONDS})",
    )
    parser.add_argument(
        "--writer-dsn",
        help="if set, app/writer.py runs against this DSN for the whole measurement",
    )
    parser.add_argument(
        "--writer-rate",
        type=int,
        default=WRITER_RATE,
        help=f"writer target orders per second (pinned default: {WRITER_RATE})",
    )
    parser.add_argument(
        "--writer-concurrency",
        type=int,
        default=WRITER_CONCURRENCY,
        help=f"writer in-flight transactions (pinned default: {WRITER_CONCURRENCY})",
    )
    args = parser.parse_args(argv)
    if args.dashboard_concurrency < 1:
        parser.error("--dashboard-concurrency must be at least 1")
    if args.duration_seconds < 1:
        parser.error("--duration-seconds must be at least 1")
    if args.warmup_seconds < 0:
        parser.error("--warmup-seconds cannot be negative")
    if args.writer_rate < 1:
        parser.error("--writer-rate must be at least 1")
    if args.writer_concurrency < 1:
        parser.error("--writer-concurrency must be at least 1")
    return args


def build_config(args: argparse.Namespace, query_count: int) -> dict[str, Any]:
    return {
        "dashboard_concurrency": args.dashboard_concurrency,
        "duration_seconds": args.duration_seconds,
        "warmup_seconds": args.warmup_seconds,
        "queries": query_count,
        "writer": bool(args.writer_dsn),
        "writer_rate": args.writer_rate if args.writer_dsn else None,
        "writer_concurrency": args.writer_concurrency if args.writer_dsn else None,
        "writer_report_seconds": WRITER_REPORT_SECONDS if args.writer_dsn else None,
        "latency_excludes_pool_acquire": True,
        "percentile_rule": "nearest-rank",
    }


async def run_benchmark(args: argparse.Namespace) -> int:
    try:
        import asyncpg
    except ModuleNotFoundError:
        print(
            "bench: asyncpg is not installed. Run: pip install -r requirements.txt",
            file=sys.stderr,
        )
        return 2

    queries = load_queries()
    if not queries:
        print(f"bench: no .sql files in {QUERY_DIR}", file=sys.stderr)
        return 2
    query_names = [name for name, _ in queries]

    writer: WriterProcess | None = None
    if args.writer_dsn:
        if not WRITER_PATH.exists():
            print(f"bench: writer not found at {WRITER_PATH}", file=sys.stderr)
            return 2
        writer = WriterProcess.start(
            dsn=args.writer_dsn,
            rate=args.writer_rate,
            concurrency=args.writer_concurrency,
        )
    else:
        print(
            "bench: no --writer-dsn, so this run measures the dashboard with no OLTP load "
            "and produces no writer TPS",
            file=sys.stderr,
        )

    stats = DashboardStats()
    measure_start = 0.0
    measure_end = 0.0
    writer_aborted = False
    try:
        pool = await asyncpg.create_pool(
            args.dsn,
            min_size=args.dashboard_concurrency,
            max_size=args.dashboard_concurrency,
        )
        try:
            if writer is not None:
                exit_code = writer.exited_early()
                if exit_code is not None:
                    print(
                        f"bench: the writer exited immediately with code {exit_code}; its "
                        "stderr above says why. Continuing without OLTP load would produce "
                        "an uncontended number labelled as a contended one.",
                        file=sys.stderr,
                    )
                    return 2

            started = time.monotonic()
            measure_start = started + args.warmup_seconds
            deadline = measure_start + args.duration_seconds
            print(
                f"bench: {args.label}: {args.warmup_seconds}s warm-up then "
                f"{args.duration_seconds}s measured at concurrency {args.dashboard_concurrency}",
                file=sys.stderr,
                flush=True,
            )
            workers = [
                asyncio.create_task(
                    dashboard_worker(pool, queries, index, measure_start, deadline, stats)
                )
                for index in range(args.dashboard_concurrency)
            ]
            watchdog = (
                asyncio.create_task(watch_writer(writer, workers, deadline))
                if writer is not None
                else None
            )
            try:
                await asyncio.gather(*workers)
            except asyncio.CancelledError:
                # The watchdog cancelled them because the writer died. Not an error path to
                # hide: it falls through so the results file records what happened.
                writer_aborted = True
            measure_end = time.monotonic()
            if watchdog is not None:
                watchdog.cancel()
                try:
                    await watchdog
                except asyncio.CancelledError:
                    pass
        finally:
            await pool.close()
    finally:
        if writer is not None:
            writer.stop()

    elapsed = max(0.0, measure_end - measure_start)
    writer_summary = writer.summary(measure_start, measure_end) if writer is not None else None
    result = build_result(
        label=args.label,
        config=build_config(args, len(queries)),
        query_names=query_names,
        stats=stats,
        elapsed_seconds=elapsed,
        writer_summary=writer_summary,
    )

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2) + "\n")

    print()
    print(render_markdown(result))
    print()
    print(f"bench: wrote {out}")

    # A query that never succeeded means the two tables are not the same experiment, and a
    # run whose writer died is not a measurement under load at all. Either one has to be a
    # failing exit status, not a line of prose in the middle of a table.
    if result["queries_without_results"] or writer_aborted or result.get("writer_died"):
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return asyncio.run(run_benchmark(args))
    except KeyboardInterrupt:
        print("bench: interrupted; no results written", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
