# Dashboard benchmark harness

One harness, two runs, one thing different between them: which database the dashboard's
queries go to. It replays the eight panel queries in `queries/` from several concurrent
dashboard sessions while `../app/writer.py` keeps the OLTP checkout load running, and
reports three things:

| Metric | Where it comes from |
| --- | --- |
| Panel p50 / p95 latency, per query | the harness's own timing of each `fetch` |
| Sustained dashboard QPS | completed panel queries over the measured seconds |
| **Writer TPS during the dashboard load** | the writer's JSON lines, filtered to the measurement window |

The third row is the headline. It is the measurable form of "dashboards steal throughput
from checkout", and it is what the whole migration is justified by. The other two rows are
supporting evidence; if you only have room for one number, it is that one.

**The load parameters are pinned in `run.py`, not chosen per run.** Dashboard QPS at
concurrency 8 and at concurrency 32 are different quantities with the same name, so a
participant who picks their own concurrency produces a number nobody can compare with
anything. The flags exist so the author can recalibrate once, for everyone; change the
constants at the top of `run.py` if the pinned values turn out wrong.

**Never quote a number from this harness without the config block that printed above it.**
A benchmark without its configuration is not a result. The block is printed on stdout and
stored in the results JSON under `config`, so there is no excuse for separating them.

## Install

```bash
cd "$(git rev-parse --show-toplevel)/workshops/postgres_migration/bench"
python3 -m pip install -r requirements.txt
```

## The two runs, verbatim

Both use the analytics role for the dashboard queries and the writer role for the OLTP
load, because the whole `pg_clickhouse` mechanism is scoped to `shop_analytics` and would
be invisible to any other role.

**Before -- everything on RDS.** Run this at the end of module 02, while the dashboard and
the writer are both pointed at the source instance.

```bash
python3 run.py \
  --dsn "postgresql://shop_analytics:PASSWORD@RDS_ENDPOINT:5432/shop" \
  --writer-dsn "postgresql://shop_writer:PASSWORD@RDS_ENDPOINT:5432/shop" \
  --label before \
  --out results/before.json
```

**After -- dashboard through `pg_clickhouse`, writer on Managed Postgres.** Run this after
`../sql/06_pg_clickhouse.sql` has shadowed `shop_analytics`'s `search_path`.

```bash
python3 run.py \
  --dsn "postgresql://shop_analytics:PASSWORD@TARGET_HOST:5432/shop" \
  --writer-dsn "postgresql://shop_writer:PASSWORD@TARGET_HOST:5432/shop" \
  --label after \
  --out results/after.json
```

Two things to get right before believing an "after" number:

- **Run the harness after the `ALTER ROLE`, never across it.** A role's `search_path` is
  applied when a session starts, so the harness's connection pool -- created once, at
  startup, and held for the whole run -- keeps whatever `search_path` was in force when it
  connected. This is the same trap as Grafana's pool, described in `../sql/06_pg_clickhouse.sql`.
  Confirm with `SELECT current_setting('search_path')` on a fresh `shop_analytics` session
  before starting.
- **The after run moves two variables at once**, and the content must not pretend
  otherwise: the dashboard moves to ClickHouse *and* the writer moves off RDS onto Managed
  Postgres. The dashboard-contention half is what this harness demonstrates; the
  engine-throughput half is cited from PostgresBench in module 03 rather than measured
  here.

## Reading the output

```
## Dashboard benchmark: before

| Config | Value |
...
| Metric | Value |
| Sustained dashboard QPS | ... |
| Writer TPS during dashboard load | ... (measurement-window, 29 samples) |
...
| Query | runs | errors | p50 ms | p95 ms |
```

- `writer_tps` is the **median** of the writer's per-interval TPS lines, counting only
  lines whose whole 10-second reporting window falls inside the measurement window. The
  basis is printed beside the number: `measurement-window` is the real thing,
  `all-samples-fallback` means the run was too short to enclose one reporting window and
  the number includes warm-up, and `no-writer` means there was no OLTP load at all.
- `errors` is a per-query column, not a footnote. A query that errors on one target and
  not the other changes the query set being compared, which is the one way two tables from
  this harness can look comparable and not be. A query that never succeeded gets a row
  reading `runs 0`, the run prints `Comparable query set | NO`, and **the process exits 1**.
  A partly failing query is reported but does not void the comparison; its failed attempts
  are excluded from p50/p95, so read the `errors` column before quoting the latency.
- `WARNINGS` collects everything that makes a number less than it appears: no writer, no
  writer samples, a fallback TPS basis, no final writer line, writer transactions that
  failed, unparseable writer output.
- Latency **excludes connection acquisition**. Each dashboard session acquires one pooled
  connection for its whole life and only the `fetch` is timed, so the figure is the cost of
  the query on that engine rather than the cost of the harness's own pool sizing. That is
  recorded in the config block as `latency_excludes_pool_acquire`.
- The first `--warmup-seconds` are executed and discarded. Without that, one cold pass over
  eight queries lands entirely in the p95 and the shorter run looks worse than it is.
- Percentiles are nearest-rank, the same rule `../app/writer.py` uses, so a p95 from the
  harness and a p95 from the writer can sit in the same sentence.

Exit codes, because a wrong number that exits 0 is how a bad figure ends up in the content:

| Code | Meaning |
| --- | --- |
| 0 | a usable run |
| 1 | a query never succeeded, or the writer died before the run finished: do not quote it |
| 2 | it never got started -- no driver, no query files, no writer script, or the writer failed on its first breath |
| 130 | interrupted; no results file written |

**A writer that dies mid-run aborts the run.** The harness polls the writer every five
seconds and cancels the dashboard workers if it is gone, because the alternative is worse
than losing the run: the dashboard would keep going alone, and an uncontended latency
reported as a contended one makes the "before" numbers look like the "after" numbers. The
writer's stderr is already on your terminal saying why it died.

## Corroborate it

Do not ask participants to trust the harness's own clock. On the Postgres side:

```sql
SELECT calls, round(mean_exec_time, 1) AS mean_ms, round(max_exec_time, 1) AS max_ms, query
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 10;
```

On the ClickHouse side:

```sql
SELECT count(), round(quantile(0.5)(query_duration_ms), 1) AS p50, round(quantile(0.95)(query_duration_ms), 1) AS p95
FROM system.query_log
WHERE type = 'QueryFinish' AND event_time > now() - INTERVAL 10 MINUTE;
```

## Tests

Offline and deterministic: no database, no subprocess, no network, no real sleeping.

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest tests -q
```

`requirements-dev.txt` deliberately omits `asyncpg`. `run.py` imports the driver lazily
inside `run_benchmark()`, so the parsing, percentile, results-assembly and rendering code
all import on a machine with no driver -- which is also why `python3 run.py --help` works
there.

## How the writer is driven, and why not the obvious way

`app/writer.py` is spawned as a subprocess with its stdout on a pipe, and a dedicated
thread drains that pipe for the whole run.

Leaving the pipe unread until the end is the obvious shape and it is wrong twice over.
First, an unread pipe fills -- 64 KB is a typical buffer, and a long run at a short report
interval gets there -- at which point the writer's own `print` blocks inside its event loop
and its throughput collapses. That corrupts the exact number this harness leads with, and
it looks like contention. Second, iterating the pipe after `terminate()` blocks forever if
the writer does not exit, and a `wait(timeout=...)` placed after such a loop never gets a
chance to fire.

With the reader on its own thread, stopping the writer is `terminate()`, a bounded `wait()`,
a `kill()` if that expires, and then a join -- and the final summary line the writer prints
on SIGTERM cannot be lost to a race, because EOF has already arrived by the time the
process is reaped.

The writer's stderr is deliberately *not* piped. Its first-failure message and its
pacing-restart warning go straight to your terminal, which is how a run that produced a
beautiful TPS out of nothing but failed transactions gets caught. The failure count is in
the results file as well, and it raises a warning.
