# RTA Mini Workshop — Real-Time Market Analytics with ClickHouse

A ~90-minute, self-contained hands-on lab. Participants sign up for ClickHouse
Cloud, load ~26.5M forex ticks from public object storage, run real
market-analytics queries, try the built-in AI Assistant and Agents, and
(optionally) run a live dashboard on their own data.

Generic and partner-neutral — reuse it as-is for any partner or customer event.

## What's here

| Path | What |
|---|---|
| `index.html` | The participant guide — a single self-contained HTML page. Open it directly, or serve it (see below). References `images/`. |
| `images/` | Screenshots used by the guide. |
| `dashboard/` | Optional live forex dashboard (FastAPI + `clickhouse-connect` + ECharts), Dockerized. Points at the participant's own `forex` table. See `dashboard/README.md`. |

## Running the guide

The guide is a static page — no build step.

- **Locally:** open `index.html` in a browser.
- **Served:** host `index.html` + `images/` on any static host, or behind the
  workshops hub (see `workshops/hub/`). Participants follow it at their own pace.

## The workshop data

The guide loads a public forex Parquet dataset via a ClickPipe and via a one-shot
`s3(...)` query. The dataset currently lives at:

```
https://inox-techtalkthai-fsq-th-959934561610.s3.ap-southeast-1.amazonaws.com/fx/ticks.parquet
```

> **Note — the one partner-named artifact.** The S3 bucket name still references
> the original event. Everything else is genericized. For a fully partner-neutral
> lab, re-host `ticks.parquet` to a neutral public bucket and update the two
> references in `index.html` (the ClickPipe path and the `s3(...)` query).

## Prerequisites (participants)

- A ClickHouse Cloud account (free trial — created during the lab).
- For the optional dashboard: Docker Desktop.
