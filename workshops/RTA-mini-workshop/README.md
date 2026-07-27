# RTA Mini Workshop — Real-Time Market Analytics with ClickHouse

A ~90-minute, self-contained hands-on lab: participants sign up for ClickHouse
Cloud, load ~26.5M forex ticks from public object storage, run real
market-analytics queries, try the built-in AI Assistant and Agents, and
(optionally) run a live dashboard on their own data. Generic and partner-neutral.

## Where the pieces live

The workshops are served by a single Next app (the playbook), so the **guide** is
served from there and only the **dashboard** (a separate service) lives here:

| Piece | Location | Served at |
|---|---|---|
| Participant guide (`index.html` + `images/`) | `workshops/build_workshop/playbook/public/rta-mini/` | `/rta-mini/index.html` (via the playbook app) |
| Optional live dashboard | `dashboard/` (this folder) | run locally with Docker — see `dashboard/README.md` |

The guide is linked from the workshops hub (the app's `/` route).

## The workshop data

The guide loads a public forex Parquet dataset via a ClickPipe and via a one-shot
`s3(...)` query:

```
https://inox-techtalkthai-fsq-th-959934561610.s3.ap-southeast-1.amazonaws.com/fx/ticks.parquet
```

> **Note — the one partner-named artifact.** The S3 bucket name still references
> the original event. Everything else is genericized. For a fully partner-neutral
> lab, re-host `ticks.parquet` to a neutral public bucket and update the two
> references in the guide (`public/rta-mini/index.html`).

## Prerequisites (participants)

- A ClickHouse Cloud account (free trial — created during the lab).
- For the optional dashboard: Docker Desktop.
