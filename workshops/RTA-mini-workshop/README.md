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
https://s3.housemate.click/fx/ticks.parquet
```

> **Note.** The dataset is served from a neutral custom domain
> (`s3.housemate.click`), so the lab is fully partner-neutral. To host your own
> copy, point a bucket at `fx/ticks.parquet` and update the two references in the
> guide (`public/rta-mini/index.html`).

## Prerequisites (participants)

- A ClickHouse Cloud account (free trial — created during the lab).
- For the optional dashboard: Docker Desktop.
