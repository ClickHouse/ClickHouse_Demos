# RTA Mini Workshop — Real-Time Market Analytics with ClickHouse

A ~90-minute, self-contained hands-on lab: participants sign up for ClickHouse
Cloud, load ~26.5M forex ticks from public object storage, run real
market-analytics queries, try the built-in AI Assistant and Agents, and
(optionally) run a live dashboard on their own data. Generic and partner-neutral.

## Where the pieces live

The workshops are served by the workshop site, so the **guide** is served from
there and only the **dashboard** (a separate service) lives here:

| Piece | Location | Served at |
|---|---|---|
| Participant guide (MDX docs tree) | `site/content/docs/rta-mini/` | `https://labs.demohouse.cloud/docs/rta-mini` |
| Optional live dashboard | `dashboard/` (this folder) | run locally with Docker — see `dashboard/README.md` |

The guide is linked from the workshops hub (the app's `/` route).

## The workshop data

The guide loads a public forex Parquet dataset via a ClickPipe and via a one-shot
`s3(...)` query:

```
https://partner-workshop.s3.ap-southeast-1.amazonaws.com/fx/ticks.parquet
```

> **Note.** The dataset lives in a neutral, public S3 bucket (`partner-workshop`
> in `ap-southeast-1`), so the lab is fully partner-neutral and works with both
> the ClickPipes S3 source and the `s3()` function (anonymous `GetObject` +
> `ListBucket`). To host your own copy, upload `fx/ticks.parquet` to a public
> bucket and update the two references in the guide
> (`site/content/docs/rta-mini/learner/load-data.mdx`, served at
> `https://labs.demohouse.cloud/docs/rta-mini`).

## Prerequisites (participants)

- A ClickHouse Cloud account (free trial — created during the lab).
- For the optional dashboard: Docker Desktop.
