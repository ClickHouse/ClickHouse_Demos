# Dynamic pricing release rehearsal

Implementation and local verification are complete through the published playbooks and CI kit. The workshop is **not ready for a public session** until the external gates below pass.

## Evidence available

- Local verification: `local-verification.json` records real SQL, model, container, browser, telemetry, catalog, build, and route checks. Local CDC rows are explicitly test replay.
- Prepared materials: `materials-preparation.json` records independent checks of the actual 1M and 100M Parquet tiers. Generated files are local and outside Git.
- Prepared model: `pricing/artifacts/prepared/preparation.json`, `model.json`, and `validation.json` record training from the manifested 1M cohort in real local ClickHouse. Both demand and synthetic decision gates passed. Distribution remains `release_candidate`.
- Local benchmark files in `benchmarks/results/` retain fixed facts, exact queries/parameters, real query IDs, cache conditions, sample counts, version/lock metadata, and missing-metric labels. They measure local imports and queries, not S3 or managed CDC.

## Open delivery gates

| Gate | Current status | Needed evidence |
| --- | --- | --- |
| S3 publication and learner read access | Passed for both tiers in partner-workshop, ap-southeast-1 | `s3-publication.json` records verified manifests and every object; core server-side S3 loading passed locally |
| Public or approved repository distribution | Unauthenticated GitHub returned 404 | Tested learner clone/download route for the reviewed code |
| Dedicated Cloud organization | Awaiting explicit organization/region | Owned ClickHouse and managed Postgres IDs, quota, CA, creation timing |
| Managed Postgres ClickPipe | Not rehearsed on Cloud | Actual four-table mapping, NULLs, replacement metadata, insert/correction/delete convergence |
| ClickStack delivery | Verified locally only | Actual snapshot/receipt trace IDs in the owned Cloud source |
| Windows Ubuntu WSL 2 | Literal contract passed; actual machine unavailable | Full tool, CA mount, browser, reset/restart, and cleanup rehearsal |
| 180-minute pacing and costs | Not measured end to end | Actual prework, creation/import timings, cadence, sizing, and observed cost |
| Cloud recovery exports | Not available | Redacted decision, source/CDC, receipt, and trace evidence with verified links |

S3 publication used the user-selected existing bucket and profile. Cloud and Windows configuration remain pending. No Cloud service or S3 bucket was guessed or created. No recorded episode was counted as a live provider or connector success.

## Run the actual Cloud harness

Follow both learner setup and the provisioning guide with a dedicated owner named `rehearsal-*`. Configure the running local app against those same owned resources and the published HTTPS manifest. Use a valid prepared model, actual ClickPipe, and actual ClickStack receiver. Then run from the host lab directory:

```bash
.venv/bin/python scripts/rehearse-cloud.py
```

The harness verifies exact owned endpoints before writes, uses the learner API session, checks source retry and stale-input rejection, controlled correction/delete convergence, learner application retry, receipt-price next sale, decision facts, real snapshot trace IDs, labeled replay, and five resets. Its source correction/delete exercise is restricted to the dedicated transport generation and is reset before pricing review.

This harness is not live-Cloud-tested yet. It returns nonzero on incomplete dependencies and writes a private failed record. A successful automated record still leaves fresh provisioning timing, visual/browser review, restart, actual sizing/costs, and the complete 180-minute pace as manual checks. Run the host entry path on macOS and real Ubuntu WSL 2; shell-text assertions cannot substitute.

Preview owned cleanup after saving evidence. Execute deletion only for those saved IDs. Shared synthetic dataset objects remain outside Cloud service cleanup. Promote release links and the model only after every required gate has evidence.

Before the dedicated correction/deletion rehearsal, export `PRICING_REHEARSAL_ADMIN_DSN` from a private credential source. Use the same source host, database, verified TLS, and CA with a separate administrator role. The harness verifies the endpoint and UPDATE/DELETE privileges before writes. Do not add this credential to the application environment or broaden `pricing_app` privileges.
