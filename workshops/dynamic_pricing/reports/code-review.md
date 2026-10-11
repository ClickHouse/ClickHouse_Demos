# Implementation review

An independent read-only review covered `983a8b5..0e018c8` against approved design revision 5 and the implementation plan. It found no Critical issues, six Important issues, and two Minor issues. All eight findings were accepted and addressed in one fix pass.

| Finding | Resolution | Verification |
| --- | --- | --- |
| Fresh service lacks the analytical database | Initialization creates the validated database through `default` before its first analytical connection. | Real ClickHouse test starts with an absent database and repeats bootstrap. |
| Lab 1 Baseline requires later CDC views | Baseline explicitly reconciles historical raw and rollup facts only. Snapshot capture keeps its live barrier. | Real SQL and API test opens Baseline without any CDC tables; capture remains blocked. |
| Rehearsal expects an omitted sale revision | Sale responses include the committed inventory revision ID. | API contract regression and actual receipt-to-sale integration. |
| Rehearsal deletes through the restricted application role | Controlled correction/deletion uses a separately configured administrator connection on the verified dedicated source. It checks UPDATE/DELETE privileges before writes. | Endpoint/role/TLS guard tests and actual Postgres grant checks. Managed Cloud execution remains open. |
| Failed model validation replaces the accepted model | Rejected artifacts and diagnostics use separate files; accepted files change only after both gates pass. Runtime rejects explicit failed gates. | Failed preparation preserves accepted bytes; runtime rejects both failed gates; real SQL preparation succeeds. |
| Role switch retains container credentials | Instructions recreate backend/writer and inspect the connected role. | Documentation checks; actual managed role-switch rehearsal remains open. |
| Unsupported subject reports a dependency outage | Validate supported subject before looking up price bounds. | API returns structured `model_unsupported`. |
| Unrelated AISRE runtime changed | Restore AISRE Python 3.11; retain the existing labs Python 3.12. | Scoped workflow diff. |

The review declined to judge actual S3 publication/download, managed CDC, Cloud telemetry, Windows execution, provisioning timing, costs, and complete pacing because external configuration or machines were unavailable. These remain delivery gates in [rehearsal.md](rehearsal.md). Real retailer applicability and production availability remain outside workshop scope. Optional live-provider success requires external access; the teaching replay remains labeled. The ongoing 100M local benchmark was outside the reviewed commit range and receives separate measured evidence.

Regression tests first reproduced the API omissions, rejected-artifact activation, missing database bootstrap, and pre-CDC dependency failures. The verification report records the final local test counts. No second reviewer was dispatched.
