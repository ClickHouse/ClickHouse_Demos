# Public learner kit

Source: ClickHouse/WorkshopHouse, commit `8c9ba85` (dynamic pricing implementation and reviewed fixes).

This directory publishes the runnable lab at `workshops/dynamic_pricing/`. Application code, SQL, fixtures, model artifacts, dependency locks, and lab tests match the maintained implementation. The public README links to the hosted playbook. Site-only `scripts/check-docs.py` and `tests/test_docs.py`, plus internal learner-review/handoff reports, stay in WorkshopHouse. The public `check-kit.sh` runs the same lab checks without the site-documentation gate.

Only tracked source files are published. Private environment files, resource inventories, generated Parquet datasets, local dependencies, exports, and scratch runs are excluded. Historical datasets remain at the published S3 manifests.

The paired WorkshopHouse PR provides the learner/instructor playbook and Use-Case catalog registration. Public session readiness still requires the remaining live Cloud, Windows, and pacing evidence described in `reports/rehearsal.md`.
