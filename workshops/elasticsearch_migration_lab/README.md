# Elasticsearch Observability Migration Lab — artifacts

This directory contains the Docker, Terraform, OpenTelemetry, ClickHouse SQL, dashboards,
validation scripts, and editable design worksheets used by the Elasticsearch Observability
Migration workshop. The step-by-step instructions and interactive assessment live in the hosted
playbook.

## Start here

Open **https://labs.demohouse.cloud/docs/elasticsearch-migration** and begin with module 00.
Each module tells you which directory to enter and which artifact to run or edit.

## Contents

| Directory | Artifacts |
|---|---|
| `common/` | Environment template, architecture sources, and cleanup script |
| `part1/` | Elasticsearch, Kibana, Filebeat, APM Server, OTel Demo, dashboards, and validation |
| `part2/` | Editable data-model, query-translation, and ADR exercises |
| `part3/` | ClickHouse DDL, collector configs, parallel-run and cutover scripts, HyperDX screenshots, and SQL exercises |

## Credentials

Copy `.example` files to local ignored files and fill in your own values. Never commit
credentials, filled-in Terraform variables, or Terraform state. The repository's `.gitignore`
protects the expected local files, but you should still review `git status` before committing.
