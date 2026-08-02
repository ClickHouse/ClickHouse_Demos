# NYC Taxi Snowflake Migration Lab — lab artifacts

This directory holds the scripts, Terraform, dbt projects, producers, and Superset assets
you run during the ClickHouse Snowflake Migration Lab. It is not the instructions.

## Start here

The playbook lives at **https://workshop.demohouse.cloud/docs/snowflake-migration**.

Begin with module 00, which covers the toolchain, the two cloud trial accounts, and the
virtualenvs. Each module tells you which files here to run and in what order. Working
through this directory without the playbook will not go well: the order matters, and
several steps depend on state an earlier module created.

## What is here

| Directory | What it is |
|---|---|
| `01-setup-snowflake/` | Terraform, SQL, dbt project, trip producer, and Superset stack for the Snowflake source environment |
| `02-plan-and-design/` | Profiling scripts, five worksheets you fill in, and `migration-plan.md` |
| `03-migrate-to-clickhouse/` | ClickHouse Cloud Terraform, the migration script, the ClickHouse dbt project, benchmark and cutover scripts |
| `04-evaluation/` | `assessment.md`, the blank assessment template |

The worksheets, `migration-plan.md`, and `assessment.md` are yours to edit. Everything
else you run as-is.

## Credentials

Copy the `.example` templates and fill in your own values:

- `01-setup-snowflake/.env.example` and `03-migrate-to-clickhouse/.env.example`
- `01-setup-snowflake/terraform/terraform.tfvars.example`
- `01-setup-snowflake/dbt/nyc_taxi_dbt/profiles.yml.example` and
  `03-migrate-to-clickhouse/dbt/nyc_taxi_dbt_ch/profiles.yml.example`

**Never commit the filled-in files.** `.env`, `*.tfvars`, `profiles.yml`, and Terraform
state files hold real credentials and are gitignored here for that reason. Terraform state
in particular stores secrets in plaintext.
