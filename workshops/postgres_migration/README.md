# Postgres Migration Workshop lab code

This directory holds the code learners run on their own machine for the
**RDS Postgres to ClickHouse Managed Postgres** workshop: Terraform for the source RDS
instance, the SQL for schema/seed/publication/cutover, the OLTP order writer, the
dashboard benchmark harness, and Grafana provisioning.

**The instructions are not here.** Follow the published playbook at
[https://labs.demohouse.cloud/docs/postgres-migration](https://labs.demohouse.cloud/docs/postgres-migration).
It is the only supported path through the workshop; the files below are the artifacts
that playbook tells you to run, in the order it tells you to run them.

This is the copy you clone. Clone the repository plainly -- no branch switching -- and work
from this directory:

```bash
git clone https://github.com/ClickHouse/ClickHouse_Demos.git
cd "$(git rev-parse --show-toplevel)/workshops/postgres_migration"
```

## Layout

| Path | What it is |
| --- | --- |
| `terraform/` | The source AWS RDS Postgres instance, its parameter group and its security group |
| `sql/` | Schema, server-side seed, publication, the sequence fix, reconciliation, and the `pg_clickhouse` routing SQL |
| `app/` | The continuous OLTP order writer |
| `bench/` | The dashboard benchmark harness and the eight panel queries it replays |
| `grafana/` | Compose file and provisioning for the operations dashboard |
| `scripts/` | Preflight checks, including the panel-SQL identity check |

## Terraform

`terraform/` provisions one RDS Postgres 17 instance with `rds.logical_replication`
enabled, so it can act as a logical replication publication source.

`subscriber_cidrs` has **no default** and must be supplied. ClickHouse Managed Postgres
creates the `SUBSCRIPTION` on the target, so it dials out to this instance: the value is
the ClickHouse Cloud egress range for your region plus your own `/32`.

```bash
cd terraform
terraform init
terraform apply -var 'subscriber_cidrs=["203.0.113.4/32"]'
terraform output -raw rds_endpoint
```

Enabling `rds.logical_replication` is a static parameter, so it is applied with
`pending-reboot` and the instance must be rebooted before `wal_level` becomes `logical`.
That reboot is a deliberate part of the workshop, not an oversight: it is the first
planned-downtime beat, and module 03 verifies it with `SHOW wal_level`.

gp3 IOPS are left at the default baseline on purpose. Over-provisioning them would make
the "before" benchmark unrepresentative of the workload the migration is meant to fix.

The instance provisions real AWS infrastructure in your own account and bills for as long
as it runs. Tear it down when you are finished:

```bash
cd terraform && terraform destroy
```
