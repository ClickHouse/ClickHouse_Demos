# Workshop Cloud resources

Follow [Module 00](https://dev-labs.demohouse.cloud/docs/dynamic-pricing/learner/00-setup) to install dependencies and authenticate ClickHouse CTL. From the workshop directory, run:

```bash
bash scripts/setup-cloud.sh
```

Setup detects your organization and public IPv4 address, creates or reuses a one-replica ClickHouse service and managed Postgres instance, configures application and replication roles, and creates the six-table operational CDC ClickPipe. It prepares the `pricing` database and analytical views. Progress messages report resource readiness.

Credentials and endpoints are saved privately in `app/.env.workshop`. Resource IDs are saved in `infra/resources.json`; retain this file for reuse and cleanup. Both are excluded from Git. Repeating setup reuses your resources and preserves scenario data.

## Cleanup

Stop the writer and local application using Module 06. Preview the owned Cloud resources:

```bash
bash scripts/setup-cloud.sh --destroy
```

After checking the resource IDs, delete them and their data:

```bash
bash scripts/setup-cloud.sh --destroy --execute
```

Cleanup preserves the shared S3 dataset. The implementation verifies resource ownership and journals confirmed deletions so an interrupted cleanup can resume with the same inventory.
