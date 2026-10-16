# Prepare and publish synthetic materials

Run from the lab directory with the locked Python 3.12 venv. Generation streams bounded Parquet shards; bytes stay outside Git:

```bash
.venv/bin/python -m generator.generate --tier core --rows 1000000 --seed 17 --output .run/core
.venv/bin/python -m generator.generate --tier showcase --rows 100000000 --seed 17 --output .run/showcase
```

Use empty output directories. Both full tiers share the same complete 90-day calibration events and availability intervals. Operational events fill the requested exact total. Seed, schema, cutover, file counts, bytes, signed monetary totals, and SHA-256 checksums are in each manifest.

## Publish to an explicit destination

Set `PRICING_DATASET_BUCKET`, `PRICING_DATASET_PREFIX`, `AWS_REGION`, and the approved `AWS_PROFILE` or credential source. Use separate versioned tier prefixes. No bucket or account is guessed; the publisher creates no bucket and changes no access policy.

```bash
.venv/bin/python -m generator.publish --manifest .run/core/manifest.json --verify-anonymous
```

The CLI uses bounded standard SDK retries and eight upload workers by default; `--workers` accepts 1–16. `--list-existing` optionally lists the selected prefix to skip missing-object HEAD requests; it requires `s3:ListBucket`. Listing is only a hint: conditional writes and service checksum verification still decide every result, including races. Data files can upload in parallel, but the manifest waits for every checksum verification. Local independent Parquet counts and sums precede writes. Uploads use service-validated SHA-256 and conditional `If-None-Match: *`. Existing identical bytes are reused. Conflicting or unverifiable objects stop publication. The manifest is uploaded and verified only after all declared objects pass. See the [S3 PutObject contract](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html) and [checksum-enabled HeadObject contract](https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html).

The function's authenticated object verification does not prove learner download access. `--verify-anonymous` separately resolves the returned HTTPS manifest and verifies its files without AWS credentials. Keep that gate open on failure. The primary ClickHouse `s3()` loader uses `NOSIGN` for public synthetic materials without learner AWS keys. Read checks retry transient network/server failures up to four attempts, including partial response bodies. Permission denial and checksum mismatches still stop verification.

`infra/s3-read-policy.json` is a **template for the synthetic dataset prefix only**. Replace its bucket/prefix placeholders and merge its statement into the reviewed bucket policy. Its only permission is `s3:GetObject`; it grants anonymous reads because the primary workshop loader uses public URLs. Do not apply it to private data or replace unrelated policy statements. Check the bucket's public-access controls and delivery configuration. The publisher does not apply this policy.

No staging cleanup deletes immutable objects. A failed run may leave verified objects without a completed manifest; a retry reuses them. Change the dataset version/prefix for different bytes.

## Prepare the model

Load the core manifest into an isolated preparation ClickHouse/PG instance, then run:

```bash
.venv/bin/python -m pricing.prepare --manifest "$PRICING_DATASET_MANIFEST" --output pricing/artifacts/prepared
```

Training exports complete eligible hourly rows from ClickHouse. Inspect both demand and synthetic decision validation before promotion. A locally prepared artifact is a release candidate, not evidence of S3 distribution or managed Cloud operation. Preserve source manifest hash, model hash, feature/SQL hashes, date splits, supported ranges, and validation.

## Published datasets

The verified release uses `partner-workshop` in AWS Singapore (`ap-southeast-1`):

- [Core: 1,000,000 sales rows](https://s3.ap-southeast-1.amazonaws.com/partner-workshop/dynamic-pricing/2026-10-10/core/manifest.json)
- [Showcase: 100,000,000 sales rows](https://s3.ap-southeast-1.amazonaws.com/partner-workshop/dynamic-pricing/2026-10-10/showcase/manifest.json)

Every declared object passed service SHA-256 and anonymous access checks. See `reports/s3-publication.json`. The 1M tier also passed a real local `INSERT SELECT FROM s3(..., NOSIGN, 'Parquet')` import and reconciliation. Managed Cloud performance remains unverified.

For this transfer, native AWS CLI conditional uploads completed objects after repeated SDK connection resets. Each upload supplied `--if-none-match '*'` and `--checksum-sha256` from the independently verified manifest. Service checksum checks resolved ambiguous acknowledgements. The manifest was published only after all data objects passed, then the complete remote manifest was verified anonymously.
# Chain-wide retail history

Generate the `retail-chain-v2` dataset with Python 3.12 from the lab directory:

```bash
.venv/bin/python -m generator.retail --rows 100000000 --seed 17 --output .run/retail-chain-v2
```

The generator writes deterministic Parquet shards of at most 100,000 sales lines,
5,000 stores, 20 ready-to-eat products, operational starting inventory, a shared
trading calendar, and separate randomized price-calibration intervals. The 28-day
history covers 10 a.m.–6 p.m. Asia/Singapore; stored timestamps use UTC. Each basket
contains up to two lines. Returns have their own transaction ID and reference an
earlier sale. Sales-line counts and transaction counts are reported separately.

`availability.parquet` describes open, ample-stock hours for every store/product
combination, including combinations with zero sales. It is compact calendar data;
expand it against the catalog when deriving exposure. `history_rate_scale` records
the operational sampling normalization. Calibration is an independent experiment
for estimating price response and is excluded from operational sales totals.

Use `--rows 100000` for a smoke run. Both small and full artifacts contain the full
catalog, so a small sample is sparse and is not the workshop's performance dataset.
Output must be an empty directory. Manifests contain checksums, counts, monetary
totals, exposure metadata, and version identifiers for immutable publication.
The existing `retail-v1` generator and objects remain available for the previous lab.
