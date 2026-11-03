# Prepare the retail dataset

Workshop attendees use the pre-generated S3 dataset in [Module 01](https://dev-labs.demohouse.cloud/docs/dynamic-pricing/learner/01-retail-baseline). Generation and model preparation are maintainer tasks.

From the workshop directory with the Python 3.12 environment installed:

```bash
.venv/bin/python -m generator.retail --rows 100000000 --seed 17 --output .run/retail-showcase
.venv/bin/python -m pricing.retail_prepare --manifest .run/retail-showcase/manifest.json --output .run/retail-model
```

Use an empty output directory. A `--rows 100000` generation is suitable for a smoke check, but does not replace the 100M-row workshop dataset.

The retail catalog contains 5,000 stores, 20 districts, and 20 products. The manifest records file checksums, row counts, monetary totals, and dataset metadata. Availability records describe open, ample-stock intervals, including combinations with zero sales. Independent randomized calibration observations support model preparation and are excluded from operational sales totals.

Keep generated datasets under `.run/`. The workshop uses the immutable `retail-chain-v2/2026-10-16/showcase` release in the `partner-workshop` bucket in AWS Singapore (`ap-southeast-1`). Do not overwrite published objects when preparing another dataset version. The learner instructions contain the S3 loading SQL.
