# Pilot readiness

Use the workshop to test the data path and review process before considering real price changes.

1. Verify historical checksums, row counts, monetary totals, and rollup reconciliation.
2. Confirm source-to-destination revisions for both sales and inventory.
3. Capture a consistent snapshot and inspect its versions, cutoff, and freshness.
4. Compare candidates with the current-price baseline. Inspect demand variability and parameter sensitivity separately.
5. Confirm that expired, unsupported, or stale proposals cannot be applied.
6. Approve a feasible mock price as the owning learner. Verify its receipt and the next sale's price/version.
7. Export the review evidence and investigate a failure in ClickStack.

The synthetic model validation does not demonstrate business uplift. A real pilot needs representative demand data, treatment of stockout censoring and price assignment, calibrated uncertainty, and an explicit business evaluation design. The workshop applies prices only to its mock sales system.
