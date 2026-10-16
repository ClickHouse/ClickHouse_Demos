# Retail data context

The workshop represents 5,000 stores across 20 districts, with 250 stores per district,
and 20 perishable SKUs. Every attendee owns a dedicated service. No attendee filter is
needed. Money is integer SGD minor units in events; modeled expectations can be fractional.
Business timestamps use UTC. Business time is synthetic and can differ from wall time.

| Object | Meaning |
| --- | --- |
| `retail_analysis_context` | Active run UUID, completed exclusive minute cutoff, model version, wall-clock delivery age in seconds. Read this first. |
| `retail_opportunities(run, district, cutoff)` | Ranked inputs per store/SKU: stock, expiry, current price, historical exposure-adjusted demand, recent demand factor, excess units, model support, source versions and eligibility. |
| `retail_price_comparison(run, store, sku, cutoff, model)` | Permitted prices expanded over demand scenarios; expected units, receipts, waste, recovery, contribution, incremental profit, recommendation. Only ready inputs produce rows. |
| `retail_outcome_series(run, receipt)` | Minute-by-minute cumulative observed units, receipts, stock, waste and contribution for applied and unchanged-price branches. |
| `retail_sales` | Immutable historical and live transaction lines; negative quantities are returns. `transaction_id` groups basket lines; `event_id` identifies a line. |
| `retail_sales_minute` | Store/SKU/minute aggregate states for opportunity drilldown. Use `sumMerge` and `uniqExactMerge`. |
| `retail_district_minute` | District/SKU/minute aggregate states for chain analytics. |
| `retail_*_history_cdc` | Operational inventory, price and policy histories replicated from managed Postgres. Versions and effective sequence identify applicable state. |
| `retail_receipts_cdc` | Acknowledged price application receipts; `receipt_id` links observed events. |
| `retail_model_parameters`, `retail_demand_scenarios` | Released elasticity parameters and sensitivity weights. |

`branch='applied'` counts chain performance. `branch='comparison'` is the paired synthetic
unchanged-price simulation and must not inflate chain KPIs. Filter live events by the
active `run_id`; historical rows are shared within this attendee's service.

The prepared model fits a constrained Poisson demand relationship to independent,
randomized synthetic price calibration observations with ample stock. Calendar and
exposure offsets account for selling time. A chronological holdout checks calibration.
The model is a teaching component; its synthetic validation does not prove real-world
price elasticity. The SQL combines historical availability-adjusted demand, recent
sales, and the fitted price response, then caps each scenario at stock before averaging.

Parameterized views require parameters even for schema inspection. Substitute actual
values returned by context into the quoted placeholders below:

```sql
SELECT run_id, cutoff, model_version, delivery_age_seconds
FROM pricing.retail_analysis_context LIMIT 1;

DESCRIBE TABLE (SELECT * FROM pricing.retail_price_comparison(
 run=toUUID('RUN_UUID'), store=1, sku=101,
 cutoff=toDateTime64('CUTOFF_UTC', 3, 'UTC'), model='MODEL_VERSION'));

SELECT store_id, sku, stock_units, current_price_minor, base_demand_units,
 excess_units, eligibility, inventory_version, price_version, rule_version
FROM pricing.retail_opportunities(
 run=toUUID('RUN_UUID'), district=1,
 cutoff=toDateTime64('CUTOFF_UTC', 3, 'UTC'))
ORDER BY excess_units DESC, store_id, sku LIMIT 10;

SELECT price_minor, units, receipts_minor, waste_units, net_recovery_minor,
 contribution_minor, incremental_profit_minor, recommended
FROM pricing.retail_price_comparison(
 run=toUUID('RUN_UUID'), store=1, sku=101,
 cutoff=toDateTime64('CUTOFF_UTC', 3, 'UTC'), model='MODEL_VERSION')
ORDER BY price_minor LIMIT 20;

SELECT minute, branch, price_minor, units, receipts_minor, remaining_stock,
 waste_units, expired, contribution_minor
FROM pricing.retail_outcome_series(
 run=toUUID('RUN_UUID'), receipt=toUUID('RECEIPT_UUID'))
ORDER BY minute, branch LIMIT 1000;
```

A LIMIT bounds output, not input work. The connection's execution, scan and memory
limits bound the query. A limit or query failure means evidence is incomplete.
