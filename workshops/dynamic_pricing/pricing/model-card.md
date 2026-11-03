# Prepared retail demand model

The chain-wide workshop uses synthetic calibration data to estimate how demand changes when a price changes. ClickHouse SQL combines these prepared coefficients with current sales, stock, expiry, and permitted prices. Learners inspect the calculation; they do not train the model.

## Production scope

A production team can use historical transactions in ClickHouse and features such as price, promotions, calendars, availability, store/product attributes, and weather to build training data for demand or revenue models. Model training, production feature selection, deployment, and retraining are outside this workshop. Here, ClickHouse ingestion and analytical execution are the learning focus.

## How it is prepared

The generator creates 28 trading days of randomized prices with ample stock, separately from business sales. Each of 20 products has a fitted coefficient for each of four store groups. A Poisson model uses a log link, trading-hour exposure, and a known calendar factor. The price coefficient is bounded between -6 and 0. Zero-sales open intervals count as observations; closed intervals, missing exposure, and stockout-censored counts do not.

Whole dates are split chronologically at 60% and 80%, rounded down: 16 days train, 6 validate, and 6 test. The report records split boundaries, supported prices, group coverage, fitting-code and data hashes, observed and predicted units, and mean absolute error. A release requires complete catalog coverage and aggregate predicted/observed units between 0.9 and 1.1 on both validation and test data. The evaluation stream uses a separate seed from calibration.

## How price comparison works

For each permitted price, SQL multiplies demand at the current price by the price ratio raised to the elasticity coefficient. Low, typical, and high scenarios multiply demand by 0.5, 1, and 1.5, with weights 0.25, 0.5, and 0.25. These are sensitivity assumptions, not confidence intervals.

Each scenario caps sales at available stock before averaging. Net recovery is receipts minus variable selling costs and disposal costs. Full contribution also subtracts the acquisition cost of all starting stock. Incremental profit compares net recovery with keeping the current price. Ties keep the current price. ClickHouse evaluates fractional demand with Float64; money enters as integer minor units, and SQL results are checked against an independent Decimal reference within one cent.

Historical generation records a rate normalization factor. Apply it once when using calibration reference rates to forecast the generated chain. Demand estimated directly from historical sales already contains that scale and must not be multiplied again. Calibration records never enter sales totals.

## How approved prices create observed transactions

Forecasting and transaction generation are separate components. Before approval, ClickHouse uses the fitted coefficients to estimate price options. After approval, the workbench commits the price and receipt in Managed Postgres. The writer replaces ordinary generation for the chosen store/SKU with a paired applied-price and unchanged-price stream.

For each batch, `pair_rows` in `app/backend/pricing_lab/retail_outcomes.py` scales captured baseline demand to the batch's share of remaining business time. It explicitly uses the synthetic environment's fixed elasticity of **−2**, not the fitted elasticity. The approved-price mean is the unchanged-price mean multiplied by `(approved_price / old_price)^-2`. Reducing SGD 8 to SGD 6 gives approximately 1.78 times the expected demand before stock limits.

`paired_demand` converts a shared uniform random draw into two Poisson counts. Each is capped by a separate stock balance. Each sold unit becomes an individual transaction row with branch, price, and receipt ID. Applied rows reduce operational stock; comparison rows affect only the comparison balance. The writer inserts these rows into ClickHouse, where materialized views maintain minute aggregates and `retail_outcome_series` computes receipt-linked running totals from raw sales and CDC evidence.

The generator shares captured baseline inputs with the forecast and uses a predefined response. It is a controlled teaching environment, not an independent test of predictive accuracy or real-world pricing effects.

## Interpretation and limits

A markdown can recover more value from excess stock before expiry. When stock is scarce, the unchanged price can win. The preparation report checks both outcomes using fitted parameters. Projections describe synthetic demand, not demonstrated revenue uplift for a retailer. Randomized prices and complete exposure make this exercise identifiable; real observational prices can be confounded by promotions, seasonality, and stockouts. Unsupported products or prices must be held for review.

## Earlier workshop model

The retained two-store exercise uses a separate 90-day artifact and a two-window simulator. Its artifacts are not interchangeable with the chain-wide model.
