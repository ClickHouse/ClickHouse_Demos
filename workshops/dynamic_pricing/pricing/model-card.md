# Prepared retail demand model

The chain-wide workshop uses synthetic calibration data to estimate how demand changes when a price changes. ClickHouse SQL combines these prepared coefficients with current sales, stock, expiry, and permitted prices. Learners inspect the calculation; they do not train the model.

## How it is prepared

The generator creates 28 trading days of randomized prices with ample stock, separately from business sales. Each of 20 products has a fitted coefficient for each of four store groups. A Poisson model uses a log link, trading-hour exposure, and a known calendar factor. The price coefficient is bounded between -6 and 0. Zero-sales open intervals count as observations; closed intervals, missing exposure, and stockout-censored counts do not.

Whole dates are split chronologically at 60% and 80%, rounded down: 16 days train, 6 validate, and 6 test. The report records split boundaries, supported prices, group coverage, fitting-code and data hashes, observed and predicted units, and mean absolute error. A release requires complete catalog coverage and aggregate predicted/observed units between 0.9 and 1.1 on both validation and test data. The evaluation stream uses a separate seed from calibration.

## How price comparison works

For each permitted price, SQL multiplies demand at the current price by the price ratio raised to the elasticity coefficient. Low, typical, and high scenarios multiply demand by 0.5, 1, and 1.5, with weights 0.25, 0.5, and 0.25. These are sensitivity assumptions, not confidence intervals.

Each scenario caps sales at available stock before averaging. Net recovery is receipts minus variable selling costs and disposal costs. Full contribution also subtracts the acquisition cost of all starting stock. Incremental profit compares net recovery with keeping the current price. Ties keep the current price. ClickHouse evaluates fractional demand with Float64; money enters as integer minor units, and SQL results are checked against an independent Decimal reference within one cent.

Historical generation records a rate normalization factor. Apply it once when using calibration reference rates to forecast the generated chain. Demand estimated directly from historical sales already contains that scale and must not be multiplied again. Calibration records never enter sales totals.

## Interpretation and limits

A markdown can recover more value from excess stock before expiry. When stock is scarce, the unchanged price can win. The preparation report checks both outcomes using fitted parameters. Projections describe synthetic demand, not demonstrated revenue uplift for a retailer. Randomized prices and complete exposure make this exercise identifiable; real observational prices can be confounded by promotions, seasonality, and stockouts. Unsupported products or prices must be held for review.

## Earlier workshop model

The retained two-store exercise uses a separate 90-day artifact and a two-window simulator. Its artifacts are not interchangeable with the chain-wide model.
