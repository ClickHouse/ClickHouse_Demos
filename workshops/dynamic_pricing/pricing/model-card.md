# Prepared demand model

This workshop uses a model trained offline on synthetic grocery transactions. Learners inspect its inputs and compare price candidates. They do not run model training.

## How demand estimates work

The model estimates units sold per hour from SKU, store, hour, day of week, exposure, and price relative to a fixed reference price. A Poisson model with a log link keeps demand positive. The price coefficient is constrained to zero or less, so a higher price cannot increase predicted demand for the same context.

The simulator uses these estimates to compare supported prices against the current price. It caps sales at available stock and subtracts selling and disposal costs. These projections describe the synthetic scenario. They do not establish revenue uplift for a real retailer.

## Training and validation

Maintainers generate 90 days of calibration data with randomized prices and ample stock. ClickHouse joins complete availability intervals to aggregated sales. Eligible hours with no sales remain in the training data. Returns and stockout intervals are excluded from demand labels.

The first 60 dates train the model. The next 15 select regularization. The final 15 remain untouched until evaluation. SciPy L-BFGS-B fits the constrained coefficients. Five seeded bootstrap fits resample whole training dates to illustrate parameter variability. Demand randomness and parameter variability are separate quantities.

Run the maintainer command against an initialized instance with a verified, active historical import:

```bash
.venv/bin/python -m pricing.prepare --manifest "$PRICING_DATASET_MANIFEST" --output pricing/artifacts/development
```

The output records the feature schema, training SQL hash, data hash, date splits, support bounds, fit convergence, held-out calibration, and deviance against a model without a price effect. Calibration must be between 0.9 and 1.1 overall and for declared groups with at least 500 observed units. The price model must improve held-out deviance.

## Release status and limits

The checked-in development artifact is not a released workshop model. Demand checks and decision checks are separate gates. Release requires supported-grid coverage, monotonic predictions, and candidate regret at most 5% against the known synthetic generator. The simulator uses identical random draws across candidate prices.

A failed fit is recorded as a failure; its coefficient is not clipped afterward. Unknown subjects, unsupported prices, invalid coefficients, and missing context hold a recommendation. Learners cannot override a held recommendation.

The model assumes randomized calibration prices, complete exposure, and no stock constraints in its training labels. Real pricing data may violate these assumptions. Training on observational transactions alone can confuse price response with promotions, seasonality, or stockouts.
