WITH
    61 AS selected_store,
    102 AS selected_sku,
    (SELECT tuple(run_id, cutoff)
     FROM pricing.retail_analysis_context LIMIT 1) AS context,
inputs AS (
    SELECT *
    FROM pricing.retail_opportunities(
        run = tupleElement(context, 1),
        district = toUInt16(intDiv(selected_store - 1, 250) + 1),
        cutoff = tupleElement(context, 2))
    WHERE store_id = selected_store AND sku = selected_sku
      AND eligibility = 'ready'
),
prices AS (
    SELECT *, arrayJoin(permitted_prices_minor) AS price_minor
    FROM inputs
),
scenarios AS (
    SELECT *, arrayJoin(arrayZip(demand_multipliers, scenario_weights)) AS scenario
    FROM prices
),
expected_sales AS (
    SELECT price_minor, current_price_minor, stock_units,
        unit_cost_minor, selling_cost_minor, disposal_cost_minor,
        sum(
            least(toFloat64(stock_units),
                base_demand_units * scenario.1 *
                pow(toFloat64(price_minor) / current_price_minor, elasticity)
            ) * scenario.2
        ) AS expected_units
    FROM scenarios
    GROUP BY price_minor, current_price_minor, stock_units,
        unit_cost_minor, selling_cost_minor, disposal_cost_minor
),
economics AS (
    SELECT *,
        stock_units - expected_units AS expected_unsold_units,
        expected_units * price_minor AS revenue_minor,
        expected_units * (price_minor - selling_cost_minor)
            - (stock_units - expected_units) * disposal_cost_minor AS net_recovery_minor,
        net_recovery_minor - stock_units * unit_cost_minor AS contribution_minor
    FROM expected_sales
)
SELECT
    selected_store AS store_id, selected_sku AS sku,
    tupleElement(context, 2) AS evidence_cutoff,
    stock_units, current_price_minor / 100 AS current_price_sgd,
    price_minor / 100 AS price_sgd,
    expected_units, expected_unsold_units,
    revenue_minor / 100 AS revenue_sgd,
    net_recovery_minor / 100 AS net_recovery_sgd,
    contribution_minor / 100 AS contribution_sgd,
    (net_recovery_minor - maxIf(net_recovery_minor,
        price_minor = current_price_minor) OVER ()) / 100 AS profit_change_sgd,
    row_number() OVER (ORDER BY net_recovery_minor DESC,
        price_minor = current_price_minor DESC, price_minor DESC) = 1 AS recommended
FROM economics
ORDER BY price_sgd DESC
LIMIT 100
SETTINGS use_query_cache = 0;
