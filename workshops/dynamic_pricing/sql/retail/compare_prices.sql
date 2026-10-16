WITH inputs AS (__INPUT__),
expanded AS (
 SELECT *, arrayJoin(permitted_prices_minor) AS price_minor FROM inputs
),
scenarios AS (
 SELECT *, arrayJoin(arrayZip(demand_multipliers,scenario_weights)) AS scenario FROM expanded
),
weighted AS (
 SELECT price_minor, current_price_minor, stock_units, unit_cost_minor, selling_cost_minor, disposal_cost_minor,
 sum(least(toFloat64(stock_units),base_demand_units*scenario.1*
     pow(toFloat64(price_minor)/current_price_minor,elasticity))*scenario.2) AS units
 FROM scenarios GROUP BY price_minor,current_price_minor,stock_units,unit_cost_minor,selling_cost_minor,disposal_cost_minor
),
economics AS (
 SELECT *, units*price_minor AS receipts_minor, stock_units-units AS waste_units,
 units*(price_minor-selling_cost_minor)-(stock_units-units)*disposal_cost_minor AS net_recovery_minor,
 net_recovery_minor-stock_units*unit_cost_minor AS contribution_minor FROM weighted
)
SELECT price_minor,units,receipts_minor,waste_units,net_recovery_minor,contribution_minor,
 net_recovery_minor-maxIf(net_recovery_minor,price_minor=current_price_minor) OVER () AS incremental_profit_minor,
 row_number() OVER (ORDER BY net_recovery_minor DESC,price_minor=current_price_minor DESC,price_minor DESC)=1 AS recommended
FROM economics ORDER BY price_minor DESC
LIMIT 100
