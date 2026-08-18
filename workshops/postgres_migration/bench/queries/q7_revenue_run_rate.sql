SELECT day, revenue, avg(revenue) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d
FROM (
  SELECT date_trunc('day', placed_at) AS day, sum(line_total) AS revenue
  FROM order_items
  WHERE placed_at >= now() - interval '90 days'
  GROUP BY 1
) daily
ORDER BY day
