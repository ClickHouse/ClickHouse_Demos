SELECT date_trunc('day', placed_at) AS day, round(avg(order_total), 2) AS avg_order_value
FROM (
  SELECT order_id, placed_at, sum(line_total) AS order_total
  FROM order_items
  WHERE placed_at >= now() - interval '30 days'
  GROUP BY 1, 2
) per_order
GROUP BY 1
ORDER BY 1
