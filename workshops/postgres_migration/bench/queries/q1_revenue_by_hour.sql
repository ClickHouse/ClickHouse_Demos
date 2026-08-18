SELECT date_trunc('hour', placed_at) AS hour, sum(line_total) AS revenue
FROM order_items
WHERE placed_at >= now() - interval '7 days'
GROUP BY 1
ORDER BY 1
