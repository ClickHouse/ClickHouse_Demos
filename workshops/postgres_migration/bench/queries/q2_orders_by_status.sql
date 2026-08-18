SELECT date_trunc('day', placed_at) AS day, status, count(*) AS orders
FROM orders
WHERE placed_at >= now() - interval '30 days'
GROUP BY 1, 2
ORDER BY 1, 2
