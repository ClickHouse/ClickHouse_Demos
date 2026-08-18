SELECT p.sku, p.category, sum(i.quantity) AS units, sum(i.line_total) AS revenue
FROM order_items i
JOIN products p ON p.product_id = i.product_id
WHERE i.placed_at >= now() - interval '30 days'
GROUP BY 1, 2
ORDER BY revenue DESC
LIMIT 20
