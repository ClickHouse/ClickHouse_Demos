SELECT c.region, sum(i.quantity) AS units, sum(i.line_total) AS revenue
FROM order_items i
JOIN orders o ON o.order_id = i.order_id
JOIN customers c ON c.customer_id = o.customer_id
WHERE i.placed_at >= now() - interval '30 days'
GROUP BY 1
ORDER BY revenue DESC
