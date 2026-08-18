SELECT p.category,
       sum(i.line_total) AS revenue,
       round(100 * sum(i.line_total) / (SELECT sum(line_total) FROM order_items WHERE placed_at >= now() - interval '90 days'), 2) AS pct_of_revenue
FROM order_items i
JOIN products p ON p.product_id = i.product_id
WHERE i.placed_at >= now() - interval '90 days'
GROUP BY 1
ORDER BY revenue DESC
