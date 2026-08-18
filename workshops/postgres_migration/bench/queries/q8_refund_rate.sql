SELECT c.region,
       count(*) AS orders,
       sum(CASE WHEN o.status = 'refunded' THEN 1 ELSE 0 END) AS refunded,
       round(100.0 * sum(CASE WHEN o.status = 'refunded' THEN 1 ELSE 0 END) / count(*), 2) AS refund_pct
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
WHERE o.placed_at >= now() - interval '90 days'
GROUP BY 1
ORDER BY refund_pct DESC
