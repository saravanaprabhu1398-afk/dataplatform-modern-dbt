-- Revenue by day and region, net of discounts.
CREATE OR REPLACE TABLE daily_revenue AS
SELECT
    DATE_TRUNC('day', o.created_at)      AS day,
    c.region                             AS region,
    SUM(o.amount)                        AS gross,
    SUM(o.amount) - COALESCE(SUM(d.value), 0) AS net,
    COUNT(*)                             AS orders
FROM stg_orders o
JOIN customers c ON c.id = o.customer_id
LEFT JOIN discounts d ON d.order_id = o.id
GROUP BY 1, 2;
