-- Staging: one row per order, filtered to real money.
CREATE OR REPLACE TABLE stg_orders AS
SELECT
    id,
    customer_id,
    amount,
    created_at,
    UPPER(status) AS status
FROM orders
WHERE amount > 0;
