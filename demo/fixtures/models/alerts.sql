-- Anything that pages someone.
CREATE OR REPLACE TABLE alerts AS
SELECT
    day,
    region,
    net < 0 AS negative_revenue,
    orders  AS order_count
FROM daily_revenue;
