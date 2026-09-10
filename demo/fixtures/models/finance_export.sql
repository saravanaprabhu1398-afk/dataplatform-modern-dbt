-- What finance actually reads.
CREATE OR REPLACE TABLE finance_export AS
SELECT
    day,
    region,
    gross AS revenue,
    net   AS revenue_net
FROM daily_revenue;
