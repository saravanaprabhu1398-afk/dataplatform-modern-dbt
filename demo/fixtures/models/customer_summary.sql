-- Deliberately unresolvable: a star over an asset the catalog does not know.
CREATE OR REPLACE TABLE customer_summary AS
SELECT * FROM crm_extract;
