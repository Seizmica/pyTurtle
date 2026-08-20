-- Daily customer aggregation.
-- Inputs are referenced by their `table` name from the job config.
SELECT
    c.customer_id,
    c.name,
    SUM(o.amount) AS total_spend,
    '${run_date}' AS run_date
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
