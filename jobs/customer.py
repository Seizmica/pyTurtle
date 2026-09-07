"""Customer data feed — daily aggregate of spend per customer.

    python -m jobs.customer --env dev --run-date 2026-06-28
    python -m jobs.customer --env prod --run-date 2026-06-28 --dry-run

Owner: data-eng@company.com
"""

from util.runner import Job, main

# ${RUN_DATE-style tokens} are resolved from the .env file plus --run-date.
SQL = """
SELECT
    c.customer_id,
    c.name,
    c.segment,
    SUM(o.amount)                AS total_spend,
    COUNT(o.order_id)            AS order_count,
    MAX(o.ordered_at)            AS last_order_at,
    '${run_date}'                AS run_date
FROM customers c
JOIN orders o
  ON c.customer_id = o.customer_id
WHERE o.ordered_at >= '${run_date}'
  AND o.status = 'settled'
GROUP BY c.customer_id, c.name, c.segment
"""

CUSTOMER = Job(
    name="customer",
    sql=SQL,
    inputs={
        "customers": "${RAW_ROOT}/customers/",
        "orders": {
            "path": "${RAW_ROOT}/orders/",
            "format": "parquet",
            "options": {"mergeSchema": "true"},
        },
    },
    output="${CURATED_ROOT}/customer/",
    format="parquet",
    mode="overwrite",
    partition_by=["run_date"],
    options={"compression": "snappy"},
    manifest=True,
)


if __name__ == "__main__":
    raise SystemExit(main(CUSTOMER))
