"""Orders data feed — incremental upsert into a Delta table.

Contrast with jobs/customer.py: this one loads incrementally (`mode="merge"`
upserts on the natural key) rather than rewriting a partition, so a late-
arriving correction updates the existing row instead of duplicating it.

    python -m jobs.orders --env uat --run-date 2026-06-28

Owner: data-eng@company.com
"""

from util.runner import Job, main

SQL = """
SELECT
    o.order_id,
    o.customer_id,
    o.amount,
    o.currency,
    o.status,
    o.ordered_at,
    '${run_date}' AS run_date
FROM orders o
WHERE o.ordered_at >= '${run_date}'
"""

ORDERS = Job(
    name="orders",
    sql=SQL,
    inputs={"orders": "${RAW_ROOT}/orders/"},
    output="${CURATED_ROOT}/orders/",
    format="delta",
    mode="merge",
    merge_keys=["order_id"],
    partition_by=["run_date"],
)


if __name__ == "__main__":
    raise SystemExit(main(ORDERS))
