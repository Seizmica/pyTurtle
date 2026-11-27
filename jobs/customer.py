"""Customer data feed — daily aggregate of spend per customer.

    python -m jobs.customer --env dev --run-date 2026-06-28
    python -m jobs.customer --env prod --run-date 2026-06-28 --dry-run
    python -m jobs.customer --env dev        # run date defaults to today

Owner: data-eng@company.com
"""

from util.runner import Job, Output, main

# ${RUN_DATE-style tokens} are resolved from the .env file plus --run-date.
SQL = """
SELECT
    customer_id,
    '${run_date}'                AS run_date
FROM i_db.i_customers
"""

CUSTOMER = Job(
    name="customer",
    sql=SQL,
    inputs={
        # A Hive metastore table: the catalog knows its location and format.
        "customers": {"table": "i_db.i_customers"},
        # Files on HDFS: read with the DataFrameReader.
        "orders": {
            "path": "${RAW_ROOT}/orders/",
            "format": "parquet",
            "options": {"mergeSchema": "true"},
        },
    },
    outputs=[
        # Curated parquet for downstream jobs.
        Output(
            path="s3a://buscket/extract/customer/",
            format="parquet",
            mode="overwrite",
            partition_by=["run_date"],
            options={"compression": "snappy"},
            manifest=True,
            # At most 8 files per run_date directory instead of one per shuffle
            # partition. Tune to roughly output size / 128 MB.
            coalesce=8,
        ),
        # A csv copy for the business hand-off, on its own path. The SQL runs
        # once; the cached result is written twice.
        Output(
            path="${EXPORT_ROOT}/customer/",
            format="csv",
            mode="overwrite",
            partition_by=["run_date"],
            options={"header": "true", "compression": "gzip"},
            # The hand-off gets a single file per run_date.
            coalesce=1,
        ),
    ],
)


if __name__ == "__main__":
    raise SystemExit(main(CUSTOMER))
