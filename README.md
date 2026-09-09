# etl-lite

Project-specific PySpark jobs. **One script per data feed**, with its SQL
inline, driven by a per-environment `.env` file.

```
jobs/
  customer.py        one feed, its own SQL, its own declaration
  orders.py
util/
  config.py          .env loading and ${VAR} resolution
  runner.py          Spark session, read -> SQL -> write, manifest
.env.dev  .env.uat  .env.preprod  .env.prod
tests/
```

Two util files. Everything else is a job or a test.

## Run a job

```bash
pip install ".[dev,delta]"

python -m jobs.customer --env dev --run-date 2026-06-28 --dry-run
python -m jobs.customer --env prod --run-date 2026-06-28
```

Run from the repo root — `jobs/` and `util/` are imported as namespace
packages, which is why there is no `__init__.py` anywhere and why `-m` is used
instead of `python jobs/customer.py`.

`--env` is required unless `APP_ENV` is set. `--dry-run` reads the inputs, runs
the SQL, prints the plan and the row count, and writes nothing.

## Adding a feed

Copy `jobs/customer.py`, change the SQL and the `Job(...)`. No other file
changes.

```python
from util.runner import Job, Output, main

SQL = """
SELECT product_id, SUM(qty) AS units, '${run_date}' AS run_date
FROM shipments
WHERE shipped_at >= '${run_date}'
GROUP BY product_id
"""

SHIPMENTS = Job(
    name="shipments",
    sql=SQL,
    inputs={"shipments": "${RAW_ROOT}/shipments/"},
    outputs=[
        Output(
            path="${CURATED_ROOT}/shipments/",
            partition_by=["run_date"],
            manifest=True,
        ),
    ],
)

if __name__ == "__main__":
    raise SystemExit(main(SHIPMENTS))
```

Input keys become the temp view names the SQL selects from. `${...}` tokens in
paths, table names, and SQL resolve from `--run-date` first, then the `.env`
file, then the process environment.

### Inputs: files and Hive tables

An input is either a **file source** or a **metastore table**. A job can mix
both — `jobs/customer.py` joins a Hive table to parquet files on HDFS.

```python
inputs={
    # Hive table — the metastore knows its location and format.
    "customers": {"table": "${HIVE_DB}.customers"},

    # Files — any scheme Hadoop understands, hdfs:// included.
    "orders": {
        "path": "hdfs://nn/raw/orders/",
        "format": "parquet",
        "options": {"mergeSchema": "true"},
    },

    # Shorthand: a bare string is a path using the job's input_format.
    "returns": "${RAW_ROOT}/returns/",
}
```

Each entry needs **exactly one** of `path` or `table`; declaring both, or
neither, fails validation before Spark starts. `format` and `options` apply
only to file sources — passing them alongside `table` is rejected rather than
silently ignored, since the metastore already describes the table.

A job with any table input gets `enableHiveSupport()` automatically. The
metastore connection itself comes from the cluster's `hive-site.xml`; set
`SPARK_CONF.hive.metastore.uris` in the `.env` file only when it doesn't.

Partition pruning still works on table inputs: filters in the job's SQL push
down through the temp view into the table scan, so a `WHERE` on a partition
column does not read the whole table.

### Outputs: parquet, csv, or both

A job declares a list of `Output`s. Writing two formats is just two entries —
each with its **own path**, options, and partitioning. The SQL runs once; the
cached result is written to each destination in turn.

```python
outputs=[
    Output(
        path="${CURATED_ROOT}/customer/",
        format="parquet",
        partition_by=["run_date"],
        options={"compression": "snappy"},
        manifest=True,
    ),
    Output(
        path="${EXPORT_ROOT}/customer/",
        format="csv",
        partition_by=["run_date"],
        options={"header": "true", "compression": "gzip"},
    ),
]
```

Supported formats are `parquet`, `csv`, and `delta` — anything else fails
validation. `delta` exists for incremental `mode="merge"` loads; `parquet` and
`csv` are the plain formats.

Outputs must have distinct paths, or the second would clobber the first.

`options` are writer options and differ per format: `compression` for parquet,
`header`/`sep`/`quote` for csv. **Set `header: "true"` explicitly** if you want
one — Spark writes csv without a header by default.

### Job options

| Field | Default | Notes |
|-------|---------|-------|
| `inputs` | required | `{view: path}`, `{view: {path, format, options}}`, or `{view: {table}}` |
| `outputs` | required | List of `Output` — at least one |
| `input_format` | `parquet` | Default read format for file inputs that don't set one |

### Output options

| Field | Default | Notes |
|-------|---------|-------|
| `path` | required | Path template |
| `format` | `parquet` | `parquet`, `csv`, or `delta` |
| `mode` | `overwrite` | `append`, `overwrite`, or `merge` (Delta upsert) |
| `partition_by` | `[]` | Hive-style partition columns |
| `merge_keys` | `[]` | Required for `mode="merge"` |
| `options` | `{}` | Writer options for that format |
| `manifest` | `False` | Write `manifest.json` per partition |

## Environments

One file per environment — `dev`, `uat`, `preprod`, `prod`. Nothing is
inherited between them, so the effective value of a key is readable in one
place.

```bash
RAW_ROOT=s3a://raw-prod
CURATED_ROOT=s3a://curated-prod
EXPORT_ROOT=s3a://export-prod        # csv hand-off copies
HIVE_DB=analytics_prod
SPARK_MASTER=yarn
SPARK_CONF.spark.sql.shuffle.partitions=200
```

Any key prefixed `SPARK_CONF.` is passed to the session as a Spark conf, so
tuning stays out of the code.

**Secrets never go in these files.** A process environment variable overrides
the same key from the file, so a secret manager or CI injects them at run time:

```bash
DB_PASSWORD=... python -m jobs.customer --env prod --run-date 2026-06-28
```

Only keys the file declares can be overridden this way — an unrelated exported
shell variable never leaks into the config.

## manifest.json

With `manifest=True`, each partition directory the run produced gets:

```json
{
  "run_date": "2026-06-28",
  "checkpoint_from": 20260627150000,
  "checkpoint_to": 20260628150000,
  "row_count": 100,
  "app_version": "0.1.0",
  "shards": [
    {"index": 0, "uri": "s3a://curated-prod/customer/run_date=2026-06-28/part-0.parquet",
     "size_bytes": 41500000, "row_count": 20}
  ]
}
```

`checkpoint_from` is read from the newest prior partition's manifest;
`checkpoint_to` is stamped from the run clock. The lookup happens **before the
write**, because an overwrite would otherwise delete the very partitions the
watermark is read from. Set `CHECKPOINT_FROM` in the environment to override it
for a backfill.

Two consequences worth knowing:

- The manifest lives **inside** the data directory, so readers must glob for
  data files — `.../run_date=2026-06-28/*.parquet` — not load the directory.
  Spark auto-skips only files starting with `_` or `.`.
- Row counts come from re-reading the output once, so `manifest=True` costs an
  extra scan. Leave it off for feeds that don't need it.

The shipped `.env` files set
`spark.sql.sources.partitionOverwriteMode=dynamic` so a rerun replaces only the
partitions it produces. Without it, a static overwrite wipes the whole table —
including prior manifests, which breaks the checkpoint chain.

## Testing

```bash
ruff check jobs/ util/ tests/ && black --check jobs/ util/ tests/
pytest tests -q
```

`tests/test_run_local.py` needs a JDK (Java 11 or 17) and is skipped without
one. The rest run anywhere.

## Relationship to the full framework

This branch is a deliberately reduced alternative to the config-driven
framework on `main`. Dropped: YAML config layering, pydantic schema
validation, the pluggable stage system, data quality gates, `.aud` checksums,
TTL/RDF output, retries, audit records, and Nexus packaging. Kept: read → SQL →
write, Delta merge, and the per-partition manifest.

`main` still has all of it if you need a piece back.
