"""Compaction — merge a feed's small output files into files of about 50 MB.

Not a data feed: a maintenance job over another feed's output, kept here so
submit.sh runs it like any other job. See util/compact.py for how it works.

    ./submit.sh compact prod 2026-06-28 \
        --path '${CURATED_ROOT}/customer/run_date=${run_date}/' --format parquet
    python -m jobs.compact --env dev --run-date 2026-06-28 \
        --path '${EXPORT_ROOT}/customer/' --format csv --option header=true --dry-run

Single-quote --path so the shell leaves ${...} for the .env file to resolve.

Owner: data-eng@company.com
"""

from util.compact import main

if __name__ == "__main__":
    raise SystemExit(main())
