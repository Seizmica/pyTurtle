#!/usr/bin/env bash
# Submit one job to YARN in cluster mode.
#
#   ./submit.sh customer prod 2026-09-25
#   ./submit.sh customer dev  2026-09-25 --dry-run
#
# Cluster mode runs the driver on a cluster node, so the submitting machine
# needs only spark-submit and a JRE — no PySpark, and no Python newer than
# whatever it happens to have.
set -euo pipefail
cd "$(dirname "$0")"

JOB="${1:?usage: submit.sh <job> <env> <run-date> [extra job args]}"
ENV="${2:?usage: submit.sh <job> <env> <run-date> [extra job args]}"
RUN_DATE="${3:?usage: submit.sh <job> <env> <run-date> [extra job args]}"
shift 3

[ -f "jobs/${JOB}.py" ] || { echo "no such job: jobs/${JOB}.py" >&2; exit 2; }
[ -f ".env.${ENV}" ] || { echo "no such environment: .env.${ENV}" >&2; exit 2; }

# The driver imports util/ from this archive. Both packages carry an
# __init__.py because zipimport cannot load PEP 420 namespace packages.
BUNDLE="$(mktemp -d)/etl.zip"
zip -qr "$BUNDLE" util jobs -x '*__pycache__*' '*.pyc'

# .env.<env> is delivered to the container working directory by --files, and
# ETL_CONFIG_ROOT points the config loader at it: inside the zip the source
# tree is not a real directory, so the default repo-relative lookup misses.
exec "${SPARK_HOME:?set SPARK_HOME to the Spark client matching the cluster}/bin/spark-submit" \
  --master "${SPARK_MASTER_URL:-yarn}" \
  --deploy-mode cluster \
  --name "${JOB}-${ENV}" \
  --queue "${YARN_QUEUE:-default}" \
  --py-files "$BUNDLE" \
  --files ".env.${ENV}" \
  --conf spark.yarn.appMasterEnv.ETL_CONFIG_ROOT=. \
  --conf spark.yarn.submit.waitAppCompletion=true \
  --driver-memory "${DRIVER_MEMORY:-2g}" \
  --executor-memory "${EXECUTOR_MEMORY:-4g}" \
  --executor-cores "${EXECUTOR_CORES:-2}" \
  --num-executors "${NUM_EXECUTORS:-4}" \
  ${SPARK_PACKAGES:+--packages "$SPARK_PACKAGES"} \
  ${KEYTAB:+--keytab "$KEYTAB" --principal "${PRINCIPAL:?PRINCIPAL is required with KEYTAB}"} \
  "jobs/${JOB}.py" \
  --env "$ENV" \
  --run-date "$RUN_DATE" "$@"
