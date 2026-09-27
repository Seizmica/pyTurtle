#!/usr/bin/env bash
# Submit one job to YARN in cluster mode.
#
#   ./submit.sh customer prod 2026-09-25
#   ./submit.sh customer dev  2026-09-25 --dry-run
#
# Cluster mode runs the driver on a cluster node, so the submitting machine
# needs only spark-submit and a JRE — no PySpark, and no Python newer than
# whatever it happens to have.
#
# Optional environment:
#   YARN_QUEUE  DRIVER_MEMORY  EXECUTOR_MEMORY  EXECUTOR_CORES  NUM_EXECUTORS
#   SPARK_PACKAGES       extra jars, e.g. io.delta:delta-spark_2.12:3.2.0
#   KEYTAB / PRINCIPAL   let the driver renew its own Kerberos tickets
#   TRUSTSTORE           a cacerts file to ship and trust (see below)
#   TRUSTSTORE_PASSWORD  only if it is not the JDK default
set -euo pipefail
cd "$(dirname "$0")"

JOB="${1:?usage: submit.sh <job> <env> <run-date> [extra job args]}"
ENV="${2:?usage: submit.sh <job> <env> <run-date> [extra job args]}"
RUN_DATE="${3:?usage: submit.sh <job> <env> <run-date> [extra job args]}"
shift 3

[ -f "jobs/${JOB}.py" ] || { echo "no such job: jobs/${JOB}.py" >&2; exit 2; }
[ -f ".env.${ENV}" ] || { echo "no such environment: .env.${ENV}" >&2; exit 2; }

command -v zip >/dev/null || { echo "zip is required to bundle util/ and jobs/" >&2; exit 2; }

# The driver imports util/ from this archive. Both packages carry an
# __init__.py because zipimport cannot load PEP 420 namespace packages.
BUNDLE="$(mktemp -d)/etl.zip"
zip -qr "$BUNDLE" util jobs -x '*__pycache__*' '*.pyc'

# --files takes ONE comma-separated list; a second --files flag would replace
# this one rather than add to it.
FILES=".env.${ENV}"
EXTRA=()

# Every --files entry is localized into the container working directory, so the
# JVM and the config loader both refer to it by basename — never by the
# submitting machine's path, which does not exist on the cluster node.
if [ -n "${TRUSTSTORE:-}" ]; then
  [ -f "$TRUSTSTORE" ] || { echo "TRUSTSTORE not found: $TRUSTSTORE" >&2; exit 2; }
  FILES="${FILES},${TRUSTSTORE}"
  TLS_OPTS="-Djavax.net.ssl.trustStore=$(basename "$TRUSTSTORE")"
  if [ -n "${TRUSTSTORE_PASSWORD:-}" ]; then
    TLS_OPTS="${TLS_OPTS} -Djavax.net.ssl.trustStorePassword=${TRUSTSTORE_PASSWORD}"
  fi
  # Both sides need it: the driver for metastore and object-store calls, the
  # executors for the reads and writes they do themselves.
  EXTRA+=(--conf "spark.driver.extraJavaOptions=${TLS_OPTS}")
  EXTRA+=(--conf "spark.executor.extraJavaOptions=${TLS_OPTS}")
fi

[ -n "${SPARK_PACKAGES:-}" ] && EXTRA+=(--packages "$SPARK_PACKAGES")

if [ -n "${KEYTAB:-}" ]; then
  EXTRA+=(--keytab "$KEYTAB" --principal "${PRINCIPAL:?PRINCIPAL is required with KEYTAB}")
fi

# ETL_CONFIG_ROOT points the config loader at the working directory where
# --files delivers .env.<env>: inside the zip the source tree is not a real
# directory, so the default repo-relative lookup misses.
exec "${SPARK_HOME:?set SPARK_HOME to the Spark client matching the cluster}/bin/spark-submit" \
  --master "${SPARK_MASTER_URL:-yarn}" \
  --deploy-mode cluster \
  --name "${JOB}-${ENV}" \
  --queue "${YARN_QUEUE:-default}" \
  --py-files "$BUNDLE" \
  --files "$FILES" \
  --conf spark.yarn.appMasterEnv.ETL_CONFIG_ROOT=. \
  --conf spark.yarn.submit.waitAppCompletion=true \
  --driver-memory "${DRIVER_MEMORY:-2g}" \
  --executor-memory "${EXECUTOR_MEMORY:-4g}" \
  --executor-cores "${EXECUTOR_CORES:-2}" \
  --num-executors "${NUM_EXECUTORS:-4}" \
  ${EXTRA[@]+"${EXTRA[@]}"} \
  "jobs/${JOB}.py" \
  --env "$ENV" \
  --run-date "$RUN_DATE" "$@"
