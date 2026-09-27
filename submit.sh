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
#   SPARK_JARS           jar paths to ship, comma-separated (local or hdfs://)
#   JARS_DIR             a directory whose *.jar files are all shipped
#   SPARK_PACKAGES       resolve jars from Maven instead, e.g.
#                        io.delta:delta-spark_2.12:3.2.0 -- prefer SPARK_JARS in
#                        cluster mode, where Ivy resolution runs in the driver
#                        container and stalls there if it cannot reach a mirror
#   KEYTAB / PRINCIPAL   let the driver renew its own Kerberos tickets
#   SPARK_FILES          extra files to ship, comma-separated (hive-site.xml,
#                        log4j2.properties, ...); each lands in the container
#                        working directory and is referred to by basename
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
# this one rather than add to it. Every entry is localized into the container
# working directory, so anything referring to one uses its basename — never the
# submitting machine's path, which does not exist on the cluster node.
FILES=".env.${ENV}"
EXTRA=()

_add_file() {
  FILES="${FILES},$1"
}

# Config the cluster does not already provide: hive-site.xml when the metastore
# is not on the AM classpath, a log4j2.properties, a krb5.conf.
if [ -n "${SPARK_FILES:-}" ]; then
  IFS=',' read -r -a FILE_ENTRIES <<< "$SPARK_FILES"
  for FILE in "${FILE_ENTRIES[@]}"; do
    [ -n "$FILE" ] || continue
    case "$FILE" in
      *://*) ;;  # hdfs://, s3a://, ... — resolved by the cluster, not here
      *) [ -f "$FILE" ] || { echo "file not found: $FILE" >&2; exit 2; } ;;
    esac
    _add_file "$FILE"
  done
fi

if [ -n "${TRUSTSTORE:-}" ]; then
  [ -f "$TRUSTSTORE" ] || { echo "TRUSTSTORE not found: $TRUSTSTORE" >&2; exit 2; }
  _add_file "$TRUSTSTORE"
  TLS_OPTS="-Djavax.net.ssl.trustStore=$(basename "$TRUSTSTORE")"
  if [ -n "${TRUSTSTORE_PASSWORD:-}" ]; then
    TLS_OPTS="${TLS_OPTS} -Djavax.net.ssl.trustStorePassword=${TRUSTSTORE_PASSWORD}"
  fi
  # Both sides need it: the driver for metastore and object-store calls, the
  # executors for the reads and writes they do themselves.
  EXTRA+=(--conf "spark.driver.extraJavaOptions=${TLS_OPTS}")
  EXTRA+=(--conf "spark.executor.extraJavaOptions=${TLS_OPTS}")
fi

# --jars, like --files, takes ONE comma-separated list. Local paths are uploaded
# on every submission; an hdfs:// path is localized by YARN without that
# re-upload, which is worth doing for jars that do not change between runs.
JARS=""
_add_jar() {
  if [ -z "$JARS" ]; then JARS="$1"; else JARS="${JARS},$1"; fi
}

if [ -n "${JARS_DIR:-}" ]; then
  [ -d "$JARS_DIR" ] || { echo "JARS_DIR is not a directory: $JARS_DIR" >&2; exit 2; }
  FOUND=0
  for JAR in "$JARS_DIR"/*.jar; do
    [ -f "$JAR" ] || continue
    _add_jar "$JAR"
    FOUND=1
  done
  [ "$FOUND" = 1 ] || { echo "no .jar files under $JARS_DIR" >&2; exit 2; }
fi

if [ -n "${SPARK_JARS:-}" ]; then
  # Split the caller's list so a typo fails here rather than as a
  # ClassNotFoundException once the driver is already running on the cluster.
  IFS=',' read -r -a JAR_ENTRIES <<< "$SPARK_JARS"
  for JAR in "${JAR_ENTRIES[@]}"; do
    [ -n "$JAR" ] || continue
    case "$JAR" in
      *://*) ;;  # hdfs://, s3a://, ... — resolved by the cluster, not here
      *) [ -f "$JAR" ] || { echo "jar not found: $JAR" >&2; exit 2; } ;;
    esac
    _add_jar "$JAR"
  done
fi

# In cluster mode --jars reaches the driver and the executors both, which is
# what Delta needs: build_spark sets spark.sql.extensions and the catalog, but
# the classes behind them have to be on the classpath.
[ -n "$JARS" ] && EXTRA+=(--jars "$JARS")

[ -n "${SPARK_PACKAGES:-}" ] && EXTRA+=(--packages "$SPARK_PACKAGES")

if [ -n "${KEYTAB:-}" ]; then
  EXTRA+=(--keytab "$KEYTAB" --principal "${PRINCIPAL:?PRINCIPAL is required with KEYTAB}")
fi

# ETL_CONFIG_ROOT points the config loader at the working directory where
# --files delivers .env.<env>: inside the zip the source tree is not a real
# directory, so the default repo-relative lookup misses.
#
# SPARK_MASTER is overridden for the same reason .env.dev can keep local[*] for
# local runs: build_spark calls builder.master() unconditionally, which would
# replace the master spark-submit set. A local master in the AM container
# builds a SparkContext that never registers with the ApplicationMaster, and
# YARN fails the app with exit code 13. The process environment beats a
# declared .env key, so passing it here is enough.
exec "${SPARK_HOME:?set SPARK_HOME to the Spark client matching the cluster}/bin/spark-submit" \
  --master "${SPARK_MASTER_URL:-yarn}" \
  --deploy-mode cluster \
  --name "${JOB}-${ENV}" \
  --queue "${YARN_QUEUE:-default}" \
  --py-files "$BUNDLE" \
  --files "$FILES" \
  --conf spark.yarn.appMasterEnv.ETL_CONFIG_ROOT=. \
  --conf "spark.yarn.appMasterEnv.SPARK_MASTER=${SPARK_MASTER_URL:-yarn}" \
  --conf spark.yarn.submit.waitAppCompletion=true \
  --driver-memory "${DRIVER_MEMORY:-2g}" \
  --executor-memory "${EXECUTOR_MEMORY:-4g}" \
  --executor-cores "${EXECUTOR_CORES:-2}" \
  --num-executors "${NUM_EXECUTORS:-4}" \
  ${EXTRA[@]+"${EXTRA[@]}"} \
  "jobs/${JOB}.py" \
  --env "$ENV" \
  --run-date "$RUN_DATE" "$@"
