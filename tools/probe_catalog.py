"""Report which catalog Spark actually reached, to a path you can read.

Standalone on purpose: it imports nothing from util/, so a result here says
something about the cluster rather than about this framework. Findings are
written to HDFS because the container logs that would otherwise carry them are
not always reachable.

    spark-submit --master yarn --deploy-mode cluster \
      --files /etc/hive/conf/hive-site.xml \
      tools/probe_catalog.py hdfs:///tmp/probe.json mydb

Then:  hdfs dfs -cat /tmp/probe.json
"""

import json
import sys

from pyspark.sql import SparkSession

KEYS = (
    "spark.sql.catalogImplementation",
    "spark.sql.warehouse.dir",
    "spark.hadoop.hive.metastore.uris",
    "spark.sql.hive.metastore.version",
    "spark.sql.hive.metastore.jars",
    "spark.yarn.appMasterEnv.PYSPARK_PYTHON",
)


def main(argv):
    if not argv:
        raise SystemExit("usage: probe_catalog.py <output-uri> [database]")
    target = argv[0]
    database = argv[1] if len(argv) > 1 else None

    spark = SparkSession.builder.appName("probe-catalog").enableHiveSupport().getOrCreate()
    context = spark.sparkContext
    hadoop = context._jsc.hadoopConfiguration()

    report = {
        "spark_version": spark.version,
        "python_version": sys.version.split()[0],
        "user": context.sparkUser(),
        "application_id": context.applicationId,
        # The conf as the session sees it, and the Hadoop conf underneath it:
        # hive-site.xml lands in the second, not the first, so a value here
        # with an empty counterpart above means the file was found.
        "conf": {key: spark.conf.get(key, None) for key in KEYS},
        "hadoop_metastore_uris": hadoop.get("hive.metastore.uris"),
        "hadoop_warehouse": hadoop.get("hive.metastore.warehouse.dir"),
        "hadoop_auth": hadoop.get("hadoop.security.authentication"),
    }

    try:
        report["databases"] = [row[0] for row in spark.sql("SHOW DATABASES").collect()]
    except Exception as exc:  # noqa: BLE001 - the failure is the finding
        report["databases_error"] = "{}: {}".format(type(exc).__name__, exc)

    if database:
        try:
            rows = spark.sql("SHOW TABLES IN {}".format(database)).collect()
            report["tables"] = [row[1] for row in rows]
        except Exception as exc:  # noqa: BLE001 - the failure is the finding
            report["tables_error"] = "{}: {}".format(type(exc).__name__, exc)

    body = json.dumps(report, indent=2, default=str)
    jvm = context._jvm
    path = jvm.org.apache.hadoop.fs.Path(target)
    stream = path.getFileSystem(hadoop).create(path, True)
    try:
        stream.write(bytearray(body.encode("utf-8")))
    finally:
        stream.close()

    print(body)
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1:])
