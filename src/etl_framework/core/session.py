"""SparkSession builder driven entirely by config."""

from __future__ import annotations

from pyspark.sql import SparkSession

from ..config.schema import JobConfig, SparkSpec

# Spark confs required for Delta Lake support.
_DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": ("org.apache.spark.sql.delta.catalog.DeltaCatalog"),
}


def _needs_delta(cfg: JobConfig | None) -> bool:
    if cfg is None:
        return False
    formats = {cfg.output.format, *(i.format for i in cfg.inputs)}
    return "delta" in formats


def build_spark_session(spec: SparkSpec, cfg: JobConfig | None = None) -> SparkSession:
    """Build (or get) a SparkSession from a :class:`SparkSpec`.

    When any input or the output uses the ``delta`` format, the required
    Delta Lake extensions are configured automatically (and the delta jars
    are wired in when ``delta-spark`` is installed).
    """
    conf = dict(spec.conf)
    if _needs_delta(cfg):
        conf = {**_DELTA_CONF, **conf}

    builder = SparkSession.builder.appName(spec.app_name)
    if spec.master:
        builder = builder.master(spec.master)
    for key, value in conf.items():
        builder = builder.config(key, value)

    if _needs_delta(cfg):
        try:
            from delta import configure_spark_with_delta_pip

            builder = configure_spark_with_delta_pip(builder)
        except ImportError as exc:  # pragma: no cover - env dependent
            raise RuntimeError(
                "Delta format requested but 'delta-spark' is not installed. "
                "Install it with: pip install delta-spark"
            ) from exc

    return builder.getOrCreate()
