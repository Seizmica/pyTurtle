"""Utilities for storing Spark output in an S3 bucket via the S3A connector.

Credentials, region, and endpoint are sourced from the environment (or an
explicit :class:`S3Options`) — never hardcoded — matching the framework's
"secrets come from env vars" principle. Only values that are actually present
are applied, so instance-profile / IAM-role credentials keep working when no
static keys are supplied.

The DataFrame write itself reuses :func:`core.writer.write_output`, so S3
output inherits the same atomic staged commit, partitioning, and multi-format
handling as any other target.

Typical use::

    from etl_framework.utils.s3 import write_to_s3
    write_to_s3(df, cfg.output)              # creds/region from environment

or configure the session once and let the normal pipeline write::

    from etl_framework.utils.s3 import configure_s3
    configure_s3(spark)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, SparkSession

from ..config.schema import OutputSpec
from ..core.writer import write_output

_S3_SCHEMES = ("s3://", "s3a://", "s3n://")


def is_s3_path(path: str) -> bool:
    """Return ``True`` if ``path`` targets S3 (``s3://``, ``s3a://``, ``s3n://``)."""
    return path.lower().startswith(_S3_SCHEMES)


def to_s3a(path: str) -> str:
    """Normalize an ``s3://`` / ``s3n://`` URI to the ``s3a://`` scheme.

    The Hadoop S3A connector (``hadoop-aws``) serves the ``s3a`` scheme; this
    lets configs keep the familiar ``s3://`` form while writes go through S3A.
    """
    for scheme in ("s3://", "s3n://"):
        if path.lower().startswith(scheme):
            return "s3a://" + path[len(scheme) :]
    return path


@dataclass
class S3Options:
    """S3A connection settings, resolved into ``fs.s3a.*`` Hadoop configs.

    All fields are optional: omit static credentials to fall back to the
    default AWS credential provider chain (IAM role, profile, etc.).
    """

    access_key: str | None = None
    secret_key: str | None = None
    session_token: str | None = None
    region: str | None = None
    endpoint: str | None = None
    # Required for most S3-compatible stores (MinIO, Ceph) and some VPC setups.
    path_style_access: bool = False
    # Server-side encryption: e.g. "SSE-S3" or "SSE-KMS".
    sse_algorithm: str | None = None
    sse_kms_key_id: str | None = None
    # Escape hatch for any other fs.s3a.* option (keys given without the prefix).
    extra: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> S3Options:
        """Build options from standard AWS environment variables.

        Reads ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``,
        ``AWS_SESSION_TOKEN``, ``AWS_REGION`` (or ``AWS_DEFAULT_REGION``),
        ``AWS_S3_ENDPOINT``, and ``AWS_S3_PATH_STYLE_ACCESS``.
        """
        env = os.environ if env is None else env
        path_style = env.get("AWS_S3_PATH_STYLE_ACCESS", "").lower() in ("1", "true", "yes")
        return cls(
            access_key=env.get("AWS_ACCESS_KEY_ID"),
            secret_key=env.get("AWS_SECRET_ACCESS_KEY"),
            session_token=env.get("AWS_SESSION_TOKEN"),
            region=env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION"),
            endpoint=env.get("AWS_S3_ENDPOINT"),
            path_style_access=path_style,
        )

    def hadoop_conf(self) -> dict[str, str]:
        """Return the ``fs.s3a.*`` Hadoop configs implied by these options.

        Only populated fields are included, so unset credentials never clobber
        a working IAM-role or profile-based credential chain.
        """
        conf: dict[str, str] = {}
        if self.access_key:
            conf["fs.s3a.access.key"] = self.access_key
        if self.secret_key:
            conf["fs.s3a.secret.key"] = self.secret_key
        if self.session_token:
            conf["fs.s3a.session.token"] = self.session_token
            # A session token implies temporary STS credentials.
            conf["fs.s3a.aws.credentials.provider"] = (
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
            )
        if self.region:
            conf["fs.s3a.endpoint.region"] = self.region
        if self.endpoint:
            conf["fs.s3a.endpoint"] = self.endpoint
        if self.path_style_access:
            conf["fs.s3a.path.style.access"] = "true"
        if self.sse_algorithm:
            conf["fs.s3a.server-side-encryption-algorithm"] = self.sse_algorithm
        if self.sse_kms_key_id:
            conf["fs.s3a.server-side-encryption.key"] = self.sse_kms_key_id
        for key, value in self.extra.items():
            conf[key if key.startswith("fs.s3a.") else f"fs.s3a.{key}"] = value
        return conf


def configure_s3(spark: SparkSession, opts: S3Options | None = None) -> None:
    """Apply S3A settings to a live session's Hadoop configuration.

    Uses :meth:`S3Options.from_env` when ``opts`` is omitted. Safe to call more
    than once; it only sets keys that resolve to a value.
    """
    opts = S3Options.from_env() if opts is None else opts
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    for key, value in opts.hadoop_conf().items():
        hconf.set(key, value)


def write_to_s3(
    df: DataFrame,
    spec: OutputSpec,
    opts: S3Options | None = None,
    *,
    force_s3a: bool = True,
) -> None:
    """Store a Spark DataFrame in S3, honoring the configured format and mode.

    Configures the S3A connector on ``df``'s session (credentials/region from
    ``opts`` or the environment), normalizes the target to the ``s3a://``
    scheme when ``force_s3a`` is set, then delegates to
    :func:`core.writer.write_output` for the atomic staged commit, partitioning,
    and format handling.

    Raises:
        ValueError: if ``spec.path`` is not an S3 URI.
    """
    if not is_s3_path(spec.path):
        raise ValueError(f"write_to_s3 requires an s3:// path, got: {spec.path!r}")

    configure_s3(df.sparkSession, opts)

    target = spec
    if force_s3a:
        target = spec.model_copy(update={"path": to_s3a(spec.path)})

    write_output(df, target)
