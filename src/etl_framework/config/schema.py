"""Config schema and validation using pydantic.

Validation happens before any Spark work so the job fails fast and loud on
malformed configuration.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid")


class JobMeta(_Base):
    name: str
    description: str = ""
    owner: str = ""


class InputSpec(_Base):
    table: str
    path: str
    format: str = "parquet"
    options: dict[str, str] = Field(default_factory=dict)


class OutputSpec(_Base):
    path: str
    format: Literal["parquet", "delta", "csv", "json", "orc"] = "parquet"
    # append | overwrite | merge  (merge is delta-only, upsert on merge_keys)
    mode: str = "overwrite"
    partition_by: list[str] = Field(default_factory=list)
    options: dict[str, str] = Field(default_factory=dict)
    # Write to a staging path then atomically commit to `path`. Applies to
    # overwrite mode only; append writes go direct (staging cannot be merged
    # atomically without duplicating existing data).
    atomic: bool = True
    # Optional expected output schema: column name -> Spark simpleString type
    # (e.g. "string", "bigint", "double"). Validated before the write.
    expected_schema: dict[str, str] = Field(default_factory=dict)
    # Business/natural keys used to match rows for incremental `mode: merge`.
    merge_keys: list[str] = Field(default_factory=list)


class TtlOutputSpec(_Base):
    enabled: bool = False
    path: str | None = None
    namespace: str = "http://example.com/ontology#"
    subject_column: str | None = None
    mapping: dict[str, str] = Field(default_factory=dict)
    fail_on_error: bool = False

    @field_validator("path", "subject_column")
    @classmethod
    def _required_when_enabled(cls, v, info):  # noqa: ANN001
        return v


class AudOutputSpec(_Base):
    """`.aud` manifest listing an MD5 checksum per written output file."""

    enabled: bool = False
    # Defaults to a sibling of the output path: "<output.path>.aud".
    path: str | None = None
    # text -> `<md5>  <relative path>` lines (verifiable with `md5sum -c`).
    format: Literal["text", "json"] = "text"
    # Include Spark side-car files (_SUCCESS, _delta_log/, .crc).
    include_hidden: bool = False
    fail_on_error: bool = False


class ManifestOutputSpec(_Base):
    """Per-partition ``manifest.json`` describing the shards written that run.

    One manifest is written *inside* each partition directory the run produced
    (``<output.path>/run_date=2026-06-28/manifest.json``), listing every data
    shard with its URI, size, and row count.
    """

    enabled: bool = False
    # File name written inside each partition directory. It lives alongside the
    # data, so downstream readers must glob for data files (e.g. "*.parquet")
    # rather than loading the directory wholesale.
    filename: str = "manifest.json"
    # Per-shard row counts require re-reading the written output once.
    row_counts: bool = True
    # Shard `uri`: fully qualified, or relative to the partition directory.
    uri_style: Literal["absolute", "relative"] = "absolute"
    # Stamped into the manifest; defaults to the framework version.
    app_version: str | None = None
    # strftime pattern for the framework-stamped `checkpoint_to`. The rendered
    # value must be all digits — it is stored as an integer.
    checkpoint_format: str = "%Y%m%d%H%M%S"
    # Explicit overrides. Set either to bypass watermark tracking (backfills,
    # or a scheduler that owns the watermark itself).
    checkpoint_from: int | None = None
    checkpoint_to: int | None = None
    # `checkpoint_from` for the first run, when no prior manifest exists.
    initial_checkpoint: int | None = None
    # How many partition directories to scan back for the previous watermark.
    lookback_partitions: int = 32
    include_hidden: bool = False
    fail_on_error: bool = False


class SparkSpec(_Base):
    app_name: str = "etl_framework"
    master: str | None = None
    conf: dict[str, str] = Field(default_factory=dict)


class QualityCheck(_Base):
    type: str
    columns: list[str] = Field(default_factory=list)
    value: Any = None
    # For `referential`: the input view + column child values must exist in.
    ref_table: str | None = None
    ref_column: str | None = None
    # For `custom`: a SQL predicate that must hold for every row. Rows failing
    # `WHERE NOT (<expr>)` are counted as violations. `{table}` is substituted
    # with the temp view name the result is registered under.
    expression: str | None = None
    # Optional human-readable name for reporting.
    name: str | None = None


class DataQualitySpec(_Base):
    enabled: bool = False
    fail_on_error: bool = True
    checks: list[QualityCheck] = Field(default_factory=list)


class LoggingSpec(_Base):
    level: str = "INFO"


class MetricsSpec(_Base):
    # Row counts trigger full scans; allow disabling them for large jobs.
    row_counts: bool = True


class AuditSpec(_Base):
    # Persist a per-run audit record: resolved config, SQL, git commit,
    # lineage, timings, and row counts.
    enabled: bool = False
    path: str = "audit"


class RetriesSpec(_Base):
    max_attempts: int = 1
    backoff_seconds: float = 5.0


class JobConfig(_Base):
    job: JobMeta
    sql_file: str
    sql_params: dict[str, Any] = Field(default_factory=dict)
    inputs: list[InputSpec]
    output: OutputSpec
    ttl_output: TtlOutputSpec = Field(default_factory=TtlOutputSpec)
    aud_output: AudOutputSpec = Field(default_factory=AudOutputSpec)
    manifest_output: ManifestOutputSpec = Field(default_factory=ManifestOutputSpec)
    environment: str = "dev"
    spark: SparkSpec = Field(default_factory=SparkSpec)
    data_quality: DataQualitySpec = Field(default_factory=DataQualitySpec)
    logging: LoggingSpec = Field(default_factory=LoggingSpec)
    metrics: MetricsSpec = Field(default_factory=MetricsSpec)
    audit: AuditSpec = Field(default_factory=AuditSpec)
    retries: RetriesSpec = Field(default_factory=RetriesSpec)


def validate_config(raw: dict[str, Any]) -> JobConfig:
    """Validate a merged config dict, raising on any schema violation."""
    cfg = JobConfig.model_validate(raw)
    if cfg.ttl_output.enabled:
        if not cfg.ttl_output.path or not cfg.ttl_output.subject_column:
            raise ValueError("ttl_output.enabled requires 'path' and 'subject_column'")
    if cfg.output.mode == "merge":
        if cfg.output.format != "delta":
            raise ValueError("output.mode 'merge' requires format 'delta'")
        if not cfg.output.merge_keys:
            raise ValueError("output.mode 'merge' requires 'merge_keys'")
    if cfg.manifest_output.enabled:
        _validate_manifest(cfg.manifest_output)
    return cfg


def _validate_manifest(spec: ManifestOutputSpec) -> None:
    """Fail before Spark starts on a manifest config that cannot produce a file."""
    if "/" in spec.filename or "\\" in spec.filename or not spec.filename:
        raise ValueError("manifest_output.filename must be a bare file name")
    stamped = datetime.now(timezone.utc).strftime(spec.checkpoint_format)
    if not stamped.isdigit():
        raise ValueError(
            "manifest_output.checkpoint_format must render digits only, "
            f"got {stamped!r} from {spec.checkpoint_format!r}"
        )
    if spec.lookback_partitions < 0:
        raise ValueError("manifest_output.lookback_partitions must not be negative")
