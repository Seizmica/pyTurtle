# etl-framework

A **config-driven PySpark ETL framework**. Each job is declared entirely in
config + SQL — no engine code changes needed to onboard a new pipeline.

## Prerequisites: JDK

PySpark runs on the JVM, so a Java runtime is required (Java 11 or 17 is
recommended for Spark 3.4+). Without it you'll see
`[JAVA_GATEWAY_EXITED] Java gateway process exited` when a job or the
integration tests start.

1. **Install a JDK** — e.g. [Eclipse Temurin 17](https://adoptium.net/) or
   OpenJDK. On Windows: `winget install EclipseAdoptium.Temurin.17.JDK`.
2. **Set `JAVA_HOME`** to the install dir and add its `bin` to `PATH`:

   ```powershell
   # Windows (PowerShell) — adjust the path to your install
   setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-17"
   setx PATH "$env:PATH;%JAVA_HOME%\bin"
   ```

   ```bash
   # macOS / Linux
   export JAVA_HOME="$(/usr/libexec/java_home -v 17)"   # macOS
   export PATH="$JAVA_HOME/bin:$PATH"
   ```

3. **Verify** in a new shell:

   ```bash
   java -version
   ```

## Install

```bash
pip install -e ".[dev]"
```

## Run a job

```bash
python -m etl_framework.main \
  --config configs/jobs/customer_daily.yaml \
  --env dev \
  --run-date 2026-07-09 \
  --dry-run false
```

Config is layered: `configs/base.yaml` → `configs/env/<env>.yaml` →
`configs/jobs/<job>.yaml`, with `--param key=value` overrides applied last.
`${ENV_VAR}` and `${ENV_VAR:-default}` interpolation is supported; never
hardcode secrets.

## Execution flow

```
load_config → validate → build_spark → read_inputs (temp views)
  → pre/post data quality → spark.sql(transform) → write_output
  → [optional] TTL/RDF stage → [optional] .aud checksum stage
  → emit metrics + lineage
```

## Incremental loads (Delta merge)

Set `output.mode: merge` (Delta only) with `merge_keys` to upsert instead of
overwrite. The target table is created on first run, then matched rows are
updated and new rows inserted:

```yaml
output:
  path: "s3://curated/customer_daily/"
  format: delta
  mode: merge
  merge_keys: [customer_id]
```

## Data quality checks

Built-in check `type`s: `not_null`, `unique`, `row_count_min`, `referential`
(child values must exist in a registered input view), and `custom` (a SQL
predicate that must hold for every row):

```yaml
data_quality:
  enabled: true
  fail_on_error: true
  checks:
    - type: referential
      columns: [customer_id]
      ref_table: customers
      ref_column: customer_id
    - type: custom
      name: non_negative_spend
      expression: "total_spend >= 0"
```

## Output schema validation

Declare the expected output schema; the run fails before writing if a column
is missing or has the wrong type (extra columns are allowed):

```yaml
output:
  expected_schema:
    customer_id: bigint
    total_spend: double
```

## Audit & reproducibility

Enable per-run audit records (resolved config snapshot, SQL text, git commit,
lineage, timings, row counts) written to `<path>/<job>/<run_id>.json`:

```yaml
audit:
  enabled: true
  path: audit
```

## Output checksums (`.aud`)

Emit a `.aud` manifest holding an MD5 checksum for every file written, so
consumers can verify the delivery:

```yaml
aud_output:
  enabled: true
  path: null            # defaults to "<output.path>.aud"
  format: text          # text | json
  include_hidden: false # include _SUCCESS, _delta_log/, .crc side-cars
  fail_on_error: false  # soft-fail: a manifest error won't fail the run
```

The default `text` format is `md5sum`-compatible, with paths relative to the
output directory:

```
d41d8cd98f00b204e9800998ecf8427e  run_date=2026-07-09/part-00000.parquet
```

```bash
# verify a downloaded copy of the output
cd /data/customer_daily && md5sum -c ../customer_daily.aud
```

Use `format: json` for a structured manifest (`target`, `algorithm`,
`generated_at`, `file_count`, `files[]`). The manifest path is recorded in the
run's lineage, so it also lands in the audit record.

## Performance: skip row counts

Row-count metrics force full scans. Disable on large jobs:

```yaml
metrics:
  row_counts: false
```

## Add a pipeline

1. Write SQL in `sql/<job>.sql` (reference inputs by their `table` name).
2. Create `configs/jobs/<job>.yaml` (inputs, output, optional `ttl_output`).
3. Add data quality checks in the config.
4. Add a test under `tests/integration/`.
5. Do **not** modify `src/etl_framework/core/` for job-specific needs.

## Testing

```bash
ruff check src/ && black --check src/
pytest tests/unit -q
pytest tests/integration -q   # spins up a local SparkSession
```

## Releasing to Nexus

Versions come from git tags via `setuptools-scm` — there is no version string to
bump by hand. Tag `v1.2.3` builds `1.2.3`; any untagged commit builds a
developmental version (`1.2.4.dev5`) that routes to the snapshot repository.

Two artifacts are published per version:

| Artifact | Destination | Why |
|----------|-------------|-----|
| `etl_framework-<ver>-py3-none-any.whl` | hosted PyPI repo | the engine, `pip install`-able |
| `etl-framework-<ver>.zip` | raw repo, under `<raw_path>/<ver>/` | wheel **+** `configs/` + `sql/` + pinned `requirements.lock` |

The bundle exists because a wheel alone is not deployable: `--config` is a
filesystem path and `sql_file` is read from disk, so neither `configs/` nor
`sql/` is importable from the wheel. Shipping them under one version means
"prod is on 1.2.3" describes the engine *and* the SQL that ran.

### Configuring Nexus

Edit `[tool.nexus]` in `pyproject.toml`, or override any field per environment:

| `pyproject.toml` | Environment variable |
|------------------|----------------------|
| `url` | `NEXUS_URL` |
| `pypi_repository` | `NEXUS_PYPI_REPOSITORY` |
| `pypi_snapshot_repository` | `NEXUS_PYPI_SNAPSHOT_REPOSITORY` |
| `raw_repository` | `NEXUS_RAW_REPOSITORY` |
| `raw_path` | `NEXUS_RAW_PATH` |

Credentials are environment-only and never read from a file:
`NEXUS_USERNAME` / `NEXUS_PASSWORD`.

### CI

`.github/workflows/ci.yml` runs `test` → `package` → `publish`. The `package`
job builds and validates the artifacts on every push and PR (so packaging
breakage surfaces before release) and uploads nothing. The `publish` job runs
only on pushes to the default branch and on `v*` tags, and is skipped entirely
until the `NEXUS_URL` repository variable is set.

Set these in **Settings → Secrets and variables → Actions**: repository
*variables* `NEXUS_URL`, `NEXUS_PYPI_REPOSITORY`, `NEXUS_PYPI_SNAPSHOT_REPOSITORY`,
`NEXUS_RAW_REPOSITORY`, `NEXUS_RAW_PATH`; repository *secrets* `NEXUS_USERNAME`,
`NEXUS_PASSWORD` (a deploy service account, not a personal token).

To cut a release:

```bash
git tag v1.2.3 && git push origin v1.2.3
```

### Building and publishing by hand

```bash
pip install ".[release]"
python -m build                                              # wheel + sdist
python scripts/build_bundle.py --wheel dist/*.whl            # bundle zip + .sha256
python scripts/publish_nexus.py --dist dist --dry-run        # show the plan
python scripts/publish_nexus.py --dist dist                  # upload
```

## Deploying a release

**Install the engine** from the Nexus index (use a *group* repo so third-party
dependencies resolve through the same index):

```bash
pip install "etl-framework==1.2.3" --index-url https://nexus.example.com/repository/pypi-group/simple
```

**Or unpack the bundle** into a versioned directory and flip a symlink, so a
rollback is one `ln -sfn`:

```bash
curl -fO https://nexus.example.com/repository/raw-hosted/etl-framework/1.2.3/etl-framework-1.2.3.zip
sha256sum -c etl-framework-1.2.3.zip.sha256
unzip -q etl-framework-1.2.3.zip -d /opt/etl/releases/
ln -sfn /opt/etl/releases/etl-framework-1.2.3 /opt/etl/current
pip install /opt/etl/current/wheels/*.whl
```

Then schedule against a pinned version — never `current` — so a rollback does
not silently change what a scheduled run executes:

```bash
/opt/etl/releases/etl-framework-1.2.3/bin/run.sh \
  --config configs/jobs/customer_daily.yaml --env prod --run-date 2026-06-28
```

`run.sh` cd's to the release root first, so the relative `configs/` and `sql/`
paths in a job config resolve.

On a YARN cluster where executors have no `pip`, build a relocatable virtualenv
with `venv-pack` from `requirements.lock` and ship it with
`spark-submit --archives venv.tar.gz#env --conf spark.pyspark.python=./env/bin/python`.
`--py-files <wheel>` ships only the framework code, not its dependencies.

## Layout

| Path | Purpose |
|------|---------|
| `src/etl_framework/config/` | Load, merge, interpolate, validate config |
| `src/etl_framework/core/`   | session, reader, transformer, writer, pipeline |
| `src/etl_framework/quality/`| Data quality expectations + validators |
| `src/etl_framework/stages/` | Pluggable side-effect stages (TTL/RDF, `.aud` checksums) |
| `src/etl_framework/observability/` | JSON logging + run metrics/lineage |
| `src/etl_framework/utils/`  | retry, IO helpers |
