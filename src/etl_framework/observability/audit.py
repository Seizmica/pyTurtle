"""Per-run audit record: config snapshot, SQL, git commit, and lineage.

Persists a reproducibility record for each run so any output can be traced
back to the exact config and code that produced it.
"""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..config.schema import JobConfig
from ..observability.metrics import RunMetrics
from ..utils.io_utils import read_text


def _git_commit() -> str | None:
    """Best-effort current git commit hash; ``None`` outside a repo."""
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        return out.stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def build_audit_record(cfg: JobConfig, metrics: RunMetrics) -> dict[str, Any]:
    """Assemble the audit record for a completed (or planned) run."""
    try:
        sql_text = read_text(cfg.sql_file)
    except OSError:
        sql_text = None
    return {
        "run_id": metrics.run_id,
        "job_name": cfg.job.name,
        "environment": cfg.environment,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
        "git_commit": _git_commit(),
        "sql_file": cfg.sql_file,
        "sql": sql_text,
        "config_snapshot": cfg.model_dump(mode="json"),
        "metrics": metrics.to_dict(),
    }


def write_audit_record(cfg: JobConfig, metrics: RunMetrics) -> str:
    """Write the audit record to ``<audit.path>/<job>/<run_id>.json``.

    Returns the path written.
    """
    out_dir = Path(cfg.audit.path) / cfg.job.name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{metrics.run_id}.json"
    record = build_audit_record(cfg, metrics)
    out_file.write_text(json.dumps(record, indent=2), encoding="utf-8")
    return str(out_file)
