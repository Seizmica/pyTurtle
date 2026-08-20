"""Run metrics and lineage capture."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class RunMetrics:
    """Aggregates counts, timings, and lineage for a single run."""

    run_id: str
    job_name: str
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    input_rows: dict[str, int] = field(default_factory=dict)
    output_rows: int = 0
    stage_timings: dict[str, float] = field(default_factory=dict)
    lineage: dict[str, Any] = field(default_factory=dict)
    _stage_starts: dict[str, float] = field(default_factory=dict)

    def start_stage(self, name: str) -> None:
        self._stage_starts[name] = time.perf_counter()

    def end_stage(self, name: str) -> None:
        if name in self._stage_starts:
            self.stage_timings[name] = round(time.perf_counter() - self._stage_starts.pop(name), 4)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "job_name": self.job_name,
            "started_at": self.started_at,
            "input_rows": self.input_rows,
            "output_rows": self.output_rows,
            "stage_timings": self.stage_timings,
            "lineage": self.lineage,
        }
