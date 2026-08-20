"""Structured JSON logging with a per-run correlation id."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    """Render log records as single-line JSON."""

    def __init__(self, run_id: str) -> None:
        super().__init__()
        self.run_id = run_id

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "run_id": self.run_id,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        extra = getattr(record, "extra_fields", None)
        if extra:
            payload.update(extra)
        return json.dumps(payload)


def configure_logging(run_id: str, level: str = "INFO") -> logging.Logger:
    """Configure the root ``etl_framework`` logger and return it."""
    logger = logging.getLogger("etl_framework")
    logger.setLevel(level.upper())
    logger.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(run_id))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def log(logger: logging.Logger, level: str, message: str, **fields) -> None:
    """Log a message with arbitrary structured fields."""
    logger.log(
        getattr(logging, level.upper()),
        message,
        extra={"extra_fields": fields},
    )
