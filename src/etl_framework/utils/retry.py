"""Retry with exponential backoff."""

from __future__ import annotations

import time
from typing import Callable, TypeVar

T = TypeVar("T")


class RetryError(RuntimeError):
    """Raised when all retry attempts are exhausted."""


def with_retry(
    fn: Callable[[], T],
    max_attempts: int = 1,
    backoff_seconds: float = 5.0,
    on_retry: Callable[[int, Exception], None] | None = None,
) -> T:
    """Call ``fn`` retrying on exception with exponential backoff."""
    attempt = 0
    last_exc: Exception | None = None
    while attempt < max_attempts:
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            attempt += 1
            if attempt >= max_attempts:
                break
            if on_retry:
                on_retry(attempt, exc)
            time.sleep(backoff_seconds * (2 ** (attempt - 1)))
    raise RetryError(f"Failed after {max_attempts} attempt(s)") from last_exc
