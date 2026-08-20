"""IO helper utilities."""

from __future__ import annotations

from pathlib import Path


def read_text(path: str | Path) -> str:
    """Read a text file (e.g. a ``.sql`` file) as UTF-8."""
    return Path(path).read_text(encoding="utf-8")


def substitute_params(text: str, params: dict[str, object]) -> str:
    """Substitute ``${key}`` tokens in ``text`` with ``params`` values."""
    for key, value in params.items():
        text = text.replace(f"${{{key}}}", str(value))
    return text
