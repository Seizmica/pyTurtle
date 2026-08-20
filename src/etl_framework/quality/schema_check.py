"""DataFrame schema validation.

Validates that a produced DataFrame matches an expected column -> type map
before it is written. Types are compared using Spark's ``simpleString``
representation (e.g. ``string``, ``bigint``, ``double``).
"""

from __future__ import annotations

from pyspark.sql import DataFrame


class SchemaValidationError(RuntimeError):
    """Raised when a DataFrame does not match the expected schema."""


def validate_schema(df: DataFrame, expected: dict[str, str]) -> None:
    """Validate ``df`` against an expected ``{column: type}`` mapping.

    Checks that every expected column is present with a matching type. Extra
    columns on the DataFrame are allowed (additive changes are non-breaking).
    Raises :class:`SchemaValidationError` on any mismatch.
    """
    if not expected:
        return

    actual = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    errors: list[str] = []
    for col, exp_type in expected.items():
        if col not in actual:
            errors.append(f"missing column '{col}'")
        elif actual[col] != exp_type:
            errors.append(f"column '{col}' type {actual[col]!r} != expected {exp_type!r}")
    if errors:
        raise SchemaValidationError("; ".join(errors))
