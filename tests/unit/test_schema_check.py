"""Unit tests for DataFrame schema validation (Spark-free via a stub)."""

from dataclasses import dataclass

import pytest

from etl_framework.quality.schema_check import (
    SchemaValidationError,
    validate_schema,
)


@dataclass
class _Type:
    _s: str

    def simpleString(self) -> str:
        return self._s


@dataclass
class _Field:
    name: str
    dataType: _Type


class _Schema:
    def __init__(self, fields):
        self.fields = fields


class _DF:
    """Minimal DataFrame stub exposing only ``.schema.fields``."""

    def __init__(self, cols: dict[str, str]):
        self.schema = _Schema([_Field(n, _Type(t)) for n, t in cols.items()])


def test_validate_schema_passes_with_extra_columns():
    df = _DF({"a": "string", "b": "bigint", "extra": "double"})
    validate_schema(df, {"a": "string", "b": "bigint"})  # no raise


def test_validate_schema_missing_column():
    df = _DF({"a": "string"})
    with pytest.raises(SchemaValidationError, match="missing column 'b'"):
        validate_schema(df, {"a": "string", "b": "bigint"})


def test_validate_schema_type_mismatch():
    df = _DF({"a": "int"})
    with pytest.raises(SchemaValidationError, match="type"):
        validate_schema(df, {"a": "string"})


def test_validate_schema_empty_is_noop():
    validate_schema(_DF({}), {})  # no raise
