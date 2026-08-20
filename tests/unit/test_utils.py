"""Unit tests for retry and IO helpers."""

import pytest

from etl_framework.utils.io_utils import substitute_params
from etl_framework.utils.retry import RetryError, with_retry


def test_substitute_params():
    assert substitute_params("x=${a}", {"a": 5}) == "x=5"


def test_with_retry_succeeds_eventually():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("boom")
        return "ok"

    assert with_retry(flaky, max_attempts=3, backoff_seconds=0) == "ok"
    assert calls["n"] == 3


def test_with_retry_exhausts():
    with pytest.raises(RetryError):
        with_retry(lambda: (_ for _ in ()).throw(ValueError()), max_attempts=2, backoff_seconds=0)
