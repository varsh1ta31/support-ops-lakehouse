"""Validation of the explicit hourly backfill contract."""

import sys
from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from support_ops.transformations import gold


def test_hour_validation() -> None:
    expected = datetime(2026, 1, 1, tzinfo=UTC)
    assert gold.validate_hour(expected) == expected
    assert (
        gold.validate_hour(datetime(2025, 12, 31, 19, tzinfo=timezone(timedelta(hours=-5))))
        == expected
    )
    with pytest.raises(ValueError, match="timezone-aware"):
        gold.validate_hour(datetime(2026, 1, 1))
    with pytest.raises(ValueError, match="aligned"):
        gold.validate_hour(expected + timedelta(microseconds=1))


def test_invalid_cli_hour_fails_before_spark() -> None:
    with pytest.raises(ValueError, match="aligned"):
        gold.main(
            ["--catalog", "support_dev", "--pipeline-run-id", "r", "--hour", "2026-01-01T01:01:00Z"]
        )


def test_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.setitem(
        sys.modules,
        "pyspark.sql",
        SimpleNamespace(
            SparkSession=SimpleNamespace(builder=SimpleNamespace(getOrCreate=lambda: None))
        ),
    )
    monkeypatch.setattr(gold, "build_support_operations", lambda *a, **kw: calls.append(kw))
    assert (
        gold.main(
            ["--catalog", "support_dev", "--pipeline-run-id", "r", "--hour", "2026-01-01T01:00:00Z"]
        )
        == 0
    )
    assert calls[0]["hour"] == datetime(2026, 1, 1, 1, tzinfo=UTC)


def test_empty_run_id() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        gold.build_support_operations(
            None, catalog="support_dev", hour=datetime(2026, 1, 1, tzinfo=UTC), pipeline_run_id=" "
        )
