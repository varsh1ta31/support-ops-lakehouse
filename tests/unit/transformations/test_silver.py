from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import yaml  # type: ignore[import-untyped]

from support_ops.transformations import silver
from tests.unit.quality.test_validation import EVENT


def evaluate(raw: dict[str, object], **kwargs: Any) -> dict[str, object]:
    return silver.evaluate_row(
        "ticket_events",
        raw,
        customer_exists=kwargs.get("customer_exists", True),
        product_exists=kwargs.get("product_exists", True),
        already_processed=kwargs.get("already_processed", False),
        now=datetime(2026, 1, 1, tzinfo=UTC),
        future_tolerance=timedelta(minutes=5),
        pipeline_run_id="test",
    )


def test_executor_adapter_normalizes_and_uses_join_flags() -> None:
    result = evaluate({**EVENT, "customer_id": " c ", "product_id": " p "})
    assert result["disposition"] == "accepted"
    payload = json.loads(str(result["normalized_payload"]))
    assert payload["customer_id"] == "c"
    assert payload["event_time"] == "2025-01-01T00:00:00+00:00"
    assert result["record_id"] is None
    assert evaluate(EVENT, already_processed=True)["disposition"] == "rejected_duplicate"
    bad = evaluate(EVENT, customer_exists=False, product_exists=False, already_processed=True)
    assert bad["disposition"] == "quarantined"
    assert "unknown customer" in str(bad["failure_reason"])
    assert "unknown product" in str(bad["failure_reason"])
    assert bad["record_id"]


def test_json_rejects_unexpected_objects() -> None:
    with pytest.raises(TypeError, match="Unsupported normalized"):
        silver._json_scalar(object())


class FakeFrame:
    def createOrReplaceTempView(self, name: str) -> None:  # noqa: N802
        self.view = name


class FakeSpark:
    def __init__(self, fail: bool = False) -> None:
        self.statements: list[str] = []
        self.dropped: list[str] = []
        self.fail = fail
        self.catalog = self

    def sql(self, query: str) -> None:
        self.statements.append(query)
        if self.fail:
            raise RuntimeError("write failed")

    def dropTempView(self, name: str) -> None:  # noqa: N802
        self.dropped.append(name)


def test_merge_cleans_up_on_failure() -> None:
    frame = FakeFrame()
    spark = FakeSpark(fail=True)
    with pytest.raises(RuntimeError, match="write failed"):
        silver.merge_insert(spark, frame, "`c`.`silver`.`tickets`", ["ticket_id"])
    assert spark.dropped == [frame.view]
    assert "WHEN NOT MATCHED THEN INSERT *" in spark.statements[0]


def test_merge_keys_allow_metadata_columns_but_reject_injection() -> None:
    spark = FakeSpark()
    silver.merge_insert(spark, FakeFrame(), "`c`.`ops`.`e`", ["source", "_record_hash"])
    assert "t.`_record_hash` = s.`_record_hash`" in spark.statements[0]
    with pytest.raises(ValueError, match="Invalid column"):
        silver.merge_insert(spark, FakeFrame(), "`c`.`ops`.`e`", ["x` OR 1=1 --"])


def test_table_contracts_and_identifier_safety() -> None:
    spark = FakeSpark()
    silver.create_tables(spark, "support_dev", "tickets")
    assert len(spark.statements) == 4
    assert "`created_at` TIMESTAMP NOT NULL" in spark.statements[0]
    assert "`_source_file` STRING" in spark.statements[0]
    assert "silver_evaluations" in spark.statements[1]
    assert "quality_metrics" in spark.statements[3]
    with pytest.raises(ValueError, match="Invalid"):
        silver.create_tables(spark, "bad; DROP TABLE x", "tickets")


def test_job_serializes_writers_and_preserves_retry_run_id() -> None:
    from pathlib import Path

    resource = yaml.safe_load(Path("resources/transformations.yml").read_text())
    job = resource["resources"]["jobs"]["silver_transformations"]
    assert job["max_concurrent_runs"] == 1
    task = job["tasks"][0]
    assert task["max_retries"] == 2
    assert "{{job.run_id}}" in task["python_wheel_task"]["parameters"]
    assert task["python_wheel_task"]["entry_point"] == "support-ops-silver"
