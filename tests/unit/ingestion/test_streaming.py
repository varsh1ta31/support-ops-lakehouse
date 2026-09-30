from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml  # type: ignore[import-untyped]

from support_ops.ingestion import streaming


def test_bronze_ddl_keeps_raw_strings_and_stream_metadata() -> None:
    ddl = streaming.bronze_events_ddl("support_dev")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS `support_dev`.`bronze`.`ticket_events`")
    assert "`event_time` STRING" in ddl and "`payload` STRING" in ddl
    assert "`_corrupt_record` STRING" in ddl and "`_is_late` BOOLEAN" in ddl
    assert "`_record_hash` STRING NOT NULL" in ddl
    with pytest.raises(ValueError, match="Invalid identifier"):
        streaming.bronze_events_ddl("bad-catalog")


def test_batch_lineage_is_stable_per_stream_batch() -> None:
    assert streaming.batch_run_id("ticket_events", 12) == "ticket_events/12"


def test_progress_accepts_dicts_and_progress_objects() -> None:
    class Progress:
        json = json.dumps({"batchId": 3, "numInputRows": 5})

    assert streaming._progress({"batchId": 1})["batchId"] == 1
    assert streaming._progress(Progress())["numInputRows"] == 5


def test_metric_timestamps_travel_as_micros() -> None:
    columns = streaming._columns(streaming.METRICS_DDL)
    assert set(streaming.TIMESTAMP_METRICS) <= set(columns)
    assert "TIMESTAMP" not in streaming._micros_ddl(streaming.METRICS_DDL)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"stream_name": "Bad-Name"}, "stream name"),
        ({"pipeline_run_id": " "}, "must not be empty"),
        ({"max_files_per_trigger": 0}, "max_files_per_trigger"),
    ],
)
def test_arguments_are_validated_before_spark_is_used(
    changes: dict[str, Any], message: str
) -> None:
    arguments: dict[str, Any] = {
        "source_path": "unused",
        "checkpoint_root": "unused",
        "catalog": "support_dev",
        "pipeline_run_id": "run",
        **changes,
    }
    with pytest.raises(ValueError, match=message):
        streaming.run_stream(None, **arguments)


def test_cli_forwards_arguments(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("pyspark")
    captured: dict[str, Any] = {}

    class Builder:
        def getOrCreate(self) -> Any:  # noqa: N802 - PySpark API
            class Conf:
                def set(self, key: str, value: str) -> None:
                    captured[key] = value

            class Session:
                conf = Conf()

            return Session()

    from pyspark.sql import SparkSession

    monkeypatch.setattr(SparkSession, "builder", Builder())

    def run(spark: Any, **kwargs: Any) -> dict[str, object]:
        captured.update(kwargs)
        return {"batches": 0}

    monkeypatch.setattr(streaming, "run_stream", run)
    argv = ["--source", "/src", "--checkpoint-root", "/cp", "--catalog", "support_dev"]
    assert streaming.main([*argv, "--pipeline-run-id", "7", "--late-tolerance-minutes", "5"]) == 0
    assert captured["spark.sql.session.timeZone"] == "UTC"
    assert captured["late_tolerance"].total_seconds() == 300
    assert (captured["stream_name"], captured["max_files_per_trigger"]) == ("ticket_events", 1)


def test_stream_jobs_are_serialized_and_separate_from_silver() -> None:
    resource = yaml.safe_load(Path("resources/streaming.yml").read_text())
    jobs = resource["resources"]["jobs"]
    ingest = jobs["event_stream_ingestion"]
    assert ingest["max_concurrent_runs"] == 1
    task = ingest["tasks"][0]
    assert task["max_retries"] == 2
    assert task["python_wheel_task"]["entry_point"] == "support-ops-stream-ingest"
    assert "{{job.run_id}}" in task["python_wheel_task"]["parameters"]
    producer = jobs["event_producer"]["tasks"][0]["python_wheel_task"]
    assert producer["entry_point"] == "support-ops-generate-events"
