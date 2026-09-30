"""Structured Streaming recovery, deduplication, lateness, and malformed-line capture."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from support_ops.ingestion import streaming
from support_ops.transformations import silver

pytestmark = pytest.mark.spark
T0 = datetime(2026, 1, 1, 10, tzinfo=UTC)


def line(event_id: str, minute: int | None, **changes: object) -> str:
    record: dict[str, object] = {
        "event_id": event_id,
        "ticket_id": "STK-1",
        "customer_id": "CUS-000001",
        "product_id": "PRD-001",
        "event_type": "agent_replied",
        "event_time": None
        if minute is None
        else (T0 + timedelta(minutes=minute)).isoformat().replace("+00:00", "Z"),
        **changes,
    }
    return json.dumps(record)


def write(directory: Path, batch: int, lines: list[str]) -> None:
    directory.mkdir(exist_ok=True)
    path = directory / f"events_{batch:06d}.json"
    path.write_text("\n".join(lines) + "\n")
    # The file source takes the oldest files first; same-instant writes would tie.
    os.utime(path, (1_700_000_000 + batch, 1_700_000_000 + batch))


def metrics(spark: Any) -> dict[int, Any]:
    rows = spark.table("ops.streaming_metrics").filter("stream_name = 'events_test'").collect()
    return {row.batch_id: row for row in rows}


def test_stream_recovers_from_checkpoint_without_duplicates(
    spark: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    for table in ("bronze.ticket_events", "ops.streaming_metrics"):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    source, checkpoints = tmp_path / "events", tmp_path / "checkpoints"

    def run() -> dict[str, object]:
        return streaming.run_stream(
            spark,
            source_path=str(source),
            checkpoint_root=str(checkpoints),
            catalog="spark_catalog",
            pipeline_run_id="job-1",
            stream_name="events_test",
            late_tolerance=timedelta(minutes=30),
        )

    # Batch A: one file per microbatch.
    write(
        source,
        1,
        [
            line("e1", 0),
            line("e2", 5),
            line("e2", 5),
            "not json",
            "",
            "{}}",
            line("x", 1) + " junk",
        ],
    )
    write(
        source,
        2,
        [line("e3", 60), line("e1", 0), line("e1", 0, payload='{"changed": true}'), "not json"],
    )
    (source / ".events_000009.json.tmp").write_text("partial")
    assert run()["batches"] == 2
    first = metrics(spark)
    assert sorted(first) == [0, 1]
    assert (first[0].rows_read, first[0].rows_written, first[0].duplicate_rows) == (6, 5, 1)
    assert (first[0].malformed_rows, first[0].late_rows, first[0].watermark) == (3, 0, None)
    assert first[0].max_event_time.astimezone(UTC) == T0 + timedelta(minutes=5)
    # The changed payload is new raw evidence; Silver decides event_id duplicates.
    # Repeated garbage in a new file is a new malformed arrival, not a duplicate event.
    assert (first[1].rows_read, first[1].rows_written, first[1].duplicate_rows) == (4, 3, 1)
    assert first[1].malformed_rows == 1
    assert first[1].watermark.astimezone(UTC) == T0 - timedelta(minutes=25)
    assert first[1].batch_duration_ms > 0 and first[1].input_rows_per_second is not None
    bronze = spark.table("bronze.ticket_events")
    assert bronze.count() == 8
    corrupt = (
        bronze.filter("_corrupt_record IS NOT NULL")
        .orderBy("_corrupt_record", "_source_file")
        .collect()
    )
    assert [row._corrupt_record for row in corrupt] == [
        "not json",
        "not json",
        line("x", 1) + " junk",
        "{}}",
    ]
    assert {row.event_id for row in corrupt} == {None}
    assert corrupt[0]._source_file.endswith("events_000001.json")
    assert bronze.filter("_pipeline_run_id = 'events_test/1'").count() == 3

    # Stop, add batch B, and fail once after its Bronze write but before its metrics row.
    write(
        source,
        3,
        [
            line("e4", 10),
            line("e5", 90),
            line("e6", 60 * 24 * 365 * 70),
            line("e7", None, event_time="garbage"),
        ],
    )
    original = silver.merge_insert
    failed = False

    def fail_metrics(spark: Any, frame: Any, target: str, keys: Any) -> None:
        nonlocal failed
        if "streaming_metrics" in target and not failed:
            failed = True
            raise RuntimeError("simulated crash before metrics")
        original(spark, frame, target, keys)

    with monkeypatch.context() as patch:
        patch.setattr(silver, "merge_insert", fail_metrics)
        with pytest.raises(RuntimeError, match="events_test failed"):
            run()
    assert spark.table("bronze.ticket_events").count() == 12
    assert 2 not in metrics(spark)

    assert run()["batches"] == 1
    assert spark.table("bronze.ticket_events").count() == 12
    replayed = metrics(spark)[2]
    assert (replayed.rows_read, replayed.rows_written, replayed.duplicate_rows) == (4, 4, 0)
    assert replayed.watermark.astimezone(UTC) == T0 + timedelta(minutes=30)
    # e4 is behind the watermark; e7 has no parseable time; the future e6 cannot advance it.
    assert replayed.late_rows == 1
    assert replayed.max_event_time.astimezone(UTC) == T0 + timedelta(minutes=90)
    late = {
        row.event_id: row._is_late
        for row in spark.table("bronze.ticket_events")
        .filter("_pipeline_run_id = 'events_test/2'")
        .collect()
    }
    assert late == {"e4": True, "e5": False, "e6": False, "e7": None}
    assert run()["batches"] == 0
    assert sorted(metrics(spark)) == [0, 1, 2]
    # A replay after the metrics commit but before the checkpoint commit is a no-op.
    streaming.process_batch(
        spark.createDataFrame([("not json", "late-file")], "value STRING, _source_file STRING"),
        2,
        catalog="spark_catalog",
        stream_name="events_test",
        pipeline_run_id="job-2",
        late_tolerance=timedelta(minutes=30),
    )
    assert spark.table("bronze.ticket_events").count() == 12
    assert metrics(spark)[2] == replayed

    result = silver.transform_entity(
        spark,
        catalog="spark_catalog",
        entity="ticket_events",
        pipeline_run_id="stream-silver",
        now=datetime(2026, 6, 1, tzinfo=UTC),
    )
    assert result["records_read"] == 12
    quarantined = (
        spark.table("ops.invalid_records")
        .filter("pipeline_run_id = 'stream-silver' AND raw_payload LIKE '%not json%'")
        .collect()
    )
    assert len(quarantined) == 2
    assert quarantined[0].failure_reason.startswith("record: malformed source line")


def test_stream_arguments_are_validated(spark: Any) -> None:
    arguments: dict[str, Any] = {
        "source_path": "unused",
        "checkpoint_root": "unused",
        "catalog": "spark_catalog",
        "pipeline_run_id": "run",
    }
    with pytest.raises(ValueError, match="stream name"):
        streaming.run_stream(spark, **arguments, stream_name="Bad-Name")
    with pytest.raises(ValueError, match="must not be empty"):
        streaming.run_stream(spark, **{**arguments, "pipeline_run_id": " "})
    with pytest.raises(ValueError, match="max_files_per_trigger"):
        streaming.run_stream(spark, **arguments, max_files_per_trigger=0)
    with pytest.raises(ValueError, match="late_tolerance"):
        streaming.run_stream(spark, **arguments, late_tolerance=timedelta(minutes=-1))
