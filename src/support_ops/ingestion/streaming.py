"""File-based Structured Streaming ingestion of ticket events into Bronze Delta."""

from __future__ import annotations

import argparse
import json
import re
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

from support_ops.ingestion.batch import INGESTION_COLUMNS, qualified_bronze_table
from support_ops.schemas import TICKET_EVENTS

CORRUPT_COLUMN = "_corrupt_record"
LATE_COLUMN = "_is_late"
STREAM_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
BUSINESS_COLUMNS = tuple(field.name for field in TICKET_EVENTS.fields)
METRICS_DDL = """
    stream_name STRING, batch_id BIGINT, pipeline_run_id STRING,
    rows_read BIGINT, rows_written BIGINT, duplicate_rows BIGINT,
    malformed_rows BIGINT, late_rows BIGINT,
    batch_max_event_time TIMESTAMP, max_event_time TIMESTAMP, watermark TIMESTAMP,
    processed_at TIMESTAMP, input_rows_per_second DOUBLE,
    processed_rows_per_second DOUBLE, batch_duration_ms BIGINT
"""


TIMESTAMP_METRICS = ("batch_max_event_time", "max_event_time", "watermark", "processed_at")


def _columns(ddl: str) -> list[str]:
    return [column.split()[0] for column in ddl.split(",")]


def _micros_ddl(ddl: str) -> str:
    return ddl.replace("TIMESTAMP", "BIGINT")


def _progress(update: Any) -> dict[str, Any]:
    """Spark returns progress as dicts or, in newer clients, objects with a ``json`` view."""
    return dict(update) if isinstance(update, dict) else dict(json.loads(update.json))


def bronze_events_ddl(catalog: str) -> str:
    """Raw-string Bronze events plus stream metadata; malformed lines keep their raw text."""
    columns = [f"`{name}` STRING" for name in BUSINESS_COLUMNS]
    columns += [f"`{CORRUPT_COLUMN}` STRING", f"`{LATE_COLUMN}` BOOLEAN"]
    columns += [f"`{name}` {kind} NOT NULL" for name, kind, _ in INGESTION_COLUMNS]
    return (
        f"CREATE TABLE IF NOT EXISTS {qualified_bronze_table(catalog, 'ticket_events')} "
        f"({', '.join(columns)}) USING DELTA"
    )


def create_stream_tables(spark: Any, catalog: str) -> None:
    from support_ops.transformations.silver import qualified

    spark.sql(bronze_events_ddl(catalog))
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {qualified(catalog, 'ops', 'streaming_metrics')} "
        f"({METRICS_DDL}) USING DELTA"
    )


def batch_run_id(stream_name: str, batch_id: int) -> str:
    """Bronze lineage for a microbatch; stable across replays of the same checkpointed batch."""
    return f"{stream_name}/{batch_id}"


def process_batch(
    batch: Any,
    batch_id: int,
    *,
    catalog: str,
    stream_name: str,
    pipeline_run_id: str,
    late_tolerance: timedelta,
) -> None:
    """Idempotently land one microbatch of raw lines, then record its metrics row last.

    A replay after partial failure reads the same files for the same ``batch_id``. The insert-only
    merge on ``_record_hash`` makes the Bronze write a no-op for rows already landed, counts derive
    from rows tagged with this batch, and the metrics row doubles as the completion marker.
    Uses the batch's own session, as required for ``foreachBatch`` under Spark Connect.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as f

    from support_ops.transformations.silver import merge_insert, qualified

    spark = batch.sparkSession
    target = qualified_bronze_table(catalog, "ticket_events")
    metrics_table = qualified(catalog, "ops", "streaming_metrics")
    metrics = spark.table(metrics_table).filter(f.col("stream_name") == stream_name)
    if metrics.filter(f.col("batch_id") == batch_id).limit(1).count():
        return
    # Timestamps stay in Spark or travel as UTC epoch microseconds, never as local datetimes.
    prior = (
        metrics.filter(f.col("batch_id") < batch_id)
        .agg(f.unix_micros(f.max("max_event_time")))
        .first()[0]
    )
    tolerance = late_tolerance // timedelta(microseconds=1)
    watermark = None if prior is None else prior - tolerance
    run_id = batch_run_id(stream_name, batch_id)

    schema = ", ".join([*(f"`{name}` STRING" for name in BUSINESS_COLUMNS), CORRUPT_COLUMN])
    parsed = batch.filter(f.trim("value") != "").withColumn(
        "_parsed",
        f.from_json(
            "value",
            f"{schema} STRING",
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_COLUMN},
        ),
    )
    malformed = f.col("_parsed").isNull() | f.col(f"_parsed.{CORRUPT_COLUMN}").isNotNull()
    rows = parsed.select(
        *(
            f.when(malformed, f.lit(None).cast("string"))
            .otherwise(f.col(f"_parsed.{name}"))
            .alias(name)
            for name in BUSINESS_COLUMNS
        ),
        f.when(malformed, f.col("value")).alias(CORRUPT_COLUMN),
        f.col("_source_file"),
    )
    event_time = f.expr("try_cast(event_time AS TIMESTAMP)")
    late = f.lit(False) if watermark is None else event_time < f.timestamp_micros(f.lit(watermark))
    rows = rows.select(
        *BUSINESS_COLUMNS,
        CORRUPT_COLUMN,
        f.when(event_time.isNull(), f.lit(None).cast("boolean")).otherwise(late).alias(LATE_COLUMN),
        f.current_timestamp().alias("_ingested_at"),
        "_source_file",
        f.lit(run_id).alias("_pipeline_run_id"),
        f.sha2(
            f.when(
                f.col(CORRUPT_COLUMN).isNotNull(), f.concat(f.lit("corrupt:"), CORRUPT_COLUMN)
            ).otherwise(f.to_json(f.struct(*BUSINESS_COLUMNS), {"ignoreNullFields": "false"})),
            256,
        ).alias("_record_hash"),
    )
    # Keep one deterministic row per hash within the batch; earlier batches win in the merge.
    first = Window.partitionBy("_record_hash").orderBy("_source_file")
    unique = rows.withColumn("_rank", f.row_number().over(first)).filter("_rank = 1").drop("_rank")
    merge_insert(spark, unique, target, ["_record_hash"])

    landed = spark.table(target).filter(f.col("_pipeline_run_id") == run_id)
    ingested_event_time = f.expr("try_cast(event_time AS TIMESTAMP)")
    summary = landed.agg(
        f.count(f.lit(1)).alias("rows_written"),
        f.count_if(f.col(CORRUPT_COLUMN).isNotNull()).alias("malformed_rows"),
        f.count_if(f.col(LATE_COLUMN)).alias("late_rows"),
        # Future-dated events must not advance the watermark past processing time.
        f.unix_micros(
            f.max(f.when(ingested_event_time <= f.col("_ingested_at"), ingested_event_time))
        ).alias("batch_max_event_time"),
        f.unix_micros(f.max("_ingested_at")).alias("processed_at"),
    ).first()
    rows_read = rows.count()
    batch_max = summary["batch_max_event_time"]
    candidates = [value for value in (prior, batch_max) if value is not None]
    metric = {
        "stream_name": stream_name,
        "batch_id": batch_id,
        "pipeline_run_id": pipeline_run_id,
        "rows_read": rows_read,
        "rows_written": summary["rows_written"],
        "duplicate_rows": rows_read - summary["rows_written"],
        "malformed_rows": summary["malformed_rows"],
        "late_rows": summary["late_rows"],
        "batch_max_event_time": batch_max,
        "max_event_time": max(candidates) if candidates else None,
        "watermark": watermark,
        "processed_at": summary["processed_at"],
    }
    row = {name: metric.get(name) for name in _columns(METRICS_DDL)}
    frame = spark.createDataFrame([row], _micros_ddl(METRICS_DDL)).select(
        *(
            f.timestamp_micros(name).alias(name) if name in TIMESTAMP_METRICS else f.col(name)
            for name in _columns(METRICS_DDL)
        )
    )
    merge_insert(spark, frame, metrics_table, ["stream_name", "batch_id"])


def record_query_progress(
    spark: Any, progress: Sequence[Any], *, catalog: str, stream_name: str
) -> int:
    """Attach Spark's per-batch rates and durations to the ingestion metrics rows."""
    from support_ops.transformations.silver import qualified

    rows = []
    for values in map(_progress, progress):
        if not values.get("numInputRows"):
            continue
        rows.append(
            {
                "stream_name": stream_name,
                "batch_id": int(values["batchId"]),
                "input_rows_per_second": float(values.get("inputRowsPerSecond") or 0.0),
                "processed_rows_per_second": float(values.get("processedRowsPerSecond") or 0.0),
                "batch_duration_ms": int(values.get("batchDuration") or 0),
            }
        )
    if not rows:
        return 0
    ddl = (
        "stream_name STRING, batch_id BIGINT, input_rows_per_second DOUBLE, "
        "processed_rows_per_second DOUBLE, batch_duration_ms BIGINT"
    )
    view = f"stream_progress_{stream_name}"
    spark.createDataFrame(rows, ddl).createOrReplaceTempView(view)
    try:
        spark.sql(
            f"MERGE INTO {qualified(catalog, 'ops', 'streaming_metrics')} t USING `{view}` s "
            "ON t.stream_name = s.stream_name AND t.batch_id = s.batch_id "
            "WHEN MATCHED THEN UPDATE SET "
            "t.input_rows_per_second = s.input_rows_per_second, "
            "t.processed_rows_per_second = s.processed_rows_per_second, "
            "t.batch_duration_ms = s.batch_duration_ms"
        )
    finally:
        spark.catalog.dropTempView(view)
    return len(rows)


def run_stream(
    spark: Any,
    *,
    source_path: str,
    checkpoint_root: str,
    catalog: str,
    pipeline_run_id: str,
    stream_name: str = "ticket_events",
    late_tolerance: timedelta = timedelta(minutes=60),
    max_files_per_trigger: int = 1,
) -> dict[str, object]:
    """Process every file that is available now, then stop.

    ``availableNow`` is the trigger serverless compute supports; scheduling the job repeatedly
    gives continuous ingestion with the same checkpoint. The checkpoint lives under
    ``checkpoint_root/stream_name``: resetting it requires a new stream name so batch IDs and
    watermarks never mix with the previous stream's metrics.
    """
    if not STREAM_NAME.fullmatch(stream_name):
        raise ValueError(f"Invalid stream name: {stream_name!r}")
    if not pipeline_run_id.strip():
        raise ValueError("pipeline_run_id must not be empty")
    if max_files_per_trigger < 1:
        raise ValueError("max_files_per_trigger must be at least 1")
    if late_tolerance < timedelta(0):
        raise ValueError("late_tolerance must be non-negative")
    from pyspark.sql import functions as f

    create_stream_tables(spark, catalog)

    def land(batch: Any, batch_id: int) -> None:
        process_batch(
            batch,
            batch_id,
            catalog=catalog,
            stream_name=stream_name,
            pipeline_run_id=pipeline_run_id,
            late_tolerance=late_tolerance,
        )

    lines = (
        spark.readStream.format("text")
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .option("pathGlobFilter", "events_*.json")
        .load(source_path)
        .select("value", f.col("_metadata.file_path").alias("_source_file"))
    )
    query = (
        lines.writeStream.foreachBatch(land)
        .option("checkpointLocation", f"{checkpoint_root.rstrip('/')}/{stream_name}")
        .queryName(stream_name)
        .trigger(availableNow=True)
        .start()
    )
    try:
        query.awaitTermination()
    except Exception as error:
        raise RuntimeError(f"Stream {stream_name} failed") from error
    batches = record_query_progress(
        spark, query.recentProgress, catalog=catalog, stream_name=stream_name
    )
    return {"stream_name": stream_name, "pipeline_run_id": pipeline_run_id, "batches": batches}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True)
    parser.add_argument("--checkpoint-root", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--stream-name", default="ticket_events")
    parser.add_argument("--late-tolerance-minutes", type=float, default=60)
    parser.add_argument("--max-files-per-trigger", type=int, default=1)
    args = parser.parse_args(argv)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    result = run_stream(
        spark,
        source_path=args.source,
        checkpoint_root=args.checkpoint_root,
        catalog=args.catalog,
        pipeline_run_id=args.pipeline_run_id,
        stream_name=args.stream_name,
        late_tolerance=timedelta(minutes=args.late_tolerance_minutes),
        max_files_per_trigger=args.max_files_per_trigger,
    )
    print(json.dumps(result, sort_keys=True))
    return 0
