"""Retry-safe distributed Bronze-to-Silver batch processing."""

from __future__ import annotations

import argparse
import json
import re
import uuid
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from typing import Any

from support_ops.platform.bootstrap import quote_identifier
from support_ops.quality.validation import KEYS, Disposition, ValidationContext, validate_record
from support_ops.schemas import INGESTION_METADATA, INVALID_RECORDS, SOURCE_SCHEMAS

BATCH_ENTITIES = ("products", "accounts", "contracts", "tickets")
EVALUATION_DDL = """
    pipeline_run_id STRING, source STRING, _record_hash STRING,
    disposition STRING, normalized_payload STRING, raw_payload STRING,
    failure_reason STRING, record_id STRING, detected_at TIMESTAMP,
    _ingested_at TIMESTAMP, _source_file STRING, _pipeline_run_id STRING
"""
METRICS_DDL = """
    pipeline_run_id STRING, source STRING, records_read BIGINT,
    accepted_count BIGINT, duplicate_count BIGINT, quarantine_count BIGINT,
    measured_at TIMESTAMP
"""
COLUMN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
RESULT_DDL = """
    disposition STRING, normalized_payload STRING, raw_payload STRING,
    failure_reason STRING, record_id STRING
"""


def qualified(catalog: str, layer: str, name: str) -> str:
    return ".".join(quote_identifier(part) for part in (catalog, layer, name))


def quote_column(name: str) -> str:
    """Quote a column name; unlike catalog identifiers, metadata columns may start with '_'."""
    if not COLUMN.fullmatch(name):
        raise ValueError(f"Invalid column name: {name!r}")
    return f"`{name}`"


def _json_scalar(value: object) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Unsupported normalized value: {type(value).__name__}")


def evaluate_row(
    entity: str,
    raw: Mapping[str, object],
    *,
    customer_exists: bool,
    product_exists: bool,
    already_processed: bool,
    now: datetime,
    future_tolerance: timedelta,
    pipeline_run_id: str,
) -> dict[str, object]:
    """Executor-side adapter: join results become tiny per-record validation contexts."""
    customer = str(raw.get("customer_id") or "").strip()
    product = str(raw.get("product_id") or "").strip()
    key = str(raw.get(KEYS[entity]) or "").strip()
    result = validate_record(
        entity,
        raw,
        ValidationContext(
            {customer} if customer_exists else set(),
            {product} if product_exists else set(),
            now,
            future_tolerance,
        ),
        processed_ids={key} if already_processed else set(),
    )
    quarantine = (
        result.quarantine_record(pipeline_run_id=pipeline_run_id, detected_at=now)
        if result.disposition is Disposition.QUARANTINED
        else {}
    )
    return {
        "disposition": result.disposition.value,
        "normalized_payload": json.dumps(dict(result.record), default=_json_scalar),
        "raw_payload": result.raw_payload,
        "failure_reason": quarantine.get("failure_reason"),
        "record_id": quarantine.get("record_id"),
    }


def create_tables(spark: Any, catalog: str, entity: str) -> None:
    schema = SOURCE_SCHEMAS[entity]
    business = [
        f"`{field.name}` {field.data_type.value}{'' if field.nullable else ' NOT NULL'}"
        for field in schema.fields
    ]
    metadata = [f"`{field.name}` {field.data_type.value}" for field in INGESTION_METADATA]
    columns = [*business, *metadata, "_validated_at TIMESTAMP", "_validation_run_id STRING"]
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {qualified(catalog, 'silver', entity)} "
        f"({', '.join(columns)}) USING DELTA"
    )
    for name, ddl in (
        ("silver_evaluations", EVALUATION_DDL),
        ("invalid_records", INVALID_RECORDS.as_ddl()),
        ("quality_metrics", METRICS_DDL),
    ):
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {qualified(catalog, 'ops', name)} ({ddl}) USING DELTA"
        )


def merge_insert(spark: Any, frame: Any, target: str, keys: Sequence[str]) -> None:
    """Insert-only merge with temporary-view cleanup, including on a failed write."""
    view = f"silver_write_{uuid.uuid4().hex}"
    condition = " AND ".join(f"t.{quote_column(key)} = s.{quote_column(key)}" for key in keys)
    frame.createOrReplaceTempView(view)
    try:
        spark.sql(
            f"MERGE INTO {target} t USING `{view}` s ON {condition} WHEN NOT MATCHED THEN INSERT *"
        )
    finally:
        spark.catalog.dropTempView(view)


def prepare_evaluations(
    spark: Any,
    *,
    catalog: str,
    entity: str,
    pipeline_run_id: str,
    now: datetime,
    future_tolerance: timedelta,
) -> Any:
    from pyspark.sql import Window
    from pyspark.sql import functions as f

    raw = spark.table(qualified(catalog, "bronze", entity))
    raw_columns = raw.columns
    normalize_id = f.udf(lambda value: value.strip() if value is not None else None, "string")
    # Bronze hashes are unique by its ingestion contract. Reference tables are unique by key.
    frame = raw.withColumn("_customer_exists", f.lit(False)).withColumn(
        "_product_exists", f.lit(False)
    )
    for field, reference, flag in (
        ("customer_id", "accounts", "_customer_exists"),
        ("product_id", "products", "_product_exists"),
    ):
        required = (
            entity in ("contracts", "tickets", "ticket_events")
            if field == "customer_id"
            else entity in ("tickets", "ticket_events")
        )
        if required:
            ids = spark.table(qualified(catalog, "silver", reference)).select(
                f.col(field).alias("_reference_id")
            )
            frame = (
                frame.join(ids, normalize_id(frame[field]) == ids["_reference_id"], "left")
                .withColumn(flag, f.col("_reference_id").isNotNull())
                .drop("_reference_id")
            )
    key = KEYS[entity]
    existing = spark.table(qualified(catalog, "silver", entity)).select(
        f.col(key).alias("_existing_id")
    )
    frame = frame.join(existing, normalize_id(frame[key]) == existing["_existing_id"], "left")

    def evaluate(raw_row: Any, customer: bool, product: bool, processed: bool) -> dict[str, object]:
        return evaluate_row(
            entity,
            raw_row.asDict(recursive=True),
            customer_exists=customer,
            product_exists=product,
            already_processed=processed,
            now=now,
            future_tolerance=future_tolerance,
            pipeline_run_id=pipeline_run_id,
        )

    validate = f.udf(evaluate, RESULT_DDL, useArrow=False)
    frame = frame.withColumn(
        "_result",
        validate(
            f.struct(*(f.col(column) for column in raw_columns)),
            f.col("_customer_exists"),
            f.col("_product_exists"),
            f.col("_existing_id").isNotNull(),
        ),
    ).select("*", "_result.*")
    # Pick the first valid candidate deterministically. Invalid variants never consume a key.
    window = Window.partitionBy(normalize_id(f.col(key)), f.col("disposition")).orderBy(
        f.col("_ingested_at"), f.col("_record_hash")
    )
    frame = frame.withColumn("_rank", f.row_number().over(window)).withColumn(
        "disposition",
        f.when(
            (f.col("disposition") == Disposition.ACCEPTED.value) & (f.col("_rank") > 1),
            f.lit(Disposition.DUPLICATE.value),
        ).otherwise(f.col("disposition")),
    )
    return frame.select(
        f.lit(pipeline_run_id).alias("pipeline_run_id"),
        f.lit(entity).alias("source"),
        "_record_hash",
        "disposition",
        "normalized_payload",
        "raw_payload",
        "failure_reason",
        "record_id",
        f.lit(now).alias("detected_at"),
        "_ingested_at",
        "_source_file",
        "_pipeline_run_id",
    )


def write_outputs(spark: Any, staged: Any, *, catalog: str, entity: str) -> None:
    from pyspark.sql import functions as f

    accepted = staged.filter(f.col("disposition") == Disposition.ACCEPTED.value)
    accepted = accepted.withColumn(
        "_record", f.from_json("normalized_payload", SOURCE_SCHEMAS[entity].as_ddl())
    ).select(
        "_record.*",
        *(field.name for field in INGESTION_METADATA),
        f.col("detected_at").alias("_validated_at"),
        f.col("pipeline_run_id").alias("_validation_run_id"),
    )
    merge_insert(spark, accepted, qualified(catalog, "silver", entity), [KEYS[entity]])
    quarantined = staged.filter(f.col("disposition") == Disposition.QUARANTINED.value).select(
        *(field.name for field in INVALID_RECORDS.fields)
    )
    merge_insert(spark, quarantined, qualified(catalog, "ops", "invalid_records"), ["record_id"])


def transform_entity(
    spark: Any,
    *,
    catalog: str,
    entity: str,
    pipeline_run_id: str,
    now: datetime | None = None,
    future_tolerance: timedelta = timedelta(minutes=5),
) -> dict[str, object]:
    """Stage a run once, then repair/replay all outputs from its durable decisions.

    Single writer required. Reuse the run ID on retry; use a new ID for a new Bronze snapshot.
    Metrics count decisions for that snapshot, not physical inserts during a repair attempt.
    """
    from pyspark.sql import functions as f

    if entity not in SOURCE_SCHEMAS:
        raise ValueError(f"Unsupported Silver entity: {entity}")
    if not pipeline_run_id.strip():
        raise ValueError("pipeline_run_id must not be empty")
    timestamp = now or datetime.now(UTC)
    ValidationContext(set(), set(), timestamp, future_tolerance)
    create_tables(spark, catalog, entity)
    evaluations = qualified(catalog, "ops", "silver_evaluations")
    metrics_table = qualified(catalog, "ops", "quality_metrics")
    predicate = (f.col("pipeline_run_id") == pipeline_run_id) & (f.col("source") == entity)
    prior_metrics = spark.table(metrics_table).filter(predicate).collect()
    if prior_metrics:
        return dict(prior_metrics[0].asDict())
    staged = spark.table(evaluations).filter(predicate)
    if not staged.limit(1).count():
        prepared = prepare_evaluations(
            spark,
            catalog=catalog,
            entity=entity,
            pipeline_run_id=pipeline_run_id,
            now=timestamp,
            future_tolerance=future_tolerance,
        )
        # A sentinel freezes even an empty input snapshot across failures and retries.
        marker = {
            "pipeline_run_id": pipeline_run_id,
            "source": entity,
            "_record_hash": "__snapshot__",
            "disposition": "snapshot",
            "detected_at": timestamp,
        }
        marker = {
            name.strip().split()[0]: marker.get(name.strip().split()[0])
            for name in EVALUATION_DDL.split(",")
        }
        prepared = prepared.unionByName(spark.createDataFrame([marker], EVALUATION_DDL))
        # One atomic Delta commit freezes the complete entity snapshot before any output writes.
        merge_insert(spark, prepared, evaluations, ["pipeline_run_id", "source", "_record_hash"])
    staged = spark.table(evaluations).filter(predicate)
    staged = staged.filter(f.col("disposition") != "snapshot")
    write_outputs(spark, staged, catalog=catalog, entity=entity)
    counts = {
        row["disposition"]: row["count"] for row in staged.groupBy("disposition").count().collect()
    }
    metric: dict[str, object] = {
        "pipeline_run_id": pipeline_run_id,
        "source": entity,
        "records_read": sum(counts.values()),
        "accepted_count": counts.get(Disposition.ACCEPTED.value, 0),
        "duplicate_count": counts.get(Disposition.DUPLICATE.value, 0),
        "quarantine_count": counts.get(Disposition.QUARANTINED.value, 0),
        "measured_at": timestamp,
    }
    # Written last: presence of this row is the entity completion marker.
    merge_insert(
        spark,
        spark.createDataFrame([metric], METRICS_DDL),
        metrics_table,
        ["pipeline_run_id", "source"],
    )
    return metric


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--future-tolerance-minutes", type=float, default=5)
    args = parser.parse_args(argv)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    now = datetime.now(UTC)
    for entity in BATCH_ENTITIES:
        result = transform_entity(
            spark,
            catalog=args.catalog,
            entity=entity,
            pipeline_run_id=args.pipeline_run_id,
            now=now,
            future_tolerance=timedelta(minutes=args.future_tolerance_minutes),
        )
        print(json.dumps(result, default=_json_scalar, sort_keys=True))
    return 0
