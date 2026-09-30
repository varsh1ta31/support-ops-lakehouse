"""Idempotent CSV-to-Bronze Delta batch ingestion."""

from __future__ import annotations

import argparse
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, cast

from support_ops.schemas import ACCOUNTS, CONTRACTS, PRODUCTS, TICKETS, RecordSchema

INGESTION_COLUMNS = (
    ("_ingested_at", "TIMESTAMP", False),
    ("_source_file", "STRING", False),
    ("_pipeline_run_id", "STRING", False),
    ("_record_hash", "STRING", False),
)
IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class BatchSource:
    entity: str
    schema: RecordSchema


@dataclass(frozen=True)
class BatchIngestionResult:
    entity: str
    target_table: str
    records_read: int
    records_written: int
    duplicate_records: int
    pipeline_run_id: str


SOURCES: Mapping[str, BatchSource] = {
    source.entity: source
    for source in (
        BatchSource("tickets", TICKETS),
        BatchSource("accounts", ACCOUNTS),
        BatchSource("contracts", CONTRACTS),
        BatchSource("products", PRODUCTS),
    )
}


class SparkCatalog(Protocol):
    def dropTempView(self, view_name: str) -> bool: ...  # noqa: N802 - PySpark API


class SparkSessionLike(Protocol):
    @property
    def catalog(self) -> SparkCatalog: ...

    @property
    def read(self) -> Any: ...

    def sql(self, query: str) -> Any: ...

    def table(self, table_name: str) -> Any: ...


def _validate_identifier(value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid identifier: {value!r}")
    return value


def qualified_bronze_table(catalog: str, entity: str) -> str:
    """Return a safely quoted Bronze table identifier."""

    return ".".join(f"`{_validate_identifier(part)}`" for part in (catalog, "bronze", entity))


def bronze_table_ddl(catalog: str, source: BatchSource) -> str:
    """Create a raw-string Bronze table while retaining typed ingestion metadata."""

    columns = [f"`{field.name}` STRING" for field in source.schema.fields]
    columns.extend(f"`{name}` {data_type} NOT NULL" for name, data_type, _ in INGESTION_COLUMNS)
    return (
        f"CREATE TABLE IF NOT EXISTS {qualified_bronze_table(catalog, source.entity)} "
        f"({', '.join(columns)}) USING DELTA"
    )


def record_hash_columns(source: BatchSource) -> tuple[str, ...]:
    """Return business columns in stable schema order for deterministic hashing."""

    return tuple(field.name for field in source.schema.fields)


def _raw_string_schema(source: BatchSource) -> Any:
    try:
        from pyspark.sql.types import StringType, StructField, StructType
    except ImportError as error:
        raise RuntimeError("PySpark is required for batch ingestion") from error
    return StructType(
        [StructField(field.name, StringType(), True) for field in source.schema.fields]
    )


def _prepare_batch(
    spark: SparkSessionLike, source: BatchSource, source_path: str, run_id: str
) -> Any:
    try:
        from pyspark.sql import functions as functions
    except ImportError as error:
        raise RuntimeError("PySpark is required for batch ingestion") from error

    raw = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(_raw_string_schema(source))
        .csv(source_path)
    )
    ordered_payload = functions.to_json(
        functions.struct(*(functions.col(name) for name in record_hash_columns(source))),
        options={"ignoreNullFields": "false"},
    )
    return (
        raw.withColumn("_ingested_at", functions.current_timestamp())
        .withColumn("_source_file", functions.col("_metadata.file_path"))
        .withColumn("_pipeline_run_id", functions.lit(run_id))
        .withColumn("_record_hash", functions.sha2(ordered_payload, 256))
    )


def ingest_batch(
    spark: SparkSessionLike,
    *,
    entity: str,
    source_path: str,
    catalog: str,
    pipeline_run_id: str,
) -> BatchIngestionResult:
    """Merge unseen raw records into a Bronze Delta table by stable record hash."""

    try:
        source = SOURCES[entity]
    except KeyError as error:
        raise ValueError(f"Unsupported batch entity: {entity}") from error
    if not pipeline_run_id.strip():
        raise ValueError("pipeline_run_id must not be empty")

    target = qualified_bronze_table(catalog, entity)
    spark.sql(bronze_table_ddl(catalog, source))
    prepared = _prepare_batch(spark, source, source_path, pipeline_run_id).cache()
    deduplicated = prepared.dropDuplicates(["_record_hash"]).cache()
    view_name = f"incoming_{entity}_{uuid.uuid4().hex}"
    view_created = False
    try:
        records_read = prepared.count()
        deduplicated.count()
        before = spark.table(target).count()
        deduplicated.createOrReplaceTempView(view_name)
        view_created = True
        spark.sql(
            f"MERGE INTO {target} AS target "
            f"USING `{view_name}` AS source "
            "ON target._record_hash = source._record_hash "
            "WHEN NOT MATCHED THEN INSERT *"
        )
        after = spark.table(target).count()
    finally:
        if view_created:
            spark.catalog.dropTempView(view_name)
        deduplicated.unpersist()
        prepared.unpersist()

    return BatchIngestionResult(
        entity=entity,
        target_table=target.replace("`", ""),
        records_read=records_read,
        records_written=after - before,
        duplicate_records=records_read - (after - before),
        pipeline_run_id=pipeline_run_id,
    )


def _active_spark_session() -> SparkSessionLike:
    try:
        from pyspark.sql import SparkSession
    except ImportError as error:
        raise RuntimeError("PySpark is required for batch ingestion") from error
    return cast(SparkSessionLike, SparkSession.builder.getOrCreate())


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--entity", required=True, choices=tuple(SOURCES))
    parser.add_argument("--source", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    arguments = parser.parse_args(argv)
    result = ingest_batch(
        _active_spark_session(),
        entity=arguments.entity,
        source_path=arguments.source,
        catalog=arguments.catalog,
        pipeline_run_id=arguments.pipeline_run_id,
    )
    print(result)
    return 0
