"""Hourly, point-in-time support operations with atomic per-hour replacement."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from support_ops.quality.validation import ACTIVE_STATUSES
from support_ops.transformations.silver import qualified
from support_ops.transformations.ticket_state import ticket_state_frame

OPERATIONS_DDL = """
    hour TIMESTAMP, product_id STRING, support_tier STRING, customer_segment STRING,
    tickets_created BIGINT, tickets_resolved BIGINT, open_backlog BIGINT,
    p1_ticket_count BIGINT, p2_ticket_count BIGINT, median_resolution_minutes DOUBLE,
    sla_breach_rate DOUBLE, escalation_rate DOUBLE, reopen_rate DOUBLE,
    _pipeline_run_id STRING
"""


def validate_hour(hour: datetime) -> datetime:
    """Require an explicit UTC hour boundary; never silently round a backfill."""
    if hour.tzinfo is None or hour.utcoffset() is None:
        raise ValueError("hour must be timezone-aware")
    hour = hour.astimezone(UTC)
    if hour.minute or hour.second or hour.microsecond:
        raise ValueError("hour must be aligned to a UTC hour boundary")
    return hour


def operations_frame(states: Any, accounts: Any, *, hour: datetime) -> Any:
    """Aggregate end-of-hour states; rate denominators are documented in ADR 0010."""
    from pyspark.sql import functions as f

    hour = validate_hour(hour)
    end = hour + timedelta(hours=1)
    active = f.col("status").isin(*sorted(ACTIVE_STATUSES))
    created = (f.col("created_at") >= f.lit(hour)) & (f.col("created_at") < f.lit(end))
    resolved = (f.col("resolved_at") >= f.lit(hour)) & (f.col("resolved_at") < f.lit(end))
    duration = (f.unix_micros("resolved_at") - f.unix_micros("created_at")) / 60_000_000
    # Compare timestamps rather than rounded minutes (a sub-minute overrun is a breach).
    breached = f.col("resolved_at") > f.col("sla_deadline")
    frame = states.join(accounts.select("customer_id", "segment"), "customer_id", "left")
    return (
        frame.filter(active | created | resolved)
        .withColumn("support_tier", f.coalesce("support_tier", f.lit("unknown")))
        .withColumn("customer_segment", f.coalesce("segment", f.lit("unknown")))
        .groupBy("product_id", "support_tier", "customer_segment")
        .agg(
            f.count_if(created).alias("tickets_created"),
            f.count_if(resolved).alias("tickets_resolved"),
            f.count_if(active).alias("open_backlog"),
            f.count_if(active & (f.col("priority") == "P1")).alias("p1_ticket_count"),
            f.count_if(active & (f.col("priority") == "P2")).alias("p2_ticket_count"),
            f.median(f.when(resolved, duration)).alias("median_resolution_minutes"),
            f.avg(f.when(resolved, breached.cast("double"))).alias("sla_breach_rate"),
            f.avg(f.when(created, (f.col("escalation_count") > 0).cast("double"))).alias(
                "escalation_rate"
            ),
            f.avg(f.when(created, (f.col("reopen_count") > 0).cast("double"))).alias("reopen_rate"),
        )
        .withColumn("hour", f.lit(hour))
    )


def write_hour(spark: Any, frame: Any, *, catalog: str, hour: datetime, run_id: str) -> None:
    """Replace exactly one hour, including stale groups and empty recomputations."""
    from pyspark.sql import functions as f

    hour = validate_hour(hour)
    if not run_id.strip():
        raise ValueError("pipeline run ID must not be empty")
    target = qualified(catalog, "gold", "support_operations")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({OPERATIONS_DDL}) USING DELTA")
    predicate = f"hour = TIMESTAMP '{hour.strftime('%Y-%m-%d %H:%M:%S')}'"
    (
        frame.withColumn("_pipeline_run_id", f.lit(run_id))
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .saveAsTable(target)
    )


def build_support_operations(
    spark: Any, *, catalog: str, hour: datetime, pipeline_run_id: str
) -> None:
    hour = validate_hour(hour)
    if not pipeline_run_id.strip():
        raise ValueError("pipeline run ID must not be empty")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    # Half-open hour: an event at the next boundary belongs to the next hour.
    states = ticket_state_frame(
        spark, catalog=catalog, as_of=hour + timedelta(hours=1) - timedelta(microseconds=1)
    )
    frame = operations_frame(
        states, spark.table(qualified(catalog, "silver", "accounts")), hour=hour
    )
    write_hour(spark, frame, catalog=catalog, hour=hour, run_id=pipeline_run_id)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--hour", required=True, help="UTC hour start, e.g. 2026-01-01T12:00:00Z")
    args = parser.parse_args(argv)
    hour = validate_hour(datetime.fromisoformat(args.hour))
    from pyspark.sql import SparkSession

    build_support_operations(
        SparkSession.builder.getOrCreate(),
        catalog=args.catalog,
        hour=hour,
        pipeline_run_id=args.pipeline_run_id,
    )
    return 0
