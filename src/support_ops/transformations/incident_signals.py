"""Hourly product incident signals against the previous seven matching UTC hours."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

from support_ops.transformations.gold import validate_hour
from support_ops.transformations.silver import qualified
from support_ops.transformations.ticket_state import ticket_state_frame

BASELINE_DAYS = 7
MIN_TICKETS = 5
MIN_CUSTOMERS = 3
VOLUME_MULTIPLIER = 2.0

SIGNALS_DDL = """
    time_window TIMESTAMP, product_id STRING, ticket_count BIGINT,
    unique_customers BIGINT, p1_count BIGINT, escalation_count BIGINT,
    ticket_volume_baseline DOUBLE, volume_deviation DOUBLE,
    incident_signal BOOLEAN, _pipeline_run_id STRING
"""


def incident_signals_frame(states: Any, products: Any, *, hour: datetime) -> Any:
    """Compare current created-ticket volume with seven prior matching UTC hours."""
    from pyspark.sql import functions as f

    hour = validate_hour(hour)
    end = hour + timedelta(hours=1)
    baseline_start = hour - timedelta(days=BASELINE_DAYS)
    created = f.col("created_at")
    current = (created >= f.lit(hour)) & (created < f.lit(end))
    baseline = (
        (created >= f.lit(baseline_start))
        & (created < f.lit(hour))
        & (f.hour(created) == f.lit(hour.hour))
    )
    counts = (
        states.filter(current | baseline)
        .groupBy("product_id")
        .agg(
            f.count_if(current).alias("ticket_count"),
            f.countDistinct(f.when(current, f.col("customer_id"))).alias("unique_customers"),
            f.count_if(current & (f.col("priority") == "P1")).alias("p1_count"),
            f.count_if(current & (f.col("escalation_count") > 0)).alias("escalation_count"),
            f.count_if(baseline).alias("_baseline_tickets"),
        )
    )
    result = products.select("product_id").join(counts, "product_id", "left")
    for name in ("ticket_count", "unique_customers", "p1_count", "escalation_count"):
        result = result.withColumn(name, f.coalesce(f.col(name), f.lit(0)).cast("bigint"))
    return (
        result.withColumn(
            "ticket_volume_baseline",
            (f.coalesce(f.col("_baseline_tickets"), f.lit(0)) / f.lit(BASELINE_DAYS)).cast(
                "double"
            ),
        )
        .withColumn("volume_deviation", f.col("ticket_count") - f.col("ticket_volume_baseline"))
        .withColumn(
            "incident_signal",
            (f.col("ticket_count") >= MIN_TICKETS)
            & (f.col("unique_customers") >= MIN_CUSTOMERS)
            & (f.col("ticket_count") >= f.col("ticket_volume_baseline") * VOLUME_MULTIPLIER),
        )
        .withColumn("time_window", f.lit(hour))
        .select(
            "time_window",
            "product_id",
            "ticket_count",
            "unique_customers",
            "p1_count",
            "escalation_count",
            "ticket_volume_baseline",
            "volume_deviation",
            "incident_signal",
        )
    )


def write_hour(spark: Any, frame: Any, *, catalog: str, hour: datetime, run_id: str) -> None:
    """Atomically replace one entire hour, including rows for removed products."""
    from pyspark.sql import functions as f

    hour = validate_hour(hour)
    if not run_id.strip():
        raise ValueError("pipeline run ID must not be empty")
    target = qualified(catalog, "gold", "incident_signals")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({SIGNALS_DDL}) USING DELTA")
    predicate = f"time_window = TIMESTAMP '{hour.strftime('%Y-%m-%d %H:%M:%S')}'"
    (
        frame.withColumn("_pipeline_run_id", f.lit(run_id))
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .saveAsTable(target)
    )


def build_incident_signals(
    spark: Any, *, catalog: str, hour: datetime, pipeline_run_id: str
) -> None:
    hour = validate_hour(hour)
    if not pipeline_run_id.strip():
        raise ValueError("pipeline run ID must not be empty")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    states = ticket_state_frame(
        spark, catalog=catalog, as_of=hour + timedelta(hours=1) - timedelta(microseconds=1)
    )
    products = spark.table(qualified(catalog, "silver", "products"))
    frame = incident_signals_frame(states, products, hour=hour)
    write_hour(spark, frame, catalog=catalog, hour=hour, run_id=pipeline_run_id)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--hour", required=True, help="UTC hour start, e.g. 2026-01-01T12:00:00Z")
    args = parser.parse_args(argv)
    hour = validate_hour(datetime.fromisoformat(args.hour))
    from pyspark.sql import SparkSession

    build_incident_signals(
        SparkSession.builder.getOrCreate(),
        catalog=args.catalog,
        hour=hour,
        pipeline_run_id=args.pipeline_run_id,
    )
    return 0
