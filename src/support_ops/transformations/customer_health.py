"""Point-in-time customer support health snapshots at UTC hour boundaries."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

from support_ops.quality.validation import ACTIVE_STATUSES
from support_ops.transformations.gold import validate_hour
from support_ops.transformations.silver import qualified
from support_ops.transformations.ticket_state import ticket_state_frame

HEALTH_DDL = """
    as_of TIMESTAMP, customer_id STRING, support_tier STRING,
    annual_contract_value DOUBLE, open_ticket_count BIGINT, p1_ticket_count BIGINT,
    ticket_count_30d BIGINT, breach_rate_90d DOUBLE,
    escalation_rate_90d DOUBLE, reopen_rate_90d DOUBLE,
    average_resolution_minutes DOUBLE, _pipeline_run_id STRING
"""


def customer_health_frame(states: Any, accounts: Any, *, as_of: datetime) -> Any:
    """Return one row per account, with ticket windows ending at ``as_of``."""
    from pyspark.sql import functions as f

    as_of = validate_hour(as_of)
    start_30d = as_of - timedelta(days=30)
    start_90d = as_of - timedelta(days=90)
    active = f.col("status").isin(*sorted(ACTIVE_STATUSES))
    recent_30d = f.col("created_at") >= f.lit(start_30d)
    recent_90d = f.col("created_at") >= f.lit(start_90d)
    resolved_90d = (f.col("resolved_at") >= f.lit(start_90d)) & (
        f.col("resolved_at") < f.lit(as_of)
    )
    duration = (f.unix_micros("resolved_at") - f.unix_micros("created_at")) / 60_000_000
    breached = f.col("resolved_at") > f.col("sla_deadline")
    totals = states.groupBy("customer_id").agg(
        f.count_if(active).alias("open_ticket_count"),
        f.count_if(active & (f.col("priority") == "P1")).alias("p1_ticket_count"),
        f.count_if(recent_30d).alias("ticket_count_30d"),
        f.avg(f.when(resolved_90d, breached.cast("double"))).alias("breach_rate_90d"),
        f.avg(f.when(recent_90d, (f.col("escalation_count") > 0).cast("double"))).alias(
            "escalation_rate_90d"
        ),
        f.avg(f.when(recent_90d, (f.col("reopen_count") > 0).cast("double"))).alias(
            "reopen_rate_90d"
        ),
        f.avg(f.when(resolved_90d, duration)).alias("average_resolution_minutes"),
    )
    return (
        accounts.select("customer_id", "support_tier", "annual_contract_value")
        .join(totals, "customer_id", "left")
        .select(
            "customer_id",
            "support_tier",
            "annual_contract_value",
            f.coalesce("open_ticket_count", f.lit(0)).alias("open_ticket_count"),
            f.coalesce("p1_ticket_count", f.lit(0)).alias("p1_ticket_count"),
            f.coalesce("ticket_count_30d", f.lit(0)).alias("ticket_count_30d"),
            "breach_rate_90d",
            "escalation_rate_90d",
            "reopen_rate_90d",
            "average_resolution_minutes",
            f.lit(as_of).alias("as_of"),
        )
    )


def write_snapshot(spark: Any, frame: Any, *, catalog: str, as_of: datetime, run_id: str) -> None:
    """Replace one complete snapshot, including deleted accounts and stale values."""
    from pyspark.sql import functions as f

    as_of = validate_hour(as_of)
    if not run_id.strip():
        raise ValueError("pipeline run ID must not be empty")
    target = qualified(catalog, "gold", "customer_support_health")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({HEALTH_DDL}) USING DELTA")
    predicate = f"as_of = TIMESTAMP '{as_of.strftime('%Y-%m-%d %H:%M:%S')}'"
    (
        frame.withColumn("_pipeline_run_id", f.lit(run_id))
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .saveAsTable(target)
    )


def build_customer_health(
    spark: Any, *, catalog: str, as_of: datetime, pipeline_run_id: str
) -> None:
    as_of = validate_hour(as_of)
    if not pipeline_run_id.strip():
        raise ValueError("pipeline run ID must not be empty")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    states = ticket_state_frame(spark, catalog=catalog, as_of=as_of - timedelta(microseconds=1))
    accounts = spark.table(qualified(catalog, "silver", "accounts"))
    frame = customer_health_frame(states, accounts, as_of=as_of)
    write_snapshot(spark, frame, catalog=catalog, as_of=as_of, run_id=pipeline_run_id)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument(
        "--as-of", required=True, help="UTC hour boundary, e.g. 2026-01-01T12:00:00Z"
    )
    args = parser.parse_args(argv)
    as_of = validate_hour(datetime.fromisoformat(args.as_of))
    from pyspark.sql import SparkSession

    build_customer_health(
        SparkSession.builder.getOrCreate(),
        catalog=args.catalog,
        as_of=as_of,
        pipeline_run_id=args.pipeline_run_id,
    )
    return 0
