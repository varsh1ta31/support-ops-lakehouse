"""Point-in-time feature snapshots for active tickets."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

from support_ops.quality.validation import ACTIVE_STATUSES
from support_ops.transformations.gold import validate_hour
from support_ops.transformations.silver import qualified
from support_ops.transformations.ticket_state import ticket_state_frame

FEATURES_DDL = """
    as_of TIMESTAMP, ticket_id STRING, ticket_age_minutes BIGINT, minutes_to_sla BIGINT,
    priority STRING, support_tier STRING, customer_segment STRING,
    message_count BIGINT, customer_message_count BIGINT, agent_message_count BIGINT,
    reopen_count BIGINT, priority_change_count BIGINT, previous_escalation_count BIGINT,
    customer_ticket_count_30d BIGINT, customer_breach_rate_90d DOUBLE,
    customer_escalation_rate_90d DOUBLE, product_ticket_count_24h BIGINT,
    product_escalation_rate_24h DOUBLE, product_breach_rate_7d DOUBLE,
    sla_breached BOOLEAN, _pipeline_run_id STRING
"""


def ticket_features_frame(states: Any, accounts: Any, events: Any, *, as_of: datetime) -> Any:
    """Build features from states and events visible strictly before ``as_of``.

    Peer cohorts exclude the scored ticket. Breach rates use tickets resolved in the
    window; escalation rates use tickets created in the window. Empty rates are null.
    """
    from pyspark.sql import functions as f

    as_of = validate_hour(as_of)
    active = states.filter(f.col("status").isin(*sorted(ACTIVE_STATUSES))).alias("current")
    history = states.alias("peer")
    customer = (
        active.select("ticket_id", "customer_id")
        .alias("target")
        .join(
            history,
            (f.col("target.customer_id") == f.col("peer.customer_id"))
            & (f.col("target.ticket_id") != f.col("peer.ticket_id")),
            "left",
        )
        .groupBy(f.col("target.ticket_id").alias("ticket_id"))
        .agg(
            f.count_if(f.col("peer.created_at") >= f.lit(as_of - timedelta(days=30))).alias(
                "customer_ticket_count_30d"
            ),
            f.avg(
                f.when(
                    f.col("peer.resolved_at") >= f.lit(as_of - timedelta(days=90)),
                    (f.col("peer.resolved_at") > f.col("peer.sla_deadline")).cast("double"),
                )
            ).alias("customer_breach_rate_90d"),
            f.avg(
                f.when(
                    f.col("peer.created_at") >= f.lit(as_of - timedelta(days=90)),
                    (f.col("peer.escalation_count") > 0).cast("double"),
                )
            ).alias("customer_escalation_rate_90d"),
        )
    )
    product = (
        active.select("ticket_id", "product_id")
        .alias("target")
        .join(
            history,
            (f.col("target.product_id") == f.col("peer.product_id"))
            & (f.col("target.ticket_id") != f.col("peer.ticket_id")),
            "left",
        )
        .groupBy(f.col("target.ticket_id").alias("ticket_id"))
        .agg(
            f.count_if(f.col("peer.created_at") >= f.lit(as_of - timedelta(hours=24))).alias(
                "product_ticket_count_24h"
            ),
            f.avg(
                f.when(
                    f.col("peer.created_at") >= f.lit(as_of - timedelta(hours=24)),
                    (f.col("peer.escalation_count") > 0).cast("double"),
                )
            ).alias("product_escalation_rate_24h"),
            f.avg(
                f.when(
                    f.col("peer.resolved_at") >= f.lit(as_of - timedelta(days=7)),
                    (f.col("peer.resolved_at") > f.col("peer.sla_deadline")).cast("double"),
                )
            ).alias("product_breach_rate_7d"),
        )
    )
    event_counts = (
        events.filter(f.col("event_time") < f.lit(as_of))
        .groupBy("ticket_id")
        .agg(
            f.count_if(f.col("event_type") == "customer_replied").alias("customer_message_count"),
            f.count_if(f.col("event_type") == "agent_replied").alias("agent_message_count"),
            f.count_if(f.col("event_type") == "priority_changed").alias("priority_change_count"),
        )
    )
    return (
        active.join(accounts.select("customer_id", "segment"), "customer_id", "left")
        .join(customer, "ticket_id", "left")
        .join(product, "ticket_id", "left")
        .join(event_counts, "ticket_id", "left")
        .select(
            f.lit(as_of).alias("as_of"),
            "ticket_id",
            f.col("minutes_open").alias("ticket_age_minutes"),
            "minutes_to_sla",
            "priority",
            "support_tier",
            f.col("segment").alias("customer_segment"),
            f.col("message_count").cast("bigint").alias("message_count"),
            f.coalesce("customer_message_count", f.lit(0)).alias("customer_message_count"),
            f.coalesce("agent_message_count", f.lit(0)).alias("agent_message_count"),
            f.col("reopen_count").cast("bigint").alias("reopen_count"),
            f.coalesce("priority_change_count", f.lit(0)).alias("priority_change_count"),
            f.col("escalation_count").cast("bigint").alias("previous_escalation_count"),
            "customer_ticket_count_30d",
            "customer_breach_rate_90d",
            "customer_escalation_rate_90d",
            "product_ticket_count_24h",
            "product_escalation_rate_24h",
            "product_breach_rate_7d",
            f.lit(None).cast("boolean").alias("sla_breached"),
        )
    )


def write_snapshot(spark: Any, frame: Any, *, catalog: str, as_of: datetime, run_id: str) -> None:
    """Atomically replace one complete scoring snapshot, including stale tickets."""
    from pyspark.sql import functions as f

    as_of = validate_hour(as_of)
    if not run_id.strip():
        raise ValueError("pipeline run ID must not be empty")
    target = qualified(catalog, "gold", "ticket_features")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({FEATURES_DDL}) USING DELTA")
    predicate = f"as_of = TIMESTAMP '{as_of.strftime('%Y-%m-%d %H:%M:%S')}'"
    (
        frame.withColumn("_pipeline_run_id", f.lit(run_id))
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .saveAsTable(target)
    )


def build_ticket_features(
    spark: Any, *, catalog: str, as_of: datetime, pipeline_run_id: str
) -> None:
    as_of = validate_hour(as_of)
    if not pipeline_run_id.strip():
        raise ValueError("pipeline run ID must not be empty")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    states = ticket_state_frame(spark, catalog=catalog, as_of=as_of - timedelta(microseconds=1))
    accounts = spark.table(qualified(catalog, "silver", "accounts"))
    events = spark.table(qualified(catalog, "silver", "ticket_events"))
    frame = ticket_features_frame(states, accounts, events, as_of=as_of)
    write_snapshot(spark, frame, catalog=catalog, as_of=as_of, run_id=pipeline_run_id)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument(
        "--as-of", required=True, help="UTC hour boundary, e.g. 2026-01-01T12:00:00Z"
    )
    args = parser.parse_args(argv)
    from pyspark.sql import SparkSession

    build_ticket_features(
        SparkSession.builder.getOrCreate(),
        catalog=args.catalog,
        as_of=validate_hour(datetime.fromisoformat(args.as_of)),
        pipeline_run_id=args.pipeline_run_id,
    )
    return 0
