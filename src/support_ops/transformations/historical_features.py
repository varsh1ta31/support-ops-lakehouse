"""Backfill one point-in-time Gold feature row per historical ticket."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from support_ops.transformations.gold import validate_hour
from support_ops.transformations.silver import qualified
from support_ops.transformations.ticket_features import FEATURES_DDL


def historical_features_frame(
    tickets: Any, contracts: Any, accounts: Any, *, before: datetime
) -> Any:
    """Score each batch ticket at the next UTC hour, using only then-visible peers.

    This path is limited to periods with no ticket events. Historical ticket rows are
    final snapshots, so escalation is treated as visible only after resolution.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as f

    before = validate_hour(before)
    source = tickets.filter(f.col("created_at") < f.lit(before)).withColumn(
        "as_of", f.expr("date_trunc('hour', created_at) + INTERVAL 1 HOUR")
    )
    history = (
        source.alias("t")
        .join(
            contracts.alias("c"),
            (f.col("t.customer_id") == f.col("c.customer_id"))
            & (f.col("t.priority") == f.col("c.priority"))
            & (f.to_date("t.created_at") >= f.col("c.effective_from"))
            & (
                f.col("c.effective_to").isNull()
                | (f.to_date("t.created_at") <= f.col("c.effective_to"))
            ),
            "left",
        )
        .withColumn(
            "_contract_rank",
            f.row_number().over(
                Window.partitionBy("t.ticket_id").orderBy(
                    f.col("c.effective_from").desc_nulls_last(),
                    f.col("c.contract_id").desc_nulls_last(),
                )
            ),
        )
        .filter(f.col("_contract_rank") == 1)
        .select(
            f.col("t.ticket_id").alias("ticket_id"),
            f.col("t.customer_id").alias("customer_id"),
            f.col("t.product_id").alias("product_id"),
            f.col("t.created_at").alias("created_at"),
            f.col("t.closed_at").alias("closed_at"),
            f.col("t.priority").alias("priority"),
            f.col("t.escalated").alias("escalated"),
            f.col("t.as_of").alias("as_of"),
            f.col("c.support_tier").alias("support_tier"),
            f.col("c.resolution_sla_minutes").alias("resolution_sla_minutes"),
        )
        .withColumn(
            "sla_deadline",
            f.timestamp_micros(
                f.unix_micros("created_at")
                + f.col("resolution_sla_minutes").cast("bigint") * f.lit(60_000_000).cast("bigint")
            ),
        )
    )
    target = history.filter(
        (f.col("as_of") < f.lit(before))
        & (f.col("closed_at").isNull() | (f.col("closed_at") >= f.col("as_of")))
    )
    customer = (
        target.select("ticket_id", "customer_id", "as_of")
        .alias("target")
        .join(
            history.alias("peer"),
            (f.col("target.customer_id") == f.col("peer.customer_id"))
            & (f.col("target.ticket_id") != f.col("peer.ticket_id"))
            & (f.col("peer.created_at") < f.col("target.as_of"))
            & (
                (f.col("peer.created_at") >= f.col("target.as_of") - f.expr("INTERVAL 90 DAYS"))
                | (
                    (f.col("peer.closed_at") >= f.col("target.as_of") - f.expr("INTERVAL 90 DAYS"))
                    & (f.col("peer.closed_at") < f.col("target.as_of"))
                )
            ),
            "left",
        )
        .groupBy(f.col("target.ticket_id").alias("ticket_id"))
        .agg(
            f.count_if(
                f.col("peer.created_at") >= f.col("target.as_of") - f.expr("INTERVAL 30 DAYS")
            ).alias("customer_ticket_count_30d"),
            f.avg(
                f.when(
                    (f.col("peer.closed_at") >= f.col("target.as_of") - f.expr("INTERVAL 90 DAYS"))
                    & (f.col("peer.closed_at") < f.col("target.as_of")),
                    (f.col("peer.closed_at") > f.col("peer.sla_deadline")).cast("double"),
                )
            ).alias("customer_breach_rate_90d"),
            f.avg(
                f.when(
                    f.col("peer.created_at") >= f.col("target.as_of") - f.expr("INTERVAL 90 DAYS"),
                    f.coalesce(
                        f.col("peer.escalated") & (f.col("peer.closed_at") < f.col("target.as_of")),
                        f.lit(False),
                    ).cast("double"),
                )
            ).alias("customer_escalation_rate_90d"),
        )
    )
    product = (
        target.select("ticket_id", "product_id", "as_of")
        .alias("target")
        .join(
            history.alias("peer"),
            (f.col("target.product_id") == f.col("peer.product_id"))
            & (f.col("target.ticket_id") != f.col("peer.ticket_id"))
            & (f.col("peer.created_at") < f.col("target.as_of"))
            & (
                (f.col("peer.created_at") >= f.col("target.as_of") - f.expr("INTERVAL 7 DAYS"))
                | (
                    (f.col("peer.closed_at") >= f.col("target.as_of") - f.expr("INTERVAL 7 DAYS"))
                    & (f.col("peer.closed_at") < f.col("target.as_of"))
                )
            ),
            "left",
        )
        .groupBy(f.col("target.ticket_id").alias("ticket_id"))
        .agg(
            f.count_if(
                f.col("peer.created_at") >= f.col("target.as_of") - f.expr("INTERVAL 24 HOURS")
            ).alias("product_ticket_count_24h"),
            f.avg(
                f.when(
                    f.col("peer.created_at") >= f.col("target.as_of") - f.expr("INTERVAL 24 HOURS"),
                    f.coalesce(
                        f.col("peer.escalated") & (f.col("peer.closed_at") < f.col("target.as_of")),
                        f.lit(False),
                    ).cast("double"),
                )
            ).alias("product_escalation_rate_24h"),
            f.avg(
                f.when(
                    (f.col("peer.closed_at") >= f.col("target.as_of") - f.expr("INTERVAL 7 DAYS"))
                    & (f.col("peer.closed_at") < f.col("target.as_of")),
                    (f.col("peer.closed_at") > f.col("peer.sla_deadline")).cast("double"),
                )
            ).alias("product_breach_rate_7d"),
        )
    )
    scored = (
        target.join(accounts.select("customer_id", "segment"), "customer_id", "left")
        .join(customer, "ticket_id", "left")
        .join(product, "ticket_id", "left")
    )
    at = f.unix_micros("as_of") - f.lit(1)
    return scored.select(
        "as_of",
        "ticket_id",
        f.floor((at - f.unix_micros("created_at")) / 60_000_000)
        .cast("bigint")
        .alias("ticket_age_minutes"),
        f.floor((f.unix_micros("sla_deadline") - at) / 60_000_000)
        .cast("bigint")
        .alias("minutes_to_sla"),
        "priority",
        "support_tier",
        f.col("segment").alias("customer_segment"),
        f.lit(0).cast("bigint").alias("message_count"),
        f.lit(0).cast("bigint").alias("customer_message_count"),
        f.lit(0).cast("bigint").alias("agent_message_count"),
        f.lit(0).cast("bigint").alias("reopen_count"),
        f.lit(0).cast("bigint").alias("priority_change_count"),
        f.lit(0).cast("bigint").alias("previous_escalation_count"),
        "customer_ticket_count_30d",
        "customer_breach_rate_90d",
        "customer_escalation_rate_90d",
        "product_ticket_count_24h",
        "product_escalation_rate_24h",
        "product_breach_rate_7d",
        f.lit(None).cast("boolean").alias("sla_breached"),
    )


def build_historical_features(
    spark: Any, *, catalog: str, before: datetime, pipeline_run_id: str
) -> None:
    from pyspark.sql import functions as f

    before = validate_hour(before)
    if not pipeline_run_id.strip():
        raise ValueError("pipeline run ID must not be empty")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    target = qualified(catalog, "gold", "ticket_features")
    events = spark.table(qualified(catalog, "silver", "ticket_events"))
    if events.filter(f.col("event_time") < f.lit(before)).limit(1).count():
        raise ValueError("historical backfill requires a period before the first ticket event")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({FEATURES_DDL}) USING DELTA")
    if (
        spark.table(target)
        .filter(
            (f.col("as_of") < f.lit(before)) & ~f.col("_pipeline_run_id").startswith("backfill_")
        )
        .limit(1)
        .count()
    ):
        raise ValueError("backfill range overlaps an operational feature snapshot")
    frame = historical_features_frame(
        spark.table(qualified(catalog, "silver", "tickets")),
        spark.table(qualified(catalog, "silver", "contracts")),
        spark.table(qualified(catalog, "silver", "accounts")),
        before=before,
    )
    predicate = f"as_of < TIMESTAMP '{before.strftime('%Y-%m-%d %H:%M:%S')}'"
    (
        frame.withColumn("_pipeline_run_id", f.lit(f"backfill_{pipeline_run_id}"))
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .saveAsTable(target)
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--before", required=True, help="UTC hour before the first Silver event")
    args = parser.parse_args(argv)
    from pyspark.sql import SparkSession

    build_historical_features(
        SparkSession.builder.getOrCreate(),
        catalog=args.catalog,
        before=validate_hour(datetime.fromisoformat(args.before)),
        pipeline_run_id=args.pipeline_run_id,
    )
    return 0
