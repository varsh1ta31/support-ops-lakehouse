"""Join historical scoring features to later observed SLA outcomes."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from support_ops.transformations.gold import validate_hour
from support_ops.transformations.silver import qualified
from support_ops.transformations.ticket_state import ticket_state_frame

FEATURE_COLUMNS = (
    "ticket_age_minutes",
    "minutes_to_sla",
    "priority",
    "support_tier",
    "customer_segment",
    "message_count",
    "customer_message_count",
    "agent_message_count",
    "reopen_count",
    "priority_change_count",
    "previous_escalation_count",
    "customer_ticket_count_30d",
    "customer_breach_rate_90d",
    "customer_escalation_rate_90d",
    "product_ticket_count_24h",
    "product_escalation_rate_24h",
    "product_breach_rate_7d",
)
TRAINING_DDL = """
    label_as_of TIMESTAMP, as_of TIMESTAMP, ticket_id STRING,
    ticket_age_minutes BIGINT, minutes_to_sla BIGINT,
    priority STRING, support_tier STRING, customer_segment STRING,
    message_count BIGINT, customer_message_count BIGINT, agent_message_count BIGINT,
    reopen_count BIGINT, priority_change_count BIGINT, previous_escalation_count BIGINT,
    customer_ticket_count_30d BIGINT, customer_breach_rate_90d DOUBLE,
    customer_escalation_rate_90d DOUBLE, product_ticket_count_24h BIGINT,
    product_escalation_rate_24h DOUBLE, product_breach_rate_7d DOUBLE,
    resolved_at TIMESTAMP, label INT, split STRING, _pipeline_run_id STRING
"""


def validate_periods(
    *, label_as_of: datetime, train_end: datetime, validation_end: datetime, test_end: datetime
) -> tuple[datetime, datetime, datetime, datetime]:
    dates = tuple(
        validate_hour(value) for value in (label_as_of, train_end, validation_end, test_end)
    )
    cutoff, train, validation, test = dates
    if not train < validation < test <= cutoff:
        raise ValueError("require train_end < validation_end < test_end <= label_as_of")
    return cutoff, train, validation, test


def training_frame(
    features: Any,
    outcomes: Any,
    *,
    label_as_of: datetime,
    train_end: datetime,
    validation_end: datetime,
    test_end: datetime,
) -> Any:
    """Select the earliest eligible snapshot per ticket, then assign chronological splits.

    The outcome is taken after the feature timestamp and by the label cutoff. Tickets
    with no observed resolution, no SLA, a reopened episode, or an already expired SLA
    at scoring time are excluded. Those are censored or ambiguous examples, not negatives.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as f

    label_as_of, train_end, validation_end, test_end = validate_periods(
        label_as_of=label_as_of,
        train_end=train_end,
        validation_end=validation_end,
        test_end=test_end,
    )
    earliest = Window.partitionBy("ticket_id").orderBy("as_of")
    scored = (
        features.filter(
            (f.col("as_of") < f.lit(test_end))
            & (f.col("minutes_to_sla") > 0)
            & f.col("sla_breached").isNull()
        )
        .withColumn("_rank", f.row_number().over(earliest))
        .filter(f.col("_rank") == 1)
        .drop("_rank")
        .alias("scored")
    )
    observed = outcomes.select("ticket_id", "resolved_at", "sla_deadline", "reopen_count").alias(
        "outcome"
    )
    return (
        scored.join(observed, "ticket_id")
        .filter(
            (f.col("resolved_at") > f.col("as_of"))
            & (f.col("resolved_at") <= f.lit(label_as_of))
            & f.col("sla_deadline").isNotNull()
            & (f.col("outcome.reopen_count") == 0)
        )
        .select(
            f.lit(label_as_of).alias("label_as_of"),
            "as_of",
            "ticket_id",
            *(f.col(f"scored.{name}").alias(name) for name in FEATURE_COLUMNS),
            "resolved_at",
            (f.col("resolved_at") > f.col("sla_deadline")).cast("int").alias("label"),
            f.when(f.col("as_of") < f.lit(train_end), "train")
            .when(f.col("as_of") < f.lit(validation_end), "validation")
            .otherwise("test")
            .alias("split"),
        )
    )


def build_training_dataset(
    spark: Any,
    *,
    catalog: str,
    pipeline_run_id: str,
    label_as_of: datetime,
    train_end: datetime,
    validation_end: datetime,
    test_end: datetime,
) -> dict[str, int]:
    from pyspark.sql import functions as f

    label_as_of, train_end, validation_end, test_end = validate_periods(
        label_as_of=label_as_of,
        train_end=train_end,
        validation_end=validation_end,
        test_end=test_end,
    )
    if not pipeline_run_id.strip():
        raise ValueError("pipeline run ID must not be empty")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    features = spark.table(qualified(catalog, "gold", "ticket_features"))
    outcomes = ticket_state_frame(spark, catalog=catalog, as_of=label_as_of)
    frame = training_frame(
        features,
        outcomes,
        label_as_of=label_as_of,
        train_end=train_end,
        validation_end=validation_end,
        test_end=test_end,
    )
    target = qualified(catalog, "ml", "training_dataset")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({TRAINING_DDL}) USING DELTA")
    predicate = f"label_as_of = TIMESTAMP '{label_as_of.strftime('%Y-%m-%d %H:%M:%S')}'"
    (
        frame.withColumn("_pipeline_run_id", f.lit(pipeline_run_id))
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .saveAsTable(target)
    )
    counts = spark.table(target).filter(f.col("label_as_of") == f.lit(label_as_of))
    return {row["split"]: row["count"] for row in counts.groupBy("split").count().collect()}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--pipeline-run-id", required=True)
    parser.add_argument("--label-as-of", required=True)
    parser.add_argument("--train-end", required=True)
    parser.add_argument("--validation-end", required=True)
    parser.add_argument("--test-end", required=True)
    args = parser.parse_args(argv)
    from pyspark.sql import SparkSession

    counts = build_training_dataset(
        SparkSession.builder.getOrCreate(),
        catalog=args.catalog,
        pipeline_run_id=args.pipeline_run_id,
        label_as_of=datetime.fromisoformat(args.label_as_of),
        train_end=datetime.fromisoformat(args.train_end),
        validation_end=datetime.fromisoformat(args.validation_end),
        test_end=datetime.fromisoformat(args.test_end),
    )
    print(f"Training dataset split counts: {counts}")
    return 0
