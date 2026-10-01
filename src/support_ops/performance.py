"""Reproducible customer-event skew experiment for Databricks Spark."""

from __future__ import annotations

import argparse
import io
import json
import time
from collections.abc import Sequence
from contextlib import redirect_stdout
from typing import Any

from support_ops.transformations.silver import qualified

RESULTS_DDL = """
    run_id STRING, strategy STRING, event_count BIGINT, customer_count BIGINT,
    shuffle_partitions INT, salt_buckets INT, elapsed_seconds DOUBLE,
    joined_count BIGINT, event_id_sum BIGINT, partition_counts_json STRING,
    plan STRING, metrics_json STRING, captured_at TIMESTAMP
"""
STRATEGIES = ("merge_baseline", "adaptive_default", "broadcast", "salted_merge")


def validate_size(
    event_count: int, customer_count: int, partitions: int, salt_buckets: int
) -> None:
    if event_count < 1 or customer_count < 3:
        raise ValueError("event_count must be positive and customer_count at least three")
    if partitions < 2 or salt_buckets < 2:
        raise ValueError("partitions and salt_buckets must be at least two")


def skewed_frames(spark: Any, *, event_count: int, customer_count: int) -> tuple[Any, Any]:
    """Create deterministic 35%/15% hot-customer events and a small account dimension."""
    from pyspark.sql import functions as f

    if event_count < 1 or customer_count < 3:
        raise ValueError("invalid benchmark size")
    event_id = f.col("id")
    bucket = event_id % 100
    customer_number = (
        f.when(bucket < 35, f.lit(1))
        .when(bucket < 50, f.lit(2))
        .otherwise(((event_id / 100).cast("bigint") % (customer_count - 2)) + 3)
    )
    events = spark.range(event_count).select(
        event_id.alias("event_id"),
        f.format_string("customer_%03d", customer_number).alias("customer_id"),
    )
    account_id = f.col("id")
    accounts = spark.range(1, customer_count + 1).select(
        f.format_string("customer_%03d", account_id).alias("customer_id"),
        f.when(account_id % 3 == 0, "Premium")
        .when(account_id % 3 == 1, "Standard")
        .otherwise("Enhanced")
        .alias("support_tier"),
    )
    return events, accounts


def partition_counts(events: Any, *, partitions: int) -> list[int]:
    """Measure pre-join hash-partition skew without relying on Spark RDD APIs."""
    from pyspark.sql import functions as f

    return sorted(
        (
            row["count"]
            for row in events.repartition(partitions, "customer_id")
            .groupBy(f.spark_partition_id().alias("partition_id"))
            .count()
            .collect()
        ),
        reverse=True,
    )


def joined_frame(events: Any, accounts: Any, *, strategy: str, salt_buckets: int) -> Any:
    from pyspark.sql import functions as f

    if strategy not in STRATEGIES:
        raise ValueError(f"unknown strategy: {strategy}")
    if strategy == "broadcast":
        return events.join(f.broadcast(accounts), "customer_id")
    if strategy == "merge_baseline":
        return events.join(accounts.hint("merge"), "customer_id")
    if strategy == "adaptive_default":
        return events.join(accounts, "customer_id")
    hot = f.col("customer_id").isin("customer_001", "customer_002")
    salted_events = events.withColumn(
        "salt", f.when(hot, f.col("event_id") % salt_buckets).otherwise(f.lit(0))
    )
    salts = accounts.sparkSession.range(salt_buckets).select(f.col("id").alias("salt"))
    salted_accounts = (
        accounts.withColumn("_replicas", f.when(hot, f.lit(salt_buckets)).otherwise(f.lit(1)))
        .join(salts, f.col("salt") < f.col("_replicas"))
        .drop("_replicas")
    )
    return salted_events.join(salted_accounts.hint("merge"), ["customer_id", "salt"])


def _plan_metrics(frame: Any) -> dict[str, int]:
    """Capture available SQL-plan counters; serverless may expose only a subset."""
    result: dict[str, int] = {}
    try:
        plan = frame._jdf.queryExecution().executedPlan()
        jvm = frame.sparkSession.sparkContext._jvm
        nodes = [plan]
        while nodes:
            node = nodes.pop()
            iterator = (
                jvm.scala.collection.JavaConverters.mapAsJavaMap(node.metrics())
                .entrySet()
                .iterator()
            )
            while iterator.hasNext():
                entry = iterator.next()
                name = str(entry.getValue().name().getOrElse(entry.getKey()))
                result[name] = result.get(name, 0) + int(entry.getValue().value())
            children = node.children().iterator()
            while children.hasNext():
                nodes.append(children.next())
    except Exception:  # Spark Connect/serverless may not expose JVM plan metrics.
        return result
    return result


def _formatted_plan(frame: Any) -> str:
    output = io.StringIO()
    with redirect_stdout(output):
        frame.explain(mode="formatted")
    return output.getvalue()


def benchmark(
    spark: Any,
    *,
    catalog: str,
    run_id: str,
    event_count: int = 1_000_000,
    customer_count: int = 1_000,
    partitions: int = 64,
    salt_buckets: int = 16,
    order: str = "forward",
) -> list[dict[str, Any]]:
    """Execute all strategies on identical deterministic data and persist results."""
    from pyspark.sql import functions as f

    validate_size(event_count, customer_count, partitions, salt_buckets)
    if order not in ("forward", "reverse"):
        raise ValueError("order must be forward or reverse")
    if not run_id or not run_id.replace("-", "").replace("_", "").isalnum():
        raise ValueError("run_id must contain only letters, numbers, hyphens, or underscores")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    events, accounts = skewed_frames(spark, event_count=event_count, customer_count=customer_count)
    if events.count() != event_count or accounts.count() != customer_count:
        raise RuntimeError("benchmark input count mismatch")
    distribution = partition_counts(events, partitions=partitions)
    rows: list[dict[str, Any]] = []
    expected: tuple[int, int] | None = None
    strategies = STRATEGIES if order == "forward" else tuple(reversed(STRATEGIES))
    for strategy in strategies:
        joined = joined_frame(events, accounts, strategy=strategy, salt_buckets=salt_buckets)
        # Referencing a dimension column prevents join elimination; the result checksum
        # verifies all strategies process the same event rows.
        result = joined.groupBy("support_tier").agg(
            f.count("event_id").alias("n"), f.sum("event_id").alias("id_sum")
        )
        start = time.perf_counter()
        totals = result.collect()
        elapsed = time.perf_counter() - start
        observed = (sum(row.n for row in totals), sum(row.id_sum for row in totals))
        if expected is None:
            expected = observed
        if observed != expected or observed[0] != event_count:
            raise RuntimeError(f"{strategy} changed the benchmark result: {observed}")
        rows.append(
            {
                "run_id": run_id,
                "strategy": strategy,
                "event_count": event_count,
                "customer_count": customer_count,
                "shuffle_partitions": partitions,
                "salt_buckets": salt_buckets,
                "elapsed_seconds": elapsed,
                "joined_count": observed[0],
                "event_id_sum": observed[1],
                "partition_counts_json": json.dumps(distribution),
                "plan": _formatted_plan(result),
                "metrics_json": json.dumps(_plan_metrics(result), sort_keys=True),
            }
        )
        print(json.dumps({k: v for k, v in rows[-1].items() if k != "plan"}), flush=True)
    target = qualified(catalog, "ops", "spark_performance_runs")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({RESULTS_DDL}) USING DELTA")
    predicate = f"run_id = '{run_id}'"
    (
        spark.createDataFrame(rows)
        .withColumn("shuffle_partitions", f.col("shuffle_partitions").cast("int"))
        .withColumn("salt_buckets", f.col("salt_buckets").cast("int"))
        .withColumn("captured_at", f.current_timestamp())
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .saveAsTable(target)
    )
    return rows


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--events", type=int, default=1_000_000)
    parser.add_argument("--customers", type=int, default=1_000)
    parser.add_argument("--partitions", type=int, default=64)
    parser.add_argument("--salt-buckets", type=int, default=16)
    parser.add_argument("--order", choices=("forward", "reverse"), default="forward")
    args = parser.parse_args(argv)
    from pyspark.sql import SparkSession

    benchmark(
        SparkSession.builder.getOrCreate(),
        catalog=args.catalog,
        run_id=args.run_id,
        event_count=args.events,
        customer_count=args.customers,
        partitions=args.partitions,
        salt_buckets=args.salt_buckets,
        order=args.order,
    )
    return 0
