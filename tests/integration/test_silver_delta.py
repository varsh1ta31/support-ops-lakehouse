"""Real Spark/Delta tests: install .[spark-test], supply Java 17+, then run pytest."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from support_ops.schemas import INGESTION_METADATA, SOURCE_SCHEMAS
from support_ops.transformations import silver, ticket_state
from tests.unit.quality.test_validation import generated

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Any]:
    pytest.importorskip("pyspark")
    delta = pytest.importorskip("delta")
    from pyspark.sql import SparkSession

    root = tmp_path_factory.mktemp("silver-delta")
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("support-ops-silver-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(root / "warehouse"))
        .config("spark.jars.ivy", str(Path(os.environ.get("SPARK_TEST_IVY", root / "ivy"))))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
    )
    session = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    for name in ("bronze", "silver", "ops"):
        session.sql(f"CREATE DATABASE {name} LOCATION '{root / name}'")
    for entity in ("accounts", "products"):
        silver.create_tables(session, "spark_catalog", entity)
    yield session
    session.stop()


def bronze(spark: Any, entity: str, rows: list[dict[str, object]], *, append: bool = False) -> None:
    data = []
    for row in rows:
        raw: dict[str, object] = {
            key: None if value is None else str(value) for key, value in row.items()
        }
        raw.update(
            {
                "_ingested_at": datetime(2026, 1, 1, tzinfo=UTC),
                "_source_file": f"landing/{entity}.csv",
                "_pipeline_run_id": "bronze-run",
                "_record_hash": hashlib.sha256(
                    json.dumps(row, sort_keys=True, default=str).encode()
                ).hexdigest(),
            }
        )
        data.append(raw)
    ddl = ", ".join(f"{field.name} STRING" for field in SOURCE_SCHEMAS[entity].fields)
    ddl += ", " + ", ".join(f"{field.name} {field.data_type.value}" for field in INGESTION_METADATA)
    spark.createDataFrame(data, ddl).write.format("delta").mode(
        "append" if append else "overwrite"
    ).saveAsTable(f"bronze.{entity}")


def run(spark: Any, entity: str, run_id: str) -> dict[str, object]:
    return silver.transform_entity(
        spark,
        catalog="spark_catalog",
        entity=entity,
        pipeline_run_id=run_id,
        now=datetime(2026, 1, 1, tzinfo=UTC),
    )


def test_delta_pipeline_replay_quarantine_and_partial_failure(
    spark: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records, _ = generated()
    for entity in silver.BATCH_ENTITIES:
        rows = records[entity] if entity != "tickets" else records[entity][:4]
        if entity == "tickets":
            # Two valid variants of one logical key; malformed variants must still quarantine.
            rows = [*rows, {**rows[0], "subject": "changed valid payload"}, *records["invalid"]]
        bronze(spark, entity, rows)
    for entity in ("products", "accounts", "contracts"):
        metric = run(spark, entity, "initial")
        assert metric["accepted_count"] == len(records[entity])

    original_merge = silver.merge_insert
    failed = False

    def fail_after_silver(spark: Any, frame: Any, target: str, keys: Any) -> None:
        nonlocal failed
        if "invalid_records" in target and not failed:
            failed = True
            raise RuntimeError("simulated interruption after Silver commit")
        original_merge(spark, frame, target, keys)

    with monkeypatch.context() as patch:
        patch.setattr(silver, "merge_insert", fail_after_silver)
        with pytest.raises(RuntimeError, match="simulated interruption"):
            run(spark, "tickets", "initial")
    assert spark.table("silver.tickets").count() == 4
    assert spark.table("ops.quality_metrics").filter("source = 'tickets'").count() == 0
    # New input arriving between failure and retry must not alter the frozen run.
    bronze(spark, "tickets", [{**records["tickets"][0], "ticket_id": "later-ticket"}], append=True)
    metric = run(spark, "tickets", "initial")
    assert metric["records_read"] == 13
    assert metric["accepted_count"] == 4
    assert metric["duplicate_count"] == 1
    assert metric["quarantine_count"] == 8
    assert spark.table("silver.tickets").count() == 4
    assert spark.table("ops.invalid_records").count() == 8
    raw = spark.table("ops.invalid_records").first().raw_payload
    assert json.loads(raw)["_source_file"] == "landing/tickets.csv"
    assert run(spark, "tickets", "initial")["accepted_count"] == 4
    rerun = run(spark, "tickets", "rerun")
    assert rerun["accepted_count"] == 1
    assert rerun["duplicate_count"] == 5
    assert rerun["quarantine_count"] == 8
    assert spark.table("silver.tickets").count() == 5
    assert spark.table("ops.invalid_records").count() == 8
    assert spark.table("ops.quality_metrics").filter("source = 'tickets'").count() == 2
    assert spark.table("silver.tickets").schema["created_at"].dataType.simpleString() == "timestamp"
    assert spark.table("silver.tickets").filter("_validation_run_id IS NULL").count() == 0


def test_empty_snapshot_survives_retry(spark: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    bronze(spark, "ticket_events", [])
    original = silver.write_outputs

    def fail(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("output unavailable")

    with monkeypatch.context() as patch:
        patch.setattr(silver, "write_outputs", fail)
        with pytest.raises(RuntimeError, match="output unavailable"):
            run(spark, "ticket_events", "empty")
    from tests.unit.quality.test_validation import EVENT

    bronze(spark, "ticket_events", [EVENT], append=True)
    assert silver.write_outputs is original
    result = run(spark, "ticket_events", "empty")
    assert result["records_read"] == 0
    assert spark.table("silver.ticket_events").count() == 0
    assert run(spark, "ticket_events", "after-empty")["quarantine_count"] == 1


def test_invalid_arguments_and_cli(spark: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(ValueError, match="Unsupported"):
        run(spark, "no-such-entity", "run")
    with pytest.raises(ValueError, match="must not be empty"):
        run(spark, "tickets", " ")
    called = []

    def transform(*args: Any, **kwargs: Any) -> dict[str, object]:
        called.append(kwargs["entity"])
        return {"entity": kwargs["entity"]}

    monkeypatch.setattr(silver, "transform_entity", transform)

    def build(*args: Any, **kwargs: Any) -> dict[str, object]:
        called.append("state")
        return {}

    monkeypatch.setattr(ticket_state, "build_ticket_state", build)
    assert silver.main(["--catalog", "spark_catalog", "--pipeline-run-id", "cli"]) == 0
    assert called == [*silver.BATCH_ENTITIES, "state"]


def silver_events(spark: Any, rows: list[dict[str, object]]) -> None:
    metadata = {
        "_ingested_at": datetime(2026, 1, 1, tzinfo=UTC),
        "_source_file": "landing/events.json",
        "_pipeline_run_id": "bronze-run",
        "_record_hash": "h",
        "_validated_at": datetime(2026, 1, 1, tzinfo=UTC),
        "_validation_run_id": "silver-run",
    }
    table = spark.table("silver.ticket_events")
    full = [
        {
            name: {**metadata, **row, "_record_hash": row["event_id"]}.get(name)
            for name in table.columns
        }
        for row in rows
    ]
    spark.createDataFrame(full, table.schema).write.insertInto("silver.ticket_events")


def test_ticket_state_reconstructs_known_sequence(spark: Any) -> None:
    silver.create_tables(spark, "spark_catalog", "ticket_events")
    # Collected timestamps are naive local times; astimezone interprets them correctly.
    ticket = (
        spark.table("silver.tickets")
        .filter("final_status = 'resolved'")
        .orderBy("ticket_id")
        .first()
        .asDict()
    )
    start: datetime = ticket["created_at"].astimezone(UTC) + timedelta(days=400)
    common = {"customer_id": ticket["customer_id"], "product_id": ticket["product_id"]}

    def at(minutes: int) -> datetime:
        return start + timedelta(minutes=minutes)

    silver_events(
        spark,
        [
            {
                **common,
                "event_id": "s1",
                "ticket_id": ticket["ticket_id"],
                "event_type": "ticket_reopened",
                "event_time": at(1),
            },
            {
                **common,
                "event_id": "s3",
                "ticket_id": ticket["ticket_id"],
                "event_type": "priority_changed",
                "event_time": at(3),
                "new_value": "P1",
            },
            {
                **common,
                "event_id": "n1",
                "ticket_id": "EVENT-ONLY",
                "event_type": "ticket_created",
                "event_time": at(0),
                "new_value": "P2",
            },
            {
                **common,
                "event_id": "n2",
                "ticket_id": "EVENT-ONLY",
                "event_type": "agent_replied",
                "event_time": at(5),
            },
            {
                **common,
                "event_id": "x1",
                "ticket_id": "ORPHAN",
                "event_type": "agent_replied",
                "event_time": at(5),
            },
            {
                **common,
                "event_id": "f1",
                "ticket_id": "EVENT-ONLY",
                "event_type": "ticket_resolved",
                "event_time": at(500),
            },
        ],
    )
    # Arrives after later events but must be ordered by event time.
    silver_events(
        spark,
        [
            {
                **common,
                "event_id": "s2",
                "ticket_id": ticket["ticket_id"],
                "event_type": "agent_assigned",
                "event_time": at(2),
                "agent_id": "late-agent",
            }
        ],
    )
    expected_tickets = spark.table("silver.tickets").count() + 1
    summary = ticket_state.build_ticket_state(
        spark, catalog="spark_catalog", pipeline_run_id="state-1", as_of=at(60)
    )
    assert summary["tickets"] == expected_tickets
    rows = {row.ticket_id: row for row in spark.table("silver.ticket_state").collect()}
    assert len(rows) == expected_tickets and "ORPHAN" not in rows
    reopened = rows[ticket["ticket_id"]]
    assert (reopened.status, reopened.priority, reopened.assigned_agent) == (
        "open",
        "P1",
        "late-agent",
    )
    assert reopened.reopen_count == 1 and reopened.resolved_at is None
    assert reopened.last_updated_at.astimezone(UTC) == at(3)
    created = rows["EVENT-ONLY"]
    assert (created.status, created.message_count, created.minutes_open) == ("open", 1, 60)
    assert created.created_at.astimezone(UTC) == at(0)
    assert created.state_as_of.astimezone(UTC) == at(60)
    contracts = {
        (row.customer_id, row.priority): row for row in spark.table("silver.contracts").collect()
    }
    # The priority change re-targets the SLA; the clock still starts at ticket creation.
    terms = contracts[(ticket["customer_id"], "P1")]
    assert (reopened.contract_id, reopened.support_tier) == (terms.contract_id, terms.support_tier)
    assert reopened.resolution_sla_minutes == terms.resolution_sla_minutes
    deadline = ticket["created_at"].astimezone(UTC) + timedelta(
        minutes=terms.resolution_sla_minutes
    )
    assert reopened.sla_deadline.astimezone(UTC) == deadline
    assert reopened.minutes_to_sla == (deadline - at(60)).total_seconds() // 60 < 0
    terms = contracts[(ticket["customer_id"], "P2")]
    assert created.contract_id == terms.contract_id
    assert created.minutes_to_sla == terms.resolution_sla_minutes - 60
    assert summary["without_sla"] == 0
    again = ticket_state.build_ticket_state(
        spark, catalog="spark_catalog", pipeline_run_id="state-1", as_of=at(60)
    )
    assert again == summary
    assert spark.table("silver.ticket_state").count() == expected_tickets
