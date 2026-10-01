"""Known hourly cohorts, historical reconstruction, and atomic Delta replay."""

from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

from support_ops.transformations import (
    customer_health,
    gold,
    incident_signals,
    silver,
    ticket_features,
)

pytestmark = pytest.mark.spark
HOUR = datetime(2026, 1, 1, 12, tzinfo=UTC)


def replace_silver(spark: Any, entity: str, records: list[dict[str, Any]]) -> None:
    silver.create_tables(spark, "spark_catalog", entity)
    schema = spark.table(f"silver.{entity}").schema
    defaults = {
        "string": "fixture",
        "double": 100.0,
        "int": 10,
        "boolean": False,
        "timestamp": HOUR,
        "date": HOUR.date(),
    }
    rows = [
        {
            field.name: row.get(
                field.name, None if field.nullable else defaults[field.dataType.simpleString()]
            )
            for field in schema
        }
        for row in records
    ]
    spark.createDataFrame(rows, schema).write.format("delta").mode("overwrite").saveAsTable(
        f"silver.{entity}"
    )


def test_hourly_history_and_replay(spark: Any) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    ticket = dict(
        customer_id="c1",
        product_id="p1",
        priority="P1",
        final_status="resolved",
        escalated=False,
        created_at=HOUR,
    )
    replace_silver(spark, "accounts", [dict(customer_id="c1", segment="enterprise")])
    replace_silver(
        spark,
        "contracts",
        [
            dict(
                contract_id="k1",
                customer_id="c1",
                priority="P1",
                support_tier="premium",
                resolution_sla_minutes=30,
                effective_from=date(2025, 1, 1),
            )
        ],
    )
    replace_silver(
        spark,
        "tickets",
        [
            dict(ticket, ticket_id="a", closed_at=HOUR + timedelta(minutes=30)),
            dict(ticket, ticket_id="b", closed_at=HOUR + timedelta(minutes=45), escalated=True),
            dict(ticket, ticket_id="c", closed_at=HOUR + timedelta(hours=1)),
            dict(
                ticket,
                ticket_id="future",
                created_at=HOUR + timedelta(hours=1),
                closed_at=HOUR + timedelta(hours=2),
            ),
            dict(ticket, ticket_id="unknown", customer_id="missing", final_status="open"),
        ],
    )
    replace_silver(
        spark,
        "ticket_events",
        [
            dict(
                event_id="future-priority",
                ticket_id="c",
                event_type="priority_changed",
                event_time=HOUR + timedelta(hours=2),
                new_value="P2",
            )
        ],
    )

    def run(hour: datetime = HOUR) -> None:
        gold.build_support_operations(
            spark, catalog="spark_catalog", hour=hour, pipeline_run_id="run"
        )

    run()
    rows = {r.support_tier: r for r in spark.table("gold.support_operations").collect()}
    known = rows["premium"]
    assert (known.tickets_created, known.tickets_resolved, known.open_backlog) == (3, 2, 1)
    assert known.p1_ticket_count == 1
    assert known.p2_ticket_count == 0
    assert known.median_resolution_minutes == 37.5
    assert known.sla_breach_rate == 0.5
    assert known.escalation_rate == pytest.approx(1 / 3)
    assert known.reopen_rate == 0
    assert rows["unknown"].customer_segment == "unknown"
    assert rows["unknown"].sla_breach_rate is None
    assert rows["unknown"].median_resolution_minutes is None
    run()
    assert spark.table("gold.support_operations").count() == 2
    run(HOUR + timedelta(hours=1))
    later = (
        spark.table("gold.support_operations")
        .filter("hour = TIMESTAMP '2026-01-01 13:00:00' AND support_tier = 'premium'")
        .first()
    )
    assert later.tickets_created == 1
    assert later.tickets_resolved == 1
    assert later.open_backlog == 1
    # Recompute an empty hour: remove stale dimension groups, preserving other hours.
    replace_silver(spark, "tickets", [])
    replace_silver(spark, "ticket_events", [])
    run()
    remaining = spark.table("gold.support_operations").collect()
    assert len(remaining) == 2
    assert (
        spark.table("gold.support_operations")
        .filter("hour <> TIMESTAMP '2026-01-01 13:00:00'")
        .count()
        == 0
    )


def test_customer_health_windows_and_replay(spark: Any) -> None:
    as_of = HOUR
    ticket = dict(product_id="p1", priority="P1", final_status="resolved", escalated=False)
    replace_silver(
        spark,
        "accounts",
        [
            dict(customer_id="c1", support_tier="platinum", annual_contract_value=1200.0),
            dict(customer_id="c2", support_tier="standard", annual_contract_value=300.0),
            dict(customer_id="c3", support_tier="standard", annual_contract_value=500.0),
        ],
    )
    replace_silver(
        spark,
        "contracts",
        [
            dict(
                contract_id="p1",
                customer_id="c1",
                priority="P1",
                support_tier="premium",
                resolution_sla_minutes=30,
                effective_from=date(2025, 1, 1),
            ),
            dict(
                contract_id="p2",
                customer_id="c1",
                priority="P2",
                support_tier="premium",
                resolution_sla_minutes=60,
                effective_from=date(2025, 1, 1),
            ),
        ],
    )
    replace_silver(
        spark,
        "tickets",
        [
            dict(
                ticket,
                ticket_id="t1",
                customer_id="c1",
                created_at=as_of - timedelta(days=10),
                closed_at=as_of - timedelta(days=9),
                escalated=True,
            ),
            dict(
                ticket,
                ticket_id="t2",
                customer_id="c1",
                priority="P2",
                created_at=as_of - timedelta(days=30),
                closed_at=as_of - timedelta(days=1),
            ),
            dict(
                ticket,
                ticket_id="t3",
                customer_id="c1",
                created_at=as_of - timedelta(days=90),
                closed_at=as_of - timedelta(days=90) + timedelta(minutes=30),
            ),
            dict(
                ticket,
                ticket_id="t4",
                customer_id="c1",
                final_status="open",
                created_at=as_of - timedelta(days=20),
                closed_at=None,
            ),
            dict(
                ticket,
                ticket_id="old",
                customer_id="c3",
                final_status="open",
                created_at=as_of - timedelta(days=91),
                closed_at=None,
            ),
            dict(
                ticket,
                ticket_id="future",
                customer_id="c1",
                final_status="open",
                created_at=as_of,
                closed_at=None,
            ),
        ],
    )
    replace_silver(
        spark,
        "ticket_events",
        [
            dict(
                event_id="resolved",
                ticket_id="t4",
                customer_id="c1",
                product_id="p1",
                event_type="ticket_resolved",
                event_time=as_of - timedelta(days=19),
            ),
            dict(
                event_id="reopened",
                ticket_id="t4",
                customer_id="c1",
                product_id="p1",
                event_type="ticket_reopened",
                event_time=as_of - timedelta(days=18),
            ),
        ],
    )

    def run(point: datetime = as_of) -> None:
        customer_health.build_customer_health(
            spark, catalog="spark_catalog", as_of=point, pipeline_run_id="health-run"
        )

    run()
    rows = {row.customer_id: row for row in spark.table("gold.customer_support_health").collect()}
    assert set(rows) == {"c1", "c2", "c3"}
    c1 = rows["c1"]
    assert c1.support_tier == "platinum"
    assert c1.annual_contract_value == 1200.0
    assert (c1.open_ticket_count, c1.p1_ticket_count, c1.ticket_count_30d) == (1, 1, 3)
    assert c1.breach_rate_90d == pytest.approx(2 / 3)
    assert c1.escalation_rate_90d == 0.25
    assert c1.reopen_rate_90d == 0.25
    assert c1.average_resolution_minutes == 14410.0
    assert rows["c2"].open_ticket_count == 0
    assert rows["c2"].ticket_count_30d == 0
    assert rows["c2"].breach_rate_90d is None
    assert rows["c3"].open_ticket_count == 1
    assert rows["c3"].ticket_count_30d == 0
    assert rows["c3"].escalation_rate_90d is None
    run()
    assert spark.table("gold.customer_support_health").count() == 3
    run(as_of + timedelta(hours=1))
    later = (
        spark.table("gold.customer_support_health")
        .filter("as_of = TIMESTAMP '2026-01-01 13:00:00' AND customer_id = 'c1'")
        .first()
    )
    assert later.open_ticket_count == 2
    # The new boundary ticket enters as the exactly-30-day-old ticket leaves.
    assert later.ticket_count_30d == 3
    assert later.breach_rate_90d == 1.0
    replace_silver(
        spark,
        "accounts",
        [
            dict(customer_id="c1", support_tier="platinum", annual_contract_value=1200.0),
            dict(customer_id="c3", support_tier="standard", annual_contract_value=500.0),
        ],
    )
    run()
    assert spark.table("gold.customer_support_health").count() == 5
    assert (
        spark.table("gold.customer_support_health")
        .filter("as_of = TIMESTAMP '2026-01-01 12:00:00' AND customer_id = 'c2'")
        .count()
        == 0
    )
    # The Spark session is shared with Silver integration tests.
    for entity in ("tickets", "ticket_events", "accounts", "contracts"):
        replace_silver(spark, entity, [])


def test_incident_signals_baseline_and_replay(spark: Any) -> None:
    hour = HOUR
    replace_silver(
        spark, "products", [dict(product_id="p1"), dict(product_id="p2"), dict(product_id="p3")]
    )
    base = dict(final_status="open", closed_at=None, priority="P2", escalated=False)
    rows = [
        dict(
            base,
            ticket_id=f"baseline-{day}",
            customer_id=f"b{day}",
            product_id="p1",
            created_at=hour - timedelta(days=day) + timedelta(minutes=5),
        )
        for day in (1, 2, 4, 5, 6, 7)
    ]
    rows += [
        dict(
            base,
            ticket_id="too-old",
            customer_id="old",
            product_id="p1",
            created_at=hour - timedelta(days=8) + timedelta(minutes=5),
        ),
        dict(
            base,
            ticket_id="wrong-hour",
            customer_id="wrong",
            product_id="p1",
            created_at=hour - timedelta(days=3, hours=1),
        ),
    ]
    rows += [
        dict(
            base,
            ticket_id=f"current-{i}",
            customer_id=f"c{i % 3}",
            product_id="p1",
            priority="P1" if i < 2 else "P2",
            escalated=i == 0,
            created_at=hour + timedelta(minutes=i),
        )
        for i in range(5)
    ]
    rows += [
        dict(
            base,
            ticket_id=f"quiet-{i}",
            customer_id=f"q{i}",
            product_id="p2",
            created_at=hour + timedelta(minutes=i),
        )
        for i in range(4)
    ]
    rows.append(
        dict(
            base,
            ticket_id="next-hour",
            customer_id="future",
            product_id="p1",
            created_at=hour + timedelta(hours=1),
        )
    )
    replace_silver(spark, "tickets", rows)
    replace_silver(spark, "ticket_events", [])
    replace_silver(spark, "contracts", [])

    def run(point: datetime = hour) -> None:
        incident_signals.build_incident_signals(
            spark, catalog="spark_catalog", hour=point, pipeline_run_id="signal-run"
        )

    run()
    signals = {r.product_id: r for r in spark.table("gold.incident_signals").collect()}
    assert set(signals) == {"p1", "p2", "p3"}
    spike = signals["p1"]
    assert (spike.ticket_count, spike.unique_customers, spike.p1_count, spike.escalation_count) == (
        5,
        3,
        2,
        1,
    )
    assert spike.ticket_volume_baseline == pytest.approx(6 / 7)
    assert spike.volume_deviation == pytest.approx(5 - 6 / 7)
    assert spike.incident_signal is True
    quiet = signals["p2"]
    assert quiet.ticket_count == 4
    assert quiet.ticket_volume_baseline == 0
    assert quiet.incident_signal is False
    assert signals["p3"].ticket_count == 0
    assert signals["p3"].incident_signal is False
    run()
    assert spark.table("gold.incident_signals").count() == 3
    run(hour + timedelta(hours=1))
    later = (
        spark.table("gold.incident_signals")
        .filter("time_window = TIMESTAMP '2026-01-01 13:00:00' AND product_id = 'p1'")
        .first()
    )
    assert later.ticket_count == 1
    assert later.incident_signal is False
    replace_silver(spark, "products", [dict(product_id="p1"), dict(product_id="p2")])
    run()
    assert spark.table("gold.incident_signals").count() == 5
    assert (
        spark.table("gold.incident_signals")
        .filter("time_window = TIMESTAMP '2026-01-01 12:00:00' AND product_id = 'p3'")
        .count()
        == 0
    )
    # The Spark session is shared with downstream Silver integration tests.
    for entity in ("tickets", "ticket_events", "products", "contracts"):
        replace_silver(spark, entity, [])


def test_ticket_features_point_in_time_and_replay(spark: Any) -> None:
    as_of = HOUR
    replace_silver(spark, "accounts", [dict(customer_id="c1", segment="Enterprise")])
    replace_silver(
        spark,
        "contracts",
        [
            dict(
                contract_id="sla",
                customer_id="c1",
                priority="P1",
                support_tier="Premium",
                resolution_sla_minutes=120,
                effective_from=date(2025, 1, 1),
            )
        ],
    )
    base = dict(
        customer_id="c1",
        product_id="p1",
        priority="P1",
        final_status="open",
        closed_at=None,
        escalated=False,
    )
    replace_silver(
        spark,
        "tickets",
        [
            dict(base, ticket_id="active", created_at=as_of - timedelta(hours=1)),
            dict(
                base,
                ticket_id="breached",
                final_status="resolved",
                escalated=True,
                created_at=as_of - timedelta(hours=3),
                closed_at=as_of - timedelta(minutes=30),
            ),
            dict(base, ticket_id="old", created_at=as_of - timedelta(days=31)),
            dict(base, ticket_id="future", created_at=as_of),
        ],
    )
    replace_silver(
        spark,
        "ticket_events",
        [
            dict(
                event_id="reply-c",
                ticket_id="active",
                customer_id="c1",
                product_id="p1",
                event_type="customer_replied",
                event_time=as_of - timedelta(minutes=40),
            ),
            dict(
                event_id="reply-a",
                ticket_id="active",
                customer_id="c1",
                product_id="p1",
                event_type="agent_replied",
                event_time=as_of - timedelta(minutes=30),
            ),
            dict(
                event_id="escalate",
                ticket_id="active",
                customer_id="c1",
                product_id="p1",
                event_type="engineering_escalated",
                event_time=as_of - timedelta(minutes=20),
            ),
            dict(
                event_id="future-reply",
                ticket_id="active",
                customer_id="c1",
                product_id="p1",
                event_type="customer_replied",
                event_time=as_of + timedelta(minutes=1),
            ),
        ],
    )

    def run(point: datetime = as_of) -> None:
        ticket_features.build_ticket_features(
            spark, catalog="spark_catalog", as_of=point, pipeline_run_id="feature-run"
        )

    run()
    rows = {row.ticket_id: row for row in spark.table("gold.ticket_features").collect()}
    assert set(rows) == {"active", "old"}
    active = rows["active"]
    assert (active.ticket_age_minutes, active.minutes_to_sla) == (59, 60)
    assert (active.message_count, active.customer_message_count, active.agent_message_count) == (
        2,
        1,
        1,
    )
    assert active.previous_escalation_count == 1
    assert active.customer_segment == "Enterprise"
    assert active.customer_ticket_count_30d == 1
    assert active.customer_breach_rate_90d == 1.0
    assert active.product_ticket_count_24h == 1
    assert active.product_escalation_rate_24h == 1.0
    assert active.product_breach_rate_7d == 1.0
    assert active.sla_breached is None
    run()
    assert spark.table("gold.ticket_features").count() == 2
    run(as_of + timedelta(hours=1))
    assert spark.table("gold.ticket_features").count() == 5
    replace_silver(spark, "tickets", [])
    replace_silver(spark, "ticket_events", [])
    run()
    assert spark.table("gold.ticket_features").count() == 3
    for entity in ("tickets", "ticket_events", "accounts", "contracts"):
        replace_silver(spark, entity, [])
