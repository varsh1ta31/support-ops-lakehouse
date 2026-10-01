"""Historical feature rows use only peer facts visible at each scoring hour."""

from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

from support_ops.transformations.historical_features import historical_features_frame

pytestmark = pytest.mark.spark
HOUR = datetime(2025, 1, 1, 12, tzinfo=UTC)


def test_backfill_peer_visibility_and_contract_sla(spark: Any) -> None:
    ticket_schema = (
        "ticket_id STRING, customer_id STRING, product_id STRING, created_at TIMESTAMP, "
        "closed_at TIMESTAMP, priority STRING, escalated BOOLEAN"
    )
    tickets = spark.createDataFrame(
        [
            (
                "first",
                "c1",
                "p1",
                HOUR - timedelta(hours=2),
                HOUR + timedelta(minutes=30),
                "P1",
                True,
            ),
            (
                "second",
                "c1",
                "p1",
                HOUR - timedelta(minutes=50),
                HOUR + timedelta(hours=1),
                "P1",
                False,
            ),
            (
                "third",
                "c1",
                "p1",
                HOUR + timedelta(minutes=20),
                HOUR + timedelta(hours=2),
                "P1",
                False,
            ),
            (
                "fast",
                "c2",
                "p2",
                HOUR + timedelta(minutes=1),
                HOUR + timedelta(minutes=10),
                "P1",
                False,
            ),
        ],
        ticket_schema,
    )
    contracts = spark.createDataFrame(
        [
            ("sla", "c1", "P1", "Premium", 120, date(2024, 1, 1), None),
            ("sla2", "c2", "P1", "Standard", 120, date(2024, 1, 1), None),
        ],
        "contract_id STRING, customer_id STRING, priority STRING, support_tier STRING, "
        "resolution_sla_minutes INT, effective_from DATE, effective_to DATE",
    )
    accounts = spark.createDataFrame(
        [("c1", "Enterprise"), ("c2", "SMB")], "customer_id STRING, segment STRING"
    )
    rows = {
        row.ticket_id: row
        for row in historical_features_frame(
            tickets, contracts, accounts, before=HOUR + timedelta(days=1)
        ).collect()
    }
    assert set(rows) == {"first", "second", "third"}
    assert (rows["first"].ticket_age_minutes, rows["first"].minutes_to_sla) == (59, 60)
    assert rows["second"].customer_ticket_count_30d == 1
    assert rows["second"].customer_breach_rate_90d is None
    assert rows["second"].customer_escalation_rate_90d == 0.0
    assert rows["third"].customer_ticket_count_30d == 2
    assert rows["third"].customer_breach_rate_90d == 1.0
    assert rows["third"].customer_escalation_rate_90d == 0.5
    assert rows["third"].product_breach_rate_7d == 1.0
    assert rows["third"].previous_escalation_count == 0
    assert rows["third"].customer_segment == "Enterprise"
