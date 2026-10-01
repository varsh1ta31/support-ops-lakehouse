"""Feature snapshots become labels only after a later observed resolution."""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from support_ops.training_dataset import FEATURE_COLUMNS, training_frame
from support_ops.transformations.ticket_features import FEATURES_DDL

pytestmark = pytest.mark.spark
HOUR = datetime(2026, 1, 1, 5, tzinfo=UTC)


def test_chronological_labels_and_censoring(spark: Any) -> None:
    def feature(ticket: str, hour: int, minutes: int = 60) -> dict[str, Any]:
        return {
            "ticket_id": ticket,
            "as_of": HOUR + timedelta(hours=hour),
            "minutes_to_sla": minutes,
            "sla_breached": None,
            **{
                name: 0
                for name in FEATURE_COLUMNS
                if name
                not in (
                    "minutes_to_sla",
                    "priority",
                    "support_tier",
                    "customer_segment",
                    "customer_breach_rate_90d",
                    "customer_escalation_rate_90d",
                    "product_escalation_rate_24h",
                    "product_breach_rate_7d",
                )
            },
            "priority": "P1",
            "support_tier": "Premium",
            "customer_segment": "Enterprise",
        }

    features = spark.createDataFrame(
        [
            feature("train", 0),
            feature("train", 1),
            feature("validation", 1),
            feature("test", 2),
            feature("censored", 0),
            feature("overdue", 0, -1),
            feature("reopened", 0),
            feature("resolved_before", 1),
        ],
        FEATURES_DDL,
    )
    outcomes = spark.createDataFrame(
        [
            ("train", HOUR + timedelta(hours=4), HOUR + timedelta(hours=3), 0),
            ("validation", HOUR + timedelta(hours=3), HOUR + timedelta(hours=4), 0),
            ("test", HOUR + timedelta(hours=4), HOUR + timedelta(hours=3), 0),
            ("censored", None, HOUR + timedelta(hours=3), 0),
            ("overdue", HOUR + timedelta(hours=2), HOUR + timedelta(hours=1), 0),
            ("reopened", HOUR + timedelta(hours=3), HOUR + timedelta(hours=2), 1),
            ("resolved_before", HOUR, HOUR + timedelta(hours=1), 0),
        ],
        "ticket_id STRING, resolved_at TIMESTAMP, sla_deadline TIMESTAMP, reopen_count INT",
    )
    rows = {
        row.ticket_id: row
        for row in training_frame(
            features,
            outcomes,
            label_as_of=HOUR + timedelta(hours=6),
            train_end=HOUR + timedelta(hours=1),
            validation_end=HOUR + timedelta(hours=2),
            test_end=HOUR + timedelta(hours=3),
        ).collect()
    }
    assert set(rows) == {"train", "validation", "test"}
    assert (rows["train"].split, rows["train"].label, rows["train"].as_of) == (
        "train",
        1,
        HOUR.replace(tzinfo=None),
    )
    assert (rows["validation"].split, rows["validation"].label) == ("validation", 0)
    assert (rows["test"].split, rows["test"].label) == ("test", 1)
