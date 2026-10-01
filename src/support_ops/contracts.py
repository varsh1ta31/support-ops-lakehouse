"""Canonical catalog, schema, and table names."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Layer(StrEnum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    ML = "ml"
    OPS = "ops"


@dataclass(frozen=True, order=True)
class TableRef:
    """A Unity Catalog table identifier without embedded environment assumptions."""

    catalog: str
    layer: Layer
    table: str

    def __post_init__(self) -> None:
        for label, value in (("catalog", self.catalog), ("table", self.table)):
            if not value or not value.replace("_", "").isalnum():
                raise ValueError(f"{label} must contain only letters, numbers, and underscores")

    @property
    def qualified_name(self) -> str:
        return f"{self.catalog}.{self.layer.value}.{self.table}"


BRONZE_TABLES = ("tickets", "ticket_events", "accounts", "contracts", "products")
SILVER_TABLES = ("tickets", "ticket_events", "accounts", "contracts", "products", "ticket_state")
GOLD_TABLES = (
    "support_operations",
    "ticket_features",
    "customer_support_health",
    "incident_signals",
    "ticket_risk_scores",
    "ticket_investigations",
)
OPS_TABLES = (
    "invalid_records",
    "pipeline_runs",
    "quality_metrics",
    "streaming_metrics",
    "silver_evaluations",
    "spark_performance_runs",
)
ML_TABLES = ("training_dataset",)


def table(catalog: str, layer: Layer, name: str) -> TableRef:
    """Build and validate a table reference against the registered project contract."""

    registered = {
        Layer.BRONZE: BRONZE_TABLES,
        Layer.SILVER: SILVER_TABLES,
        Layer.GOLD: GOLD_TABLES,
        Layer.ML: ML_TABLES,
        Layer.OPS: OPS_TABLES,
    }
    if name not in registered[layer]:
        raise ValueError(f"Unknown {layer.value} table: {name}")
    return TableRef(catalog=catalog, layer=layer, table=name)
