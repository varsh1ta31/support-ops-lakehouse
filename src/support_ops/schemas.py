"""Engine-neutral schemas for source records and operational metadata."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class DataType(StrEnum):
    STRING = "STRING"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"
    INTEGER = "INTEGER"
    DOUBLE = "DOUBLE"
    BOOLEAN = "BOOLEAN"


@dataclass(frozen=True)
class Field:
    name: str
    data_type: DataType
    nullable: bool = True


@dataclass(frozen=True)
class RecordSchema:
    name: str
    fields: tuple[Field, ...]

    def __post_init__(self) -> None:
        names = [field.name for field in self.fields]
        if len(names) != len(set(names)):
            raise ValueError(f"Schema {self.name} contains duplicate field names")

    def field(self, name: str) -> Field:
        try:
            return next(field for field in self.fields if field.name == name)
        except StopIteration as error:
            raise KeyError(f"Unknown field {name!r} in schema {self.name}") from error

    def as_ddl(self) -> str:
        return ", ".join(f"{field.name} {field.data_type.value}" for field in self.fields)


def _fields(*definitions: tuple[str, DataType, bool]) -> tuple[Field, ...]:
    return tuple(Field(*definition) for definition in definitions)


TICKETS = RecordSchema(
    "tickets",
    _fields(
        ("ticket_id", DataType.STRING, False),
        ("customer_id", DataType.STRING, False),
        ("product_id", DataType.STRING, False),
        ("created_at", DataType.TIMESTAMP, False),
        ("closed_at", DataType.TIMESTAMP, True),
        ("priority", DataType.STRING, False),
        ("channel", DataType.STRING, False),
        ("category", DataType.STRING, False),
        ("subject", DataType.STRING, False),
        ("description", DataType.STRING, False),
        ("resolution", DataType.STRING, True),
        ("final_status", DataType.STRING, False),
        ("escalated", DataType.BOOLEAN, False),
        ("sla_breached", DataType.BOOLEAN, False),
        ("agent_id", DataType.STRING, True),
    ),
)

TICKET_EVENTS = RecordSchema(
    "ticket_events",
    _fields(
        ("event_id", DataType.STRING, False),
        ("ticket_id", DataType.STRING, False),
        ("customer_id", DataType.STRING, False),
        ("product_id", DataType.STRING, False),
        ("event_type", DataType.STRING, False),
        ("event_time", DataType.TIMESTAMP, False),
        ("agent_id", DataType.STRING, True),
        ("old_value", DataType.STRING, True),
        ("new_value", DataType.STRING, True),
        ("payload", DataType.STRING, True),
    ),
)

ACCOUNTS = RecordSchema(
    "accounts",
    _fields(
        ("customer_id", DataType.STRING, False),
        ("customer_name", DataType.STRING, False),
        ("segment", DataType.STRING, False),
        ("region", DataType.STRING, False),
        ("industry", DataType.STRING, False),
        ("annual_contract_value", DataType.DOUBLE, False),
        ("support_tier", DataType.STRING, False),
        ("account_status", DataType.STRING, False),
    ),
)

CONTRACTS = RecordSchema(
    "contracts",
    _fields(
        ("contract_id", DataType.STRING, False),
        ("customer_id", DataType.STRING, False),
        ("support_tier", DataType.STRING, False),
        ("priority", DataType.STRING, False),
        ("response_sla_minutes", DataType.INTEGER, False),
        ("resolution_sla_minutes", DataType.INTEGER, False),
        ("effective_from", DataType.DATE, False),
        ("effective_to", DataType.DATE, True),
    ),
)

PRODUCTS = RecordSchema(
    "products",
    _fields(
        ("product_id", DataType.STRING, False),
        ("product_name", DataType.STRING, False),
        ("product_family", DataType.STRING, False),
        ("service_owner", DataType.STRING, False),
        ("criticality", DataType.STRING, False),
    ),
)

INGESTION_METADATA = _fields(
    ("_ingested_at", DataType.TIMESTAMP, False),
    ("_source_file", DataType.STRING, False),
    ("_pipeline_run_id", DataType.STRING, False),
    ("_record_hash", DataType.STRING, False),
)

INVALID_RECORDS = RecordSchema(
    "invalid_records",
    _fields(
        ("record_id", DataType.STRING, False),
        ("source", DataType.STRING, False),
        ("raw_payload", DataType.STRING, False),
        ("failure_reason", DataType.STRING, False),
        ("detected_at", DataType.TIMESTAMP, False),
        ("pipeline_run_id", DataType.STRING, False),
    ),
)

SOURCE_SCHEMAS = {
    schema.name: schema for schema in (TICKETS, TICKET_EVENTS, ACCOUNTS, CONTRACTS, PRODUCTS)
}
