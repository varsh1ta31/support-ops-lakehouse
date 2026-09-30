"""Validate Bronze records without discarding raw evidence or mutating input."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Set
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum

from support_ops.schemas import SOURCE_SCHEMAS, DataType

EVENT_TYPES = frozenset(
    [
        "ticket_created",
        "agent_assigned",
        "priority_changed",
        "status_changed",
        "customer_replied",
        "agent_replied",
        "engineering_escalated",
        "ticket_resolved",
        "ticket_reopened",
    ]
)
ACTIVE_STATUSES = frozenset(["open", "in_progress", "pending_customer"])
STATUSES = ACTIVE_STATUSES | {"resolved", "closed"}
KEYS = {
    "tickets": "ticket_id",
    "ticket_events": "event_id",
    "accounts": "customer_id",
    "contracts": "contract_id",
    "products": "product_id",
}
ENUMS = {
    "priority": ("P1", "P2", "P3", "P4"),
    "support_tier": ("Standard", "Enhanced", "Premium"),
    "segment": ("SMB", "Mid-Market", "Enterprise", "Strategic"),
    "region": ("North America", "Europe", "Asia Pacific", "Latin America"),
}


class Disposition(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "rejected_duplicate"
    QUARANTINED = "quarantined"


@dataclass(frozen=True)
class ValidationContext:
    """Reference IDs and clock are explicit, reproducible inputs; no hidden lookups."""

    customer_ids: Set[str]
    product_ids: Set[str]
    now: datetime
    future_tolerance: timedelta = timedelta(minutes=5)

    def __post_init__(self) -> None:
        if self.now.tzinfo is None or self.now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        if self.future_tolerance < timedelta(0):
            raise ValueError("future_tolerance must be non-negative")


@dataclass(frozen=True)
class ValidationResult:
    entity: str
    disposition: Disposition
    record: Mapping[str, object]
    reasons: tuple[str, ...]
    raw_payload: str

    def quarantine_record(
        self, *, pipeline_run_id: str, detected_at: datetime
    ) -> dict[str, object]:
        """Build the canonical ops.invalid_records row; persistence belongs to the writer."""
        if self.disposition is not Disposition.QUARANTINED:
            raise ValueError("Only quarantined results have a quarantine record")
        if not pipeline_run_id.strip():
            raise ValueError("pipeline_run_id must not be empty")
        if detected_at.tzinfo is None or detected_at.utcoffset() is None:
            raise ValueError("detected_at must be timezone-aware")
        digest = hashlib.sha256(f"{self.entity}\n{self.raw_payload}".encode()).hexdigest()
        return {
            "record_id": digest,
            "source": self.entity,
            "raw_payload": self.raw_payload,
            "failure_reason": "; ".join(self.reasons),
            "detected_at": detected_at.astimezone(UTC),
            "pipeline_run_id": pipeline_run_id,
        }


def _cast(value: object, kind: DataType) -> object:
    if kind is DataType.STRING:
        if not isinstance(value, str):
            raise ValueError("expected string")
        return value.strip()
    if kind is DataType.TIMESTAMP:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("timestamp requires an explicit timezone")
        return parsed.astimezone(UTC)
    if kind is DataType.DATE:
        return date.fromisoformat(str(value))
    if kind is DataType.BOOLEAN:
        if str(value).lower() not in ("true", "false"):
            raise ValueError("expected true or false")
        return str(value).lower() == "true"
    if kind is DataType.INTEGER:
        parsed_int = int(str(value))
        if not -(2**31) <= parsed_int < 2**31:
            raise ValueError("integer outside Spark INT range")
        return parsed_int
    parsed_float = float(str(value))
    if not math.isfinite(parsed_float):
        raise ValueError("expected finite number")
    return parsed_float


def validate_record(
    entity: str,
    raw: Mapping[str, object],
    context: ValidationContext,
    *,
    processed_ids: Set[str] = frozenset(),
) -> ValidationResult:
    """Normalize and validate one record. Callers persist accepted IDs for replay checks.

    Validation precedes duplicate rejection so changed, malformed payloads remain inspectable.
    Reference checks are mandatory for contracts, tickets, and events. Timestamp strings must
    include an offset. Lateness and ticket transitions belong to subsequent state processing.
    An unparseable source line arrives with its raw text in ``_corrupt_record``.
    """
    schema = SOURCE_SCHEMAS[entity]
    payload = json.dumps(dict(raw), sort_keys=True, separators=(",", ":"), default=str)
    record: dict[str, object] = {}
    reasons: list[str] = []
    if raw.get("_corrupt_record") is not None:
        reasons.append("record: malformed source line")
    for field in schema.fields:
        value = raw.get(field.name)
        if isinstance(value, str):
            value = value.strip()
        if value is None or value == "":
            record[field.name] = None
            if not field.nullable:
                reasons.append(f"{field.name}: required")
            continue
        try:
            record[field.name] = _cast(value, field.data_type)
        except (ValueError, TypeError, OverflowError):
            record[field.name] = None
            reasons.append(f"{field.name}: invalid {field.data_type.value.lower()}")

    for name, choices in ENUMS.items():
        value = record.get(name)
        if isinstance(value, str):
            canonical = next(
                (item for item in choices if item.casefold() == value.casefold()), None
            )
            if canonical is None:
                reasons.append(f"{name}: unsupported value")
            else:
                record[name] = canonical

    if (
        entity in ("tickets", "ticket_events", "contracts")
        and record.get("customer_id") not in context.customer_ids
    ):
        reasons.append("customer_id: unknown customer")
    if (
        entity in ("tickets", "ticket_events")
        and record.get("product_id") not in context.product_ids
    ):
        reasons.append("product_id: unknown product")
    if entity == "tickets" and isinstance(record.get("final_status"), str):
        record["final_status"] = str(record["final_status"]).lower()
        if record["final_status"] not in STATUSES:
            reasons.append("final_status: unsupported value")
    if entity == "ticket_events":
        event_type = record.get("event_type")
        if isinstance(event_type, str):
            record["event_type"] = event_type.lower()
            if record["event_type"] not in EVENT_TYPES:
                reasons.append("event_type: unsupported value")
        if record.get("payload") is not None:
            try:
                parsed_payload = json.loads(str(record["payload"]))
                if not isinstance(parsed_payload, dict):
                    raise ValueError("expected JSON object")
            except ValueError:
                reasons.append("payload: invalid JSON object")
        if record.get("event_type") in ("priority_changed", "ticket_created"):
            priority = str(record.get("new_value")).upper()
            if priority not in ENUMS["priority"]:
                reasons.append("new_value: invalid priority")
            else:
                record["new_value"] = priority
        if record.get("event_type") == "status_changed":
            status = str(record.get("new_value")).lower()
            if status not in STATUSES:
                reasons.append("new_value: invalid status")
            else:
                record["new_value"] = status

    for name in ("annual_contract_value", "response_sla_minutes", "resolution_sla_minutes"):
        number = record.get(name)
        if isinstance(number, (int, float)) and (
            number < 0 if name == "annual_contract_value" else number <= 0
        ):
            reasons.append(f"{name}: outside allowed range")
    for start, end in (("created_at", "closed_at"), ("effective_from", "effective_to")):
        first, last = record.get(start), record.get(end)
        if isinstance(first, date) and isinstance(last, date) and last < first:
            reasons.append(f"{end}: before {start}")
    for name in ("created_at", "closed_at", "event_time"):
        timestamp = record.get(name)
        if isinstance(timestamp, datetime) and timestamp > context.now + context.future_tolerance:
            reasons.append(f"{name}: future timestamp outside tolerance")

    disposition = Disposition.ACCEPTED
    if reasons:
        disposition = Disposition.QUARANTINED
    elif record[KEYS[entity]] in processed_ids:
        disposition = Disposition.DUPLICATE
    return ValidationResult(entity, disposition, record, tuple(reasons), payload)
