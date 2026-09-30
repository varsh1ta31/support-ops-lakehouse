from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from support_ops.quality.validation import Disposition, ValidationContext, validate_record
from support_ops.schemas import INVALID_RECORDS
from support_ops.synthetic.config import load_generation_config
from support_ops.synthetic.generate import DatasetGenerator

NOW = datetime(2026, 1, 1, tzinfo=UTC)
CONTEXT = ValidationContext(frozenset({"c"}), frozenset({"p"}), NOW)
EVENT: dict[str, object] = {
    "event_id": "e",
    "ticket_id": "t",
    "customer_id": "c",
    "product_id": "p",
    "event_type": "customer_replied",
    "event_time": "2025-01-01T00:00:00Z",
}


def test_event_normalization_and_duplicate() -> None:
    raw = {**EVENT, "event_type": " CUSTOMER_REPLIED ", "event_time": "2025-01-01T02:00:00+02:00"}
    result = validate_record("ticket_events", raw, CONTEXT)
    assert result.disposition is Disposition.ACCEPTED
    assert result.record["event_type"] == "customer_replied"
    assert result.record["event_time"] == datetime(2025, 1, 1, tzinfo=UTC)
    assert raw["event_type"] == " CUSTOMER_REPLIED "
    assert (
        validate_record("ticket_events", raw, CONTEXT, processed_ids={"e"}).disposition
        is Disposition.DUPLICATE
    )


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("event_id", " ", "event_id: required"),
        ("customer_id", "unknown", "customer_id: unknown customer"),
        ("product_id", "unknown", "product_id: unknown product"),
        ("event_type", "unknown", "event_type: unsupported value"),
        ("event_time", "bad", "event_time: invalid timestamp"),
        ("event_time", "2025-01-01T00:00:00", "event_time: invalid timestamp"),
        ("event_time", "2026-01-01T00:05:01Z", "event_time: future timestamp outside tolerance"),
        ("payload", "[1]", "payload: invalid JSON object"),
        ("payload", "{", "payload: invalid JSON object"),
        ("ticket_id", 123, "ticket_id: invalid string"),
    ],
)
def test_bad_event(field: str, value: object, reason: str) -> None:
    raw = {**EVENT, field: value, "_source_file": "landing/events/a.json"}
    result = validate_record("ticket_events", raw, CONTEXT, processed_ids={"e"})
    assert result.disposition is Disposition.QUARANTINED
    assert reason in result.reasons
    row = result.quarantine_record(pipeline_run_id="run", detected_at=NOW)
    assert set(row) == {field.name for field in INVALID_RECORDS.fields}
    assert json.loads(str(row["raw_payload"])) == raw
    assert (
        row["record_id"]
        == result.quarantine_record(pipeline_run_id="retry", detected_at=NOW)["record_id"]
    )


def test_priority_change_and_boundary() -> None:
    raw = {
        **EVENT,
        "event_type": "priority_changed",
        "new_value": "p2",
        "payload": "{}",
        "event_time": "2026-01-01T00:05:00Z",
    }
    result = validate_record("ticket_events", raw, CONTEXT)
    assert result.disposition is Disposition.ACCEPTED
    assert result.record["new_value"] == "P2"
    assert (
        "new_value: invalid priority"
        in validate_record("ticket_events", {**raw, "new_value": "urgent"}, CONTEXT).reasons
    )


def generated() -> tuple[dict[str, list[dict[str, object]]], ValidationContext]:
    generator = DatasetGenerator(load_generation_config("config/generation/quality.toml"))
    records: dict[str, list[dict[str, object]]] = {}
    for entity, rows in (
        ("products", generator.product_records()),
        ("accounts", generator.account_records()),
        ("contracts", generator.contract_records()),
        ("tickets", generator.ticket_records()),
        ("invalid", generator.invalid_ticket_records()),
    ):
        records[entity] = [dict(row) for row in rows]
    context = ValidationContext(
        {str(row["customer_id"]) for row in records["accounts"]},
        {str(row["product_id"]) for row in records["products"]},
        NOW,
    )
    return records, context


def test_quality_profile_end_to_end() -> None:
    records, context = generated()
    for entity in ("products", "accounts", "contracts", "tickets"):
        seen: set[str] = set()
        from support_ops.quality.validation import KEYS

        for raw in records[entity]:
            # Match CSV/Bronze string values, including blank optional fields.
            bronze = {name: "" if value is None else str(value) for name, value in raw.items()}
            result = validate_record(entity, bronze, context, processed_ids=seen)
            assert result.disposition is Disposition.ACCEPTED, result.reasons
            key = str(result.record[KEYS[entity]])
            seen.add(key)
            assert (
                validate_record(entity, bronze, context, processed_ids=seen).disposition
                is Disposition.DUPLICATE
            )
    assert len(records["invalid"]) == 8
    assert all(
        validate_record("tickets", row, context).disposition is Disposition.QUARANTINED
        for row in records["invalid"]
    )


@pytest.mark.parametrize(
    ("entity", "field", "value", "reason"),
    [
        ("accounts", "annual_contract_value", "NaN", "invalid double"),
        ("accounts", "annual_contract_value", "inf", "invalid double"),
        ("accounts", "annual_contract_value", "-1", "outside allowed range"),
        ("accounts", "support_tier", "bad", "unsupported value"),
        ("contracts", "resolution_sla_minutes", "0", "outside allowed range"),
        ("contracts", "response_sla_minutes", "-1", "outside allowed range"),
        ("contracts", "response_sla_minutes", "1.5", "invalid integer"),
        ("contracts", "response_sla_minutes", str(2**31), "invalid integer"),
        ("contracts", "effective_from", "bad", "invalid date"),
        ("contracts", "effective_to", "1900-01-01", "before effective_from"),
        ("tickets", "closed_at", "1900-01-01T00:00:00Z", "before created_at"),
        ("tickets", "escalated", "1", "invalid boolean"),
    ],
)
def test_entity_rules(entity: str, field: str, value: object, reason: str) -> None:
    records, context = generated()
    result = validate_record(entity, {**records[entity][0], field: value}, context)
    assert f"{field}: {reason}" in result.reasons


def test_canonical_account_values_and_zero_contract_value() -> None:
    records, context = generated()
    result = validate_record(
        "accounts",
        {
            **records["accounts"][0],
            "segment": " enterprise ",
            "region": "europe",
            "support_tier": "premium",
            "annual_contract_value": "0",
        },
        context,
    )
    assert result.disposition is Disposition.ACCEPTED
    assert result.record["segment"] == "Enterprise"
    assert result.record["region"] == "Europe"
    assert result.record["support_tier"] == "Premium"


def test_invalid_caller_arguments() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        ValidationContext(set(), set(), datetime(2026, 1, 1))
    with pytest.raises(ValueError, match="non-negative"):
        ValidationContext(set(), set(), NOW, timedelta(seconds=-1))
    with pytest.raises(KeyError):
        validate_record("unknown", {}, CONTEXT)
    with pytest.raises(ValueError, match="Only quarantined"):
        validate_record("ticket_events", EVENT, CONTEXT).quarantine_record(
            pipeline_run_id="r", detected_at=NOW
        )
    result = validate_record("ticket_events", {}, CONTEXT)
    with pytest.raises(ValueError, match="must not be empty"):
        result.quarantine_record(pipeline_run_id=" ", detected_at=NOW)
    with pytest.raises(ValueError, match="timezone-aware"):
        result.quarantine_record(pipeline_run_id="r", detected_at=datetime(2026, 1, 1))


def test_status_and_creation_rules() -> None:
    created = {**EVENT, "event_type": "ticket_created", "new_value": "p1"}
    assert validate_record("ticket_events", created, CONTEXT).record["new_value"] == "P1"
    assert "new_value: invalid priority" in (
        validate_record("ticket_events", {**created, "new_value": None}, CONTEXT).reasons
    )
    changed = {**EVENT, "event_type": "status_changed", "new_value": " Pending_Customer "}
    assert validate_record("ticket_events", changed, CONTEXT).record["new_value"] == (
        "pending_customer"
    )
    assert "new_value: invalid status" in (
        validate_record("ticket_events", {**changed, "new_value": "limbo"}, CONTEXT).reasons
    )
    records, context = generated()
    ticket = {**records["tickets"][0], "final_status": "RESOLVED"}
    assert validate_record("tickets", ticket, context).record["final_status"] == "resolved"
    assert "final_status: unsupported value" in (
        validate_record("tickets", {**ticket, "final_status": "limbo"}, context).reasons
    )
