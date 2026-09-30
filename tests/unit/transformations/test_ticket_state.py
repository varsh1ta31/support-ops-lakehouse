from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from support_ops.transformations.ticket_state import (
    Contract,
    TicketEvent,
    TicketState,
    apply_event,
    historical_state,
    order_events,
    reconstruct,
    reconstruct_row,
    select_contract,
    sla_fields,
    sla_row,
)

T0 = datetime(2026, 1, 1, tzinfo=UTC)
TICKET: dict[str, object] = {
    "ticket_id": "t",
    "customer_id": "c",
    "product_id": "p",
    "created_at": T0,
    "closed_at": None,
    "priority": "P3",
    "final_status": "open",
    "escalated": False,
    "agent_id": None,
}


def event(minute: int, event_type: str, **kwargs: str | None) -> TicketEvent:
    return TicketEvent(
        event_id=str(kwargs.pop("event_id", f"e{minute:03d}")),
        ticket_id="t",
        event_type=event_type,
        event_time=T0 + timedelta(minutes=minute),
        customer_id="c",
        product_id="p",
        **kwargs,
    )


def at(minute: int) -> datetime:
    return T0 + timedelta(minutes=minute)


def open_state(**changes: object) -> TicketState:
    state = historical_state(TICKET, at(0))
    assert state is not None
    return TicketState(**{**state.__dict__, **changes})


def test_known_sequence_produces_expected_current_state() -> None:
    events = [
        event(1, "agent_assigned", agent_id="a1"),
        event(2, "agent_replied"),
        event(3, "status_changed", new_value="pending_customer"),
        event(4, "customer_replied"),
        event(5, "priority_changed", new_value="P1"),
        event(6, "engineering_escalated"),
        event(7, "ticket_resolved"),
        event(8, "ticket_reopened"),
        event(9, "ticket_resolved"),
        event(10, "status_changed", new_value="closed"),
    ]
    state = reconstruct(TICKET, reversed(events), at(60))
    assert state is not None
    row = state.as_row(at(60))
    assert row == {
        **row,
        "status": "closed",
        "priority": "P1",
        "assigned_agent": "a1",
        "resolved_at": at(9),
        "last_updated_at": at(10),
        "message_count": 2,
        "reopen_count": 1,
        "escalation_count": 1,
        "applied_event_count": 10,
        "rejected_event_count": 0,
        "minutes_open": 9,
        "state_as_of": at(60),
    }


def test_point_in_time_state_excludes_future_events() -> None:
    events = [event(1, "ticket_resolved"), event(5, "ticket_reopened")]
    state = reconstruct(TICKET, events, at(3))
    assert state is not None
    assert (state.status, state.reopen_count) == ("resolved", 0)
    assert reconstruct(TICKET, events, at(-1)) is None


@pytest.mark.parametrize(
    ("status", "change"),
    [
        ("resolved", event(1, "agent_assigned", agent_id="a")),
        ("open", event(1, "agent_assigned")),
        ("resolved", event(1, "priority_changed", new_value="P1")),
        ("open", event(1, "status_changed", new_value="closed")),
        ("closed", event(1, "status_changed", new_value="open")),
        ("resolved", event(1, "engineering_escalated")),
        ("resolved", event(1, "ticket_resolved")),
        ("open", event(1, "ticket_reopened")),
        ("open", event(1, "ticket_created", new_value="P1")),
        ("open", event(-1, "agent_replied")),
    ],
)
def test_invalid_transitions_are_counted_not_applied(status: str, change: TicketEvent) -> None:
    state = open_state(status=status)
    result = apply_event(state, change)
    assert result == TicketState(**{**state.__dict__, "rejected_event_count": 1})


def test_replies_count_in_any_status_and_customer_reply_reopens_waiting_ticket() -> None:
    state = apply_event(open_state(status="resolved"), event(1, "customer_replied"))
    assert state is not None
    assert (state.status, state.message_count) == ("resolved", 1)
    state = apply_event(open_state(status="pending_customer"), event(1, "customer_replied"))
    assert state is not None and state.status == "open"


def test_event_only_ticket_requires_valid_creation() -> None:
    created = event(0, "ticket_created", new_value="P2", agent_id="a")
    state = reconstruct(None, [event(1, "agent_replied"), created], at(5))
    assert state is not None
    assert (state.ticket_id, state.priority, state.message_count) == ("t", "P2", 1)
    assert reconstruct(None, [event(1, "agent_replied")], at(5)) is None
    assert reconstruct(None, [event(0, "ticket_created", new_value="bad")], at(5)) is None


def test_events_reflected_in_historical_snapshot_are_skipped() -> None:
    resolved = {**TICKET, "closed_at": at(30), "final_status": "resolved", "escalated": True}
    events = [event(0, "ticket_created", new_value="P3"), event(10, "agent_replied")]
    state = reconstruct(resolved, [*events, event(40, "ticket_reopened")], at(50))
    assert state is not None
    assert (state.status, state.message_count, state.reopen_count) == ("open", 0, 1)
    assert state.escalation_count == 1
    assert state.as_row(at(50))["minutes_open"] == 50


def test_historical_snapshot_before_close_does_not_leak_outcome() -> None:
    resolved = {**TICKET, "closed_at": at(30), "final_status": "resolved", "escalated": True}
    state = historical_state(resolved, at(10))
    assert state is not None
    assert (state.status, state.resolved_at, state.escalation_count) == ("open", None, 0)
    assert historical_state({**TICKET, "escalated": True}, at(10)) == open_state(escalation_count=1)


def test_ordering_is_deterministic_and_deduplicates() -> None:
    first, second = event(1, "agent_replied", event_id="b"), event(1, "agent_replied", event_id="a")
    assert order_events([first, second, first]) == [second, first]


def test_naive_timestamps_are_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        reconstruct(TICKET, [], datetime(2026, 1, 1))
    with pytest.raises(ValueError, match="timezone-aware"):
        historical_state({**TICKET, "created_at": "2026-01-01"}, at(1))


def micros(value: datetime) -> int:
    return int((value - datetime(1970, 1, 1, tzinfo=UTC)).total_seconds()) * 1_000_000


def test_udf_row_adapter_round_trips_utc_microseconds() -> None:
    ticket = {**TICKET, "created_at": micros(at(0)), "closed_at": micros(at(30))}
    ticket["final_status"] = "resolved"
    reopen = {**event(40, "ticket_reopened").__dict__, "event_time": micros(at(40))}
    row = reconstruct_row(ticket, [reopen], micros(at(45)) + 7)
    assert row is not None
    assert row["created_at"] == micros(at(0))
    assert row["last_updated_at"] == micros(at(40))
    assert row["resolved_at"] is None
    assert row["state_as_of"] == micros(at(45)) + 7
    assert (row["status"], row["reopen_count"], row["minutes_open"]) == ("open", 1, 45)
    assert reconstruct_row(None, None, micros(at(45))) is None


def contract(contract_id: str = "k", **changes: object) -> Contract:
    fields: dict[str, object] = {
        "contract_id": contract_id,
        "customer_id": "c",
        "support_tier": "Premium",
        "priority": "P3",
        "resolution_sla_minutes": 60,
        "effective_from": date(2025, 1, 1),
        "effective_to": None,
        **changes,
    }
    return Contract(**fields)  # type: ignore[arg-type]


def test_contract_effective_dates_are_inclusive_utc_days() -> None:
    single_day = contract(effective_from=date(2026, 1, 1), effective_to=date(2026, 1, 1))
    assert single_day.active_at(T0)
    assert single_day.active_at(T0 + timedelta(hours=23, minutes=59))
    assert not single_day.active_at(T0 + timedelta(days=1))
    assert not single_day.active_at(T0 - timedelta(microseconds=1))
    # 2026-01-01 01:00 at UTC+05:00 is 2025-12-31 in UTC.
    assert not single_day.active_at(datetime.fromisoformat("2026-01-01T01:00:00+05:00"))


def test_select_contract_matches_customer_priority_and_time() -> None:
    contracts = [
        contract("other-customer", customer_id="x"),
        contract("other-priority", priority="P1"),
        contract("expired", effective_to=date(2025, 12, 31)),
        contract("future", effective_from=date(2026, 1, 2)),
        contract("base"),
    ]
    chosen = select_contract(contracts, customer_id="c", priority="P3", at=T0)
    assert chosen is not None and chosen.contract_id == "base"
    assert select_contract(contracts, customer_id="c", priority="P2", at=T0) is None
    assert select_contract([], customer_id="c", priority="P3", at=T0) is None


def test_overlapping_contracts_resolve_deterministically() -> None:
    renewal = contract("a-renewal", effective_from=date(2025, 6, 1))
    contracts = [
        contract("z-base"),
        renewal,
        contract("b-renewal", effective_from=date(2025, 6, 1)),
    ]
    for ordering in (contracts, list(reversed(contracts))):
        chosen = select_contract(ordering, customer_id="c", priority="P3", at=T0)
        assert chosen is not None and chosen.contract_id == "b-renewal"


def test_sla_fields_for_open_ticket_age_with_as_of() -> None:
    state = open_state()
    fields = sla_fields(state, [contract()], at(45))
    assert fields == {
        "contract_id": "k",
        "support_tier": "Premium",
        "resolution_sla_minutes": 60,
        "sla_deadline": at(60),
        "minutes_to_sla": 15,
    }
    assert sla_fields(state, [contract()], at(60))["minutes_to_sla"] == 0
    assert sla_fields(state, [contract()], at(60) + timedelta(seconds=1))["minutes_to_sla"] == -1
    assert sla_fields(state, [contract()], at(200))["minutes_to_sla"] == -140


def test_sla_fields_for_resolved_ticket_measure_to_resolution() -> None:
    state = open_state(status="resolved", resolved_at=at(50))
    assert sla_fields(state, [contract()], at(500))["minutes_to_sla"] == 10
    late = open_state(status="resolved", resolved_at=at(90))
    assert sla_fields(late, [contract()], at(500))["minutes_to_sla"] == -30


def test_priority_change_retargets_deadline_from_creation() -> None:
    contracts = [contract(), contract("urgent", priority="P1", resolution_sla_minutes=15)]
    state = reconstruct(TICKET, [event(10, "priority_changed", new_value="P1")], at(20))
    assert state is not None
    fields = sla_fields(state, contracts, at(20))
    assert (fields["contract_id"], fields["sla_deadline"]) == ("urgent", at(15))
    assert fields["minutes_to_sla"] == -5


def test_contract_is_chosen_at_creation_not_evaluation_time() -> None:
    contracts = [
        contract("old", effective_to=date(2026, 1, 1)),
        contract("new", effective_from=date(2026, 1, 2), resolution_sla_minutes=30),
    ]
    state = open_state()
    assert sla_fields(state, contracts, at(3 * 24 * 60))["contract_id"] == "old"
    assert sla_fields(open_state(created_at=at(24 * 60)), contracts, at(0))["contract_id"] == (
        "new"
    )


def test_missing_contract_leaves_sla_fields_null() -> None:
    fields = sla_fields(open_state(), [contract(priority="P1")], at(10))
    assert fields == dict.fromkeys(
        ("contract_id", "support_tier", "resolution_sla_minutes", "sla_deadline", "minutes_to_sla")
    )


def test_sla_udf_adapter_uses_utc_microseconds() -> None:
    row = reconstruct_row({**TICKET, "created_at": micros(at(0))}, None, micros(at(45)))
    assert row is not None
    raw_contract = {**contract().__dict__, "unused": "ignored"}
    fields = sla_row(row, [raw_contract], micros(at(45)))
    assert fields["sla_deadline"] == micros(at(60))
    assert fields["minutes_to_sla"] == 15
    assert sla_row(row, None, micros(at(45)))["contract_id"] is None
