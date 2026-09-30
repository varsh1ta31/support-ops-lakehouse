"""Engine-neutral ticket-state reconstruction from historical tickets plus ordered events."""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, replace
from datetime import UTC, date, datetime, timedelta
from typing import Any

from support_ops.quality.validation import ACTIVE_STATUSES, ENUMS


@dataclass(frozen=True)
class TicketEvent:
    event_id: str
    ticket_id: str
    event_type: str
    event_time: datetime
    agent_id: str | None = None
    new_value: str | None = None
    customer_id: str | None = None
    product_id: str | None = None


@dataclass(frozen=True)
class TicketState:
    ticket_id: str
    customer_id: str
    product_id: str
    status: str
    priority: str
    assigned_agent: str | None
    created_at: datetime
    last_updated_at: datetime
    resolved_at: datetime | None
    message_count: int = 0
    reopen_count: int = 0
    escalation_count: int = 0
    applied_event_count: int = 0
    rejected_event_count: int = 0

    def as_row(self, as_of: datetime) -> dict[str, object]:
        """Add as-of-dependent measures; open tickets age with the evaluation time."""
        end = self.resolved_at or as_of
        return {
            **asdict(self),
            "minutes_open": max(0, int((end - self.created_at).total_seconds() // 60)),
            "state_as_of": as_of,
        }


@dataclass(frozen=True)
class Contract:
    contract_id: str
    customer_id: str
    support_tier: str
    priority: str
    resolution_sla_minutes: int
    effective_from: date
    effective_to: date | None = None

    def active_at(self, moment: datetime) -> bool:
        """Effective dates are whole UTC days; both bounds are inclusive."""
        day = _aware(moment).astimezone(UTC).date()
        return self.effective_from <= day and (
            self.effective_to is None or day <= self.effective_to
        )


def select_contract(
    contracts: Iterable[Contract], *, customer_id: str, priority: str, at: datetime
) -> Contract | None:
    """Pick the customer's contract for ``priority`` in force at ``at``.

    Overlapping contracts resolve to the latest ``effective_from`` (a renewal supersedes its
    predecessor), then the highest ``contract_id`` as a deterministic tie-breaker.
    """
    candidates = [
        contract
        for contract in contracts
        if contract.customer_id == customer_id
        and contract.priority == priority
        and contract.active_at(at)
    ]
    return max(candidates, key=lambda c: (c.effective_from, c.contract_id), default=None)


def sla_fields(
    state: TicketState, contracts: Iterable[Contract], as_of: datetime
) -> dict[str, object]:
    """Resolution-SLA measures for ``state`` evaluated at ``as_of``.

    The SLA clock starts at ticket creation, so the terms are those of the contract in force at
    ``created_at`` for the ticket's current priority. A priority change re-targets the deadline
    without restarting the clock. ``minutes_to_sla`` is measured to resolution (or ``as_of``
    while unresolved) and is negative exactly when the deadline was missed.
    """
    contract = select_contract(
        contracts, customer_id=state.customer_id, priority=state.priority, at=state.created_at
    )
    if contract is None:
        return dict.fromkeys(name for name, _ in SLA_COLUMNS)
    deadline = state.created_at + timedelta(minutes=contract.resolution_sla_minutes)
    remaining = deadline - (state.resolved_at or _aware(as_of))
    return {
        "contract_id": contract.contract_id,
        "support_tier": contract.support_tier,
        "resolution_sla_minutes": contract.resolution_sla_minutes,
        "sla_deadline": deadline,
        "minutes_to_sla": int(remaining.total_seconds() // 60),
    }


def historical_state(ticket: Mapping[str, object], as_of: datetime) -> TicketState | None:
    """Project a validated Silver ticket to its state at ``as_of``; None before creation.

    A historical row is a snapshot at its close time (or creation time when still open). A
    resolved ticket evaluated before ``closed_at`` is therefore treated as still open.
    """
    created_at = _aware(ticket["created_at"])
    if created_at > as_of:
        return None
    closed_at = ticket.get("closed_at")
    resolved_at = _aware(closed_at) if closed_at is not None else None
    if resolved_at is not None and resolved_at > as_of:
        resolved_at = None
    final_status = str(ticket["final_status"])
    active = final_status in ACTIVE_STATUSES
    status = final_status if active or resolved_at is not None else "open"
    return TicketState(
        ticket_id=str(ticket["ticket_id"]),
        customer_id=str(ticket["customer_id"]),
        product_id=str(ticket["product_id"]),
        status=status,
        priority=str(ticket["priority"]),
        assigned_agent=None if ticket.get("agent_id") is None else str(ticket["agent_id"]),
        created_at=created_at,
        last_updated_at=resolved_at or created_at,
        resolved_at=resolved_at,
        # Escalation time is unknown: count it only once the whole snapshot is visible.
        escalation_count=int(
            bool(ticket.get("escalated")) and (closed_at is None or resolved_at is not None)
        ),
    )


def order_events(events: Iterable[TicketEvent]) -> list[TicketEvent]:
    """Order by event time with event ID as a deterministic tie-breaker; drop repeated IDs."""
    ordered: list[TicketEvent] = []
    seen: set[str] = set()
    for event in sorted(events, key=_order_key):
        if event.event_id not in seen:
            seen.add(event.event_id)
            ordered.append(event)
    return ordered


def apply_event(state: TicketState | None, event: TicketEvent) -> TicketState | None:
    """Apply one event; an invalid transition leaves the state unchanged but is counted."""
    if state is None:
        return _create(event)
    if event.event_type == "ticket_created" or event.event_time < state.created_at:
        return _reject(state)
    active = state.status in ACTIVE_STATUSES
    changes: dict[str, Any]
    match event.event_type:
        case "agent_assigned" if active and (event.agent_id or event.new_value):
            changes = {"assigned_agent": event.agent_id or event.new_value}
        case "priority_changed" if active:
            changes = {"priority": event.new_value}
        case "status_changed" if (active and event.new_value in ACTIVE_STATUSES) or (
            state.status == "resolved" and event.new_value == "closed"
        ):
            changes = {"status": event.new_value}
        case "customer_replied":
            changes = {"message_count": state.message_count + 1}
            if state.status == "pending_customer":
                changes["status"] = "open"
        case "agent_replied":
            changes = {"message_count": state.message_count + 1}
        case "engineering_escalated" if active:
            changes = {"escalation_count": state.escalation_count + 1}
        case "ticket_resolved" if active:
            changes = {"status": "resolved", "resolved_at": event.event_time}
        case "ticket_reopened" if not active:
            changes = {
                "status": "open",
                "resolved_at": None,
                "reopen_count": state.reopen_count + 1,
            }
        case _:
            return _reject(state)
    return replace(
        state,
        **changes,
        last_updated_at=max(state.last_updated_at, event.event_time),
        applied_event_count=state.applied_event_count + 1,
    )


def reconstruct(
    ticket: Mapping[str, object] | None,
    events: Iterable[TicketEvent],
    as_of: datetime,
) -> TicketState | None:
    """Rebuild one ticket's state at ``as_of`` from its full history.

    Events at or before the historical snapshot are already reflected in it and are skipped.
    Events after ``as_of`` are excluded, so the same function yields point-in-time state.
    Recomputing from full history places late-arriving events in event-time order.
    """
    _aware(as_of)
    state = None if ticket is None else historical_state(ticket, as_of)
    if ticket is not None and state is None:
        return None
    snapshot = None if state is None else state.last_updated_at
    for event in order_events(events):
        if event.event_time > as_of:
            break
        if snapshot is None or event.event_time > snapshot:
            state = apply_event(state, event)
    return state


def _create(event: TicketEvent) -> TicketState | None:
    if (
        event.event_type != "ticket_created"
        or event.new_value not in ENUMS["priority"]
        or event.customer_id is None
        or event.product_id is None
    ):
        return None
    return TicketState(
        ticket_id=event.ticket_id,
        customer_id=event.customer_id,
        product_id=event.product_id,
        status="open",
        priority=str(event.new_value),
        assigned_agent=event.agent_id,
        created_at=event.event_time,
        last_updated_at=event.event_time,
        resolved_at=None,
        applied_event_count=1,
    )


def _reject(state: TicketState) -> TicketState:
    return replace(state, rejected_event_count=state.rejected_event_count + 1)


def _order_key(event: TicketEvent) -> tuple[datetime, str]:
    return (event.event_time, event.event_id)


def _aware(value: object) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamps must be timezone-aware datetimes")
    return value


STATE_COLUMNS = (
    ("ticket_id", "STRING"),
    ("customer_id", "STRING"),
    ("product_id", "STRING"),
    ("status", "STRING"),
    ("priority", "STRING"),
    ("assigned_agent", "STRING"),
    ("created_at", "TIMESTAMP"),
    ("last_updated_at", "TIMESTAMP"),
    ("resolved_at", "TIMESTAMP"),
    ("message_count", "INT"),
    ("reopen_count", "INT"),
    ("escalation_count", "INT"),
    ("applied_event_count", "INT"),
    ("rejected_event_count", "INT"),
    ("minutes_open", "BIGINT"),
    ("state_as_of", "TIMESTAMP"),
)
SLA_COLUMNS = (
    ("contract_id", "STRING"),
    ("support_tier", "STRING"),
    ("resolution_sla_minutes", "INT"),
    ("sla_deadline", "TIMESTAMP"),
    ("minutes_to_sla", "BIGINT"),
)
TABLE_COLUMNS = STATE_COLUMNS + SLA_COLUMNS
CONTRACT_FIELDS = tuple(Contract.__dataclass_fields__)
TICKET_TIMESTAMPS = ("created_at", "closed_at")
STATE_TIMESTAMPS = ("created_at", "last_updated_at", "resolved_at")


def reconstruct_row(
    ticket: Mapping[str, object] | None,
    events: Iterable[Mapping[str, object]] | None,
    as_of_micros: int,
) -> dict[str, object] | None:
    """Executor-side adapter. Timestamps cross the UDF boundary as UTC epoch microseconds.

    PySpark converts TIMESTAMP values to naive local-time datetimes, so integers keep the UTC
    contract independent of the worker's time zone.
    """
    as_of = _epoch(as_of_micros)
    history = None
    if ticket is not None:
        history = {
            **ticket,
            **{name: _from_micros(ticket.get(name)) for name in TICKET_TIMESTAMPS},
        }
    parsed = [
        TicketEvent(**{**event, "event_time": _from_micros(event["event_time"])})  # type: ignore[arg-type]
        for event in events or ()
    ]
    state = reconstruct(history, parsed, as_of)
    if state is None:
        return None
    row = state.as_row(as_of)
    for name, kind in STATE_COLUMNS:
        value = row[name]
        if kind == "TIMESTAMP" and isinstance(value, datetime):
            row[name] = _to_micros(value)
    return row


def sla_row(
    state: Mapping[str, object],
    contracts: Iterable[Mapping[str, object]] | None,
    as_of_micros: int,
) -> dict[str, object]:
    """Executor-side adapter for a ``reconstruct_row`` result and the customer's contracts.

    Timestamps are UTC epoch microseconds as in ``reconstruct_row``; DATE values arrive as
    ``datetime.date`` and need no time-zone handling.
    """
    ticket = TicketState(
        **{  # type: ignore[arg-type]
            **{name: state[name] for name in TicketState.__dataclass_fields__},
            **{name: _from_micros(state[name]) for name in STATE_TIMESTAMPS},
        }
    )
    parsed = [
        Contract(**{name: contract[name] for name in CONTRACT_FIELDS})  # type: ignore[arg-type]
        for contract in contracts or ()
    ]
    fields = sla_fields(ticket, parsed, _epoch(as_of_micros))
    deadline = fields["sla_deadline"]
    if isinstance(deadline, datetime):
        fields["sla_deadline"] = _to_micros(deadline)
    return fields


def ticket_state_frame(spark: Any, *, catalog: str, as_of: datetime) -> Any:
    """Reconstruct a lazy point-in-time frame without changing the current-state table."""
    from pyspark.sql import functions as f

    from support_ops.transformations.silver import create_tables, qualified

    _aware(as_of)
    for entity in ("tickets", "ticket_events", "contracts"):
        create_tables(spark, catalog, entity)
    micros = {name: f.unix_micros(f.col(name)).alias(name) for name in TICKET_TIMESTAMPS}
    tickets = spark.table(qualified(catalog, "silver", "tickets")).select(
        "ticket_id",
        f.struct(
            *(
                micros.get(name, f.col(name))
                for name in (
                    "ticket_id",
                    "customer_id",
                    "product_id",
                    "created_at",
                    "closed_at",
                    "priority",
                    "final_status",
                    "escalated",
                    "agent_id",
                )
            )
        ).alias("_ticket"),
    )
    events = (
        spark.table(qualified(catalog, "silver", "ticket_events"))
        .filter(f.col("event_time") <= f.lit(as_of))
        .groupBy("ticket_id")
        .agg(
            f.collect_list(
                f.struct(
                    "event_id",
                    "ticket_id",
                    "event_type",
                    f.unix_micros(f.col("event_time")).alias("event_time"),
                    "agent_id",
                    "new_value",
                    "customer_id",
                    "product_id",
                )
            ).alias("_events")
        )
    )
    # Contracts join on the reconstructed customer, which event-only tickets learn in the fold.
    contracts = (
        spark.table(qualified(catalog, "silver", "contracts"))
        .groupBy(f.col("customer_id").alias("_contract_customer"))
        .agg(f.collect_list(f.struct(*CONTRACT_FIELDS)).alias("_contracts"))
    )

    def evaluate(ticket: Any, ticket_events: Any) -> dict[str, object] | None:
        return reconstruct_row(
            None if ticket is None else ticket.asDict(),
            None if ticket_events is None else [event.asDict() for event in ticket_events],
            _to_micros(as_of),
        )

    def evaluate_sla(ticket: Any, customer_contracts: Any) -> dict[str, object]:
        return sla_row(
            ticket.asDict(),
            None if customer_contracts is None else [row.asDict() for row in customer_contracts],
            _to_micros(as_of),
        )

    state = f.udf(evaluate, _micros_ddl(STATE_COLUMNS), useArrow=False)
    sla = f.udf(evaluate_sla, _micros_ddl(SLA_COLUMNS), useArrow=False)
    frame = (
        tickets.join(events, "ticket_id", "full_outer")
        .select(state(f.col("_ticket"), f.col("_events")).alias("_state"))
        .filter(f.col("_state").isNotNull())
        .join(contracts, f.col("_state.customer_id") == f.col("_contract_customer"), "left")
        .select("_state", sla(f.col("_state"), f.col("_contracts")).alias("_sla"))
        .select(
            *(
                f.timestamp_micros(f.col(f"{struct}.{name}")).alias(name)
                if kind == "TIMESTAMP"
                else f.col(f"{struct}.{name}")
                for struct, columns in (("_state", STATE_COLUMNS), ("_sla", SLA_COLUMNS))
                for name, kind in columns
            ),
        )
    )
    return frame


def build_ticket_state(
    spark: Any, *, catalog: str, pipeline_run_id: str, as_of: datetime
) -> dict[str, object]:
    """Recompute ``silver.ticket_state`` from full Silver history in one atomic overwrite.

    The result is a pure function of Silver tickets, events, contracts, and ``as_of``, so a
    retry rewrites identical state for the same inputs. Single writer required.
    """
    from pyspark.sql import functions as f

    from support_ops.transformations.silver import create_tables, qualified

    _aware(as_of)
    for entity in ("tickets", "ticket_events", "contracts"):
        create_tables(spark, catalog, entity)
    target = qualified(catalog, "silver", "ticket_state")
    columns = [f"`{name}` {kind}" for name, kind in TABLE_COLUMNS]
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {target} "
        f"({', '.join([*columns, '_state_run_id STRING'])}) USING DELTA"
    )
    frame = ticket_state_frame(spark, catalog=catalog, as_of=as_of).withColumn(
        "_state_run_id", f.lit(pipeline_run_id)
    )
    view = f"ticket_state_{uuid.uuid4().hex}"
    frame.createOrReplaceTempView(view)
    try:
        spark.sql(f"INSERT OVERWRITE TABLE {target} SELECT * FROM `{view}`")
    finally:
        spark.catalog.dropTempView(view)
    summary = (
        spark.table(target)
        .agg(
            f.count(f.lit(1)).alias("tickets"),
            f.coalesce(f.sum("applied_event_count"), f.lit(0)).alias("applied_events"),
            f.coalesce(f.sum("rejected_event_count"), f.lit(0)).alias("rejected_events"),
            f.count_if(f.col("contract_id").isNull()).alias("without_sla"),
            f.count_if(f.col("minutes_to_sla") < 0).alias("sla_breached"),
        )
        .first()
    )
    return {"source": "ticket_state", "pipeline_run_id": pipeline_run_id, **summary.asDict()}


def _micros_ddl(columns: Iterable[tuple[str, str]]) -> str:
    """UDF result schema: timestamps travel as BIGINT epoch microseconds."""
    return ", ".join(
        f"{name} {'BIGINT' if kind == 'TIMESTAMP' else kind}" for name, kind in columns
    )


def _to_micros(value: datetime) -> int:
    delta = _aware(value) - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86_400 + delta.seconds) * 1_000_000 + delta.microseconds


def _epoch(micros: int) -> datetime:
    return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=micros)


def _from_micros(value: object) -> datetime | None:
    return None if value is None else _epoch(int(str(value)))
