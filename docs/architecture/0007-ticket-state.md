# ADR 0007: Ticket state as a deterministic fold over history

- Status: accepted
- Date: 2026-09-29

## Decision

Reconstruct `silver.ticket_state` with `support_ops.transformations.ticket_state`. Each ticket's
state is a pure function of its validated Silver ticket row (if any), its Silver events, and an
explicit `as_of` timestamp. The same function serves current state and point-in-time state for
later Gold metrics and ML features.

Order events by `(event_time, event_id)`; the ID breaks ties deterministically and repeated IDs
apply once. Events after `as_of` are excluded. Recomputing from full history places late events
in event-time order, so no late-event correction logic is needed here. Streaming watermarks and
late-event classification remain Phase 3 work.

A historical ticket is a snapshot taken at `closed_at`, or at `created_at` if still open. Events at
or before that snapshot are already reflected and are skipped. Before `closed_at`, a resolved
ticket appears open with no resolution time. Its `escalated` flag counts only once the snapshot is
visible, because escalation time is unknown and earlier counting would leak the outcome.
A ticket without a history row starts from a `ticket_created` event carrying its priority in
`new_value`. Events for tickets that have neither are not materialized.

## Transition rules

Statuses are `open`, `in_progress`, `pending_customer` (active) and `resolved`, `closed`.
Validation quarantines unsupported ticket `final_status`, `status_changed` values, and
`ticket_created` events without a valid priority.

| Event | Allowed from | Effect |
| --- | --- | --- |
| `agent_assigned` | active, with an agent | set `assigned_agent` |
| `priority_changed` | active | set `priority` |
| `status_changed` | active → active; `resolved` → `closed` | set `status` |
| `customer_replied` | any | message +1; `pending_customer` → `open` |
| `agent_replied` | any | message +1 |
| `engineering_escalated` | active | escalation +1 |
| `ticket_resolved` | active | `resolved`, set `resolved_at` |
| `ticket_reopened` | `resolved`, `closed` | `open`, clear `resolved_at`, reopen +1 |

A disallowed transition, a repeated `ticket_created`, or an event before `created_at` leaves the
state unchanged and increments `rejected_event_count`. These events already passed record-level
validation, so they stay in `silver.ticket_events` and are not quarantined; the counter makes
sequence anomalies queryable per ticket.

## Persistence and retries

The Spark adapter groups events per ticket, joins them with Silver tickets, and applies the fold
on workers in a scalar UDF. Timestamps cross the UDF boundary as UTC epoch microseconds because
PySpark converts timestamps to naive local-time datetimes. The whole table is replaced in one
atomic `INSERT OVERWRITE`; a retry with the same inputs and `as_of` rewrites identical rows.
The Silver job builds state after all batch entities, using the run's start time as `as_of`.

## Tradeoffs and remaining work

A full recompute scans all tickets and events per run. That is simple and correct at the
project's scale; incremental recomputation of touched tickets can be introduced later without
changing the fold. `minutes_open` depends on `as_of`, so current-state rows age only when rebuilt.
SLA fields (`support_tier`, `resolution_sla_minutes`, `sla_deadline`, `minutes_to_sla`) are
defined in ADR 0008.
