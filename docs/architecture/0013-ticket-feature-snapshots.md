# ADR 0013: Point-in-time ticket feature snapshots

Status: accepted; [dev workspace validation](../validation/ticket-features.md) completed.

## Decision

`gold.ticket_features` has grain `(as_of, ticket_id)`. `as_of` is an explicit UTC hour boundary.
Only tickets active immediately before that boundary appear. Silver ticket state is reconstructed
at `as_of - 1 microsecond`; current `silver.ticket_state` is never used for historical snapshots.
Account segment is taken from the accepted account record; support tier and remaining SLA minutes
come from the contract selected by ticket-state reconstruction.

Ticket age and remaining SLA minutes are measured at the scoring point. Customer and agent
message counts and priority-change counts come from accepted Silver event rows before `as_of`;
total message, reopen, and escalation counts come from reconstructed state. Historical batch
tickets without full event history may have fewer event-type counts than their complete real-world
history. Silver event validation does not establish whether each event was a valid state transition,
so event-type counts can include a transition rejected by state reconstruction.

Peer cohorts exclude the scored ticket. Ticket-volume features count other tickets created in
`[as_of - 30 days, as_of)` for the customer or `[as_of - 24 hours, as_of)` for the product.
Escalation rates use other tickets created in the stated window and indicate whether they have
escalated by `as_of`. Breach rates use other tickets resolved in the stated 90-day or 7-day window
and compare the exact resolution timestamp to the SLA deadline. Missing SLA contracts are excluded
from breach-rate denominators. Empty cohorts yield null rates; empty volume cohorts yield zero.
Customer and product attributes are currently insert-only, so historical changes to those
dimensions cannot be reconstructed.

`sla_breached` is nullable and null for every active scoring row. A future resolution cannot be
used as a feature or as a label at scoring time. Phase 6 training must join a later observed
outcome to an earlier feature snapshot and exclude unresolved or censored examples; it must not
turn this null into a negative label.

## Persistence and operation

A single Delta `replaceWhere` commit replaces the complete `as_of` snapshot, including stale
tickets, while preserving other scoring points. Each row carries `_pipeline_run_id`. Rerun the
snapshot after late or corrected Silver data. Keep Gold writers serialized and Silver stable while
building a snapshot. The bundle job is parameterized and unscheduled pending Phase 8 orchestration.

## Verification

The Spark/Delta fixture covers historical state, future-event exclusion, cohort rates, absent
future tickets, null labels, replay, neighboring snapshots, and stale-row removal. Its local run
requires Java 17 or newer.
