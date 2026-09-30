# ADR 0011: Customer support health at a scoring point

Status: accepted; verified in the dev workspace (see [Gold live validation](../validation/gold-live.md)).

## Decision

`gold.customer_support_health` has grain `(as_of, customer_id)`. `as_of` is an explicit UTC hour
boundary; the snapshot includes data strictly before it. Every accepted Silver account gets one
row, including accounts with no tickets. Account `support_tier` and `annual_contract_value` are
dimensions from the account record. Ticket state and contract SLA are reconstructed from Silver
history at the last microsecond before `as_of` (ADR 0007 and 0008). Current `silver.ticket_state`
is not used for historical snapshots.

Metric contracts:

- `open_ticket_count`: tickets in an active status at `as_of`; `p1_ticket_count` is the P1 subset.
- `ticket_count_30d`: tickets created in `[as_of - 30 days, as_of)`.
- `breach_rate_90d`: share of resolved tickets in `[as_of - 90 days, as_of)` whose exact resolution
  timestamp exceeded their SLA deadline. A missing SLA is excluded from the denominator; an empty
  denominator gives null.
- `escalation_rate_90d` and `reopen_rate_90d`: share of tickets created in
  `[as_of - 90 days, as_of)` with at least one escalation or reopen visible by `as_of`. These are
  ticket cohort rates, not counts of transitions.
- `average_resolution_minutes`: mean creation-to-resolution duration among the same resolved
  90-day cohort. No resolved tickets gives null.

Count metrics are zero for an account with no tickets. Rates and mean duration are null if their
cohort is empty. Tickets without an accepted account are not represented because this product's
grain is the account. Historical batch snapshots have the transition-history limits in ADR 0007.
The account table is insert-only; historical account attribute changes would require an
effective-dated account source. A later reopen removes the ticket from the current resolved cohort,
while an earlier snapshot retains its prior resolution.

## Persistence and operation

A single Delta `replaceWhere` commit replaces an entire `as_of` snapshot. It deletes stale customer
rows while preserving all other snapshots. Rerun the same `as_of` after corrected or late Silver
input; changing source data between attempts can change results. Keep writers serialized and run
against stable Silver input. Each row carries `_pipeline_run_id`.

The bundle job is parameterized and unscheduled. Supply `as_of` after Silver completes. Full
workflow scheduling and dependencies remain Phase 8. No paid service is required.

## Verification

The local Spark/Delta fixture covers accounts with no tickets, old and future tickets, exact 30/
90-day window boundaries, reopened state, SLA breaches, rates, duration, replay, stale account
removal, and preservation of a neighboring snapshot. The subsequent workspace run is recorded in [Gold live validation](../validation/gold-live.md).
