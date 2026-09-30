# ADR 0010: Hourly support operations from point-in-time state

Status: accepted; verified in the dev workspace (see [Gold live validation](../validation/gold-live.md)).

## Decision

The first Phase 4 slice is `gold.support_operations`. Its grain is
`(hour, product_id, support_tier, customer_segment)`. Hours are UTC, half-open
`[hour, hour + 1 hour)`. Reconstruct Silver tickets and ordered events as of the last
microsecond of the hour, using the existing ticket-state and contract rules. Never read
`silver.ticket_state` for historical aggregation. The reusable state DataFrame also feeds the
existing current-state writer.

Metric contracts:

- `tickets_created`: tickets created within the hour.
- `tickets_resolved`: tickets whose end-of-hour state has a resolution timestamp in the hour.
  This counts tickets, not resolution events. A ticket resolved then reopened within the hour
  contributes to backlog, not resolved count. A later reopen does not alter the earlier hour.
- `open_backlog`: tickets in an active status at hour end, including tickets created earlier.
- `p1_ticket_count`, `p2_ticket_count`: active backlog by end-of-hour priority.
- `median_resolution_minutes`: exact median elapsed creation-to-resolution time among the
  resolved cohort; null when no tickets resolved.
- `sla_breach_rate`: breached / known-SLA resolved tickets in the hour. Compare exact timestamps;
  resolution exactly at the deadline is on time. Unknown SLA is excluded; empty denominator is null.
- `escalation_rate`, `reopen_rate`: share of the hour's created cohort with at least one
  escalation or reopen visible by hour end. Empty denominator is null. These are cohort rates,
  not counts of transitions during the hour.

Support tier is selected from the contract effective at creation for the reconstructed priority
(ADR 0008). Segment comes from the accepted Silver account. Missing tier/segment uses `unknown`.
Emit only dimension groups with creations, resolutions, or active backlog; no dense zero grid.
Historical batch snapshots retain the limitations in ADR 0007: intermediate priority, assignment,
and transition history cannot be recovered if it was never provided. Accounts are currently
insert-only; mutable account dimensions would require effective-dated joins.

## Persistence and operation

An explicit, timezone-aware hour is required, including on retries. There is no wall-clock default
that could move a retry to a different hour. A single Delta `replaceWhere` commit replaces that
hour, removing stale groups even if recomputation is empty, while preserving all other hours.
Each row carries `_pipeline_run_id`. Input corrections or late arrivals require replaying affected
hours. Same source contents and hour produce the same metrics; source versions are not frozen
across retries. Serialize writers and run against stable Silver inputs for consistent backfills.
The Gold schema is created by the existing platform bootstrap.

The bundle job is manually parameterized and unscheduled. Run Silver first, then submit an hour;
production scheduling/dependencies remain in Phase 8. No paid services are introduced.

## Tradeoffs and next dependency

This slice scans full history and folds events once per requested hour. It favors correctness and
reuse before Phase 5 performance measurement. It is not yet a bulk backfill engine. Customer
health, incident baselines, and point-in-time feature snapshots remain Phase 4 work.

## Verification

`tests/integration/test_gold_delta.py` uses real Spark/Delta to check known cohorts, exact-deadline
SLA behavior, null rates, next-hour boundaries, future-event exclusion, duplicate-free replay,
empty-hour replacement, and preservation of neighboring hours. Existing ticket-state tests cover
transition folding; the full suite exercises the refactored state writer. Unit tests validate hour
arguments and CLI dispatch. The subsequent live Databricks runs are recorded in [Gold live validation](../validation/gold-live.md).
