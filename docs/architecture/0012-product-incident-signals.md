# ADR 0012: Product incident signals against a fixed hourly baseline

Status: accepted; verified in the dev workspace (see [Gold live validation](../validation/gold-live.md)).

## Decision

`gold.incident_signals` has grain `(time_window, product_id)`. `time_window` is the explicit UTC
hour start and means `[time_window, time_window + 1 hour)`. Every accepted Silver product gets one
row, including products with zero tickets. Ticket creation counts use the same point-in-time fold
as other Gold products, evaluated at the end of the hour. Repeated source events do not create
multiple ticket rows.

The baseline is the arithmetic mean of ticket creations in the **same UTC hour on each of the
previous seven calendar days**. Each day contributes a value, including zero if no tickets were
created. The current hour is excluded. The baseline therefore has a fixed denominator of seven
and is available without an incident-free training period. It is not seasonally adjusted beyond
hour-of-day matching, and is a heuristic rather than a statistical confidence interval.

Metric contracts:

- `ticket_count`: tickets created in the current hour.
- `unique_customers`: distinct customers among those tickets.
- `p1_count`: current-hour tickets whose priority is P1 at hour end.
- `escalation_count`: current-hour tickets with at least one escalation visible by hour end.
- `ticket_volume_baseline`: total creation count in the seven comparison hours divided by seven.
- `volume_deviation`: current ticket count minus the baseline, in tickets per hour.
- `incident_signal`: true only when `ticket_count >= 5`, `unique_customers >= 3`, and current
  ticket count is at least twice the baseline. The five-ticket floor handles zero baselines. A
  true signal merits investigation but is not proof of a production incident.

Counts are ticket based, including one per event-created ticket. Historical Silver tickets may
lack intermediate priority or escalation timestamps (ADR 0007); their end-of-hour state is used
where history is available. Later input corrections or late arrivals require replaying affected
hours and the following seven comparison days. UTC boundaries make the baseline independent of
local daylight-saving changes.

## Persistence and operation

A single Delta `replaceWhere` commit replaces the complete target hour, deleting stale products
while preserving other hours. Each row carries `_pipeline_run_id`. Source snapshots are not frozen
across retries, so run against stable Silver input and serialize writers. The bundle job requires
an explicit UTC hour and has no schedule; orchestration remains Phase 8. No paid service is used.

## Verification

The local Spark/Delta fixture checks six tickets across seven comparison hours (one zero hour),
out-of-window tickets, a five-ticket spike across three customers, P1 and escalation counts,
zero-baseline and quiet products, neighboring hours, replay, and stale product removal. The subsequent workspace run is recorded in [Gold live validation](../validation/gold-live.md).
