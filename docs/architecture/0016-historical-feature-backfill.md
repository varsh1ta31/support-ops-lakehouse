# ADR 0016: Historical ticket feature backfill before event streaming

Status: accepted; verified in the dev workspace (see [validation](../validation/historical-backfill.md)).

The 2026-01-01 operational feature snapshot produced too few labeled examples for chronological
model evaluation. A separate unscheduled job now builds one feature row per historical Silver
ticket at the next UTC hour after creation. Tickets resolved before that hour are excluded.
The backfill is limited to scoring hours before the first accepted Silver ticket event. The job
checks this boundary before writing. In that interval, event counts are zero; batch ticket
history provides creation, resolution, contract SLA, and peer cohort facts.

The ticket's resolution is used only to decide whether it was still active at scoring and, for
peer rates, only after that peer had resolved. Escalation from the batch ticket record is visible
only after its resolution. The ticket being scored is excluded from its peer cohorts. SLA terms
are selected from the contract effective on the ticket's creation date and priority. Account
segment and contract records are insert-only dimensions without a load-time history, so a
retroactive correction could change a rebuilt feature row; run against stable Silver inputs and
record the Gold table version used for training.

An atomic Delta `replaceWhere` replaces all backfill rows before the configured UTC boundary,
including stale rows, while preserving operational snapshots at or after it. The job refuses
to overwrite an operational row in its range. Rerunning with unchanged inputs is idempotent.
The period restriction avoids pretending the batch records contain message or state-transition
events. A later event-aware backfill would need point-in-time state reconstruction instead.
