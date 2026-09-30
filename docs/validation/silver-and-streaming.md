# Silver and event-streaming validation

- Workspace type: Databricks Free Edition (serverless)
- Target: `dev`
- Catalog: `support_dev`
- Validation date: 2026-09-29

## Silver on batch Bronze

Run ID: `1037737889621189`

| Entity | Read | Accepted | Duplicates | Quarantined |
| --- | ---: | ---: | ---: | ---: |
| products | 6 | 6 | 0 | 0 |
| accounts | 100 | 100 | 0 | 0 |
| contracts | 400 | 400 | 0 | 0 |
| tickets | 10,000 | 10,000 | 0 | 0 |
| ticket_events | 0 | 0 | 0 | 0 |

`silver.ticket_state` held 10,000 tickets, all with a governing contract (`without_sla = 0`).
There were 2,498 breaches as of the run: generated resolution breaches (12% of the 85% resolved)
plus 2025 tickets still open at run time.

## Stream recovery

The producer appended four files per run. Each file was one `availableNow` microbatch.

| Stream run | Files available | Microbatches processed | Batch IDs |
| --- | --- | ---: | --- |
| `115099419220111` | 1–4 | 4 | 0–3 |
| `712021556819442` | 1–8 | 4 | 4–7 |
| `733441432886037` | 1–8 | 0 | — |

The restarted stream resumed from its checkpoint, processed only files 5–8, and a third run with
no new files processed nothing. After all runs, `bronze.ticket_events` held 1,889 rows with 1,889
distinct `_record_hash` values, equal to the sum of `rows_written` in `ops.streaming_metrics`.

## Duplicate, late, and malformed events

- Duplicates: re-delivered events were counted in `duplicate_rows` and not stored twice. Silver
  then saw no `event_id` duplicates.
- Late: the producer delays a share of events by 180 minutes. The first such events arrived in
  file 13 and were labelled `_is_late` against the watermark in effect (running maximum event
  time minus 60 minutes). Ten late events were labelled across files 13–20.
- Malformed: each file carries one malformed line. All were retained with raw text and quarantined
  by Silver with `record: malformed source line`.

## Findings fixed during validation

1. `{}}` parsed as an empty object without a corrupt flag, landing as an all-null event without its
   raw text. Such lines are now malformed (ADR 0009). One all-null row from before the fix remains
   in Bronze (file 5). Silver quarantines it for missing required fields.
2. Identical malformed text in different files was deduplicated as if it were one event,
   under-reporting malformed arrivals (files 10–16 reported 0). Malformed hashes now include the
   source file; files 17–20 each reported one malformed row.

## Silver with streamed events

Run ID: `266322773978327`

- `ticket_events`: 1,889 read, 1,860 accepted, 0 duplicates, and 29 quarantined. The quarantine is
  20 injected invalid records, 8 malformed lines, and the pre-fix all-null row.
- Batch entities were all duplicates of the first run, as expected for insert-only natural keys.
- `silver.ticket_state`: 10,395 tickets (10,000 historical and 395 streamed), 1,845 applied events,
  0 without SLA. Five streamed tickets are not materialized yet because their `ticket_created`
  event is still in flight as a late delivery.
- Three events were rejected as transitions. Each belongs to a ticket whose late
  `ticket_resolved` had not arrived, so the later `status_changed → closed` was not allowed. Ticket
  state is recomputed from full history, so these clear once the late events land.
