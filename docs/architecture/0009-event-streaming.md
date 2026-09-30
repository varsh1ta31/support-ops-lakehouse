# ADR 0009: File-based event streaming into Bronze

- Status: accepted
- Date: 2026-09-29

## Decision

Simulate the event feed with deterministic JSON Lines files and ingest them into
`bronze.ticket_events` with Structured Streaming. The ingestion job
(`support-ops-stream-ingest`) is separate from the Silver job, so a Silver failure never stops raw
event capture.

### Producer

`support_ops.synthetic.events` writes `events_000001.json`, `events_000002.json`, … into
`/Volumes/<catalog>/raw/ticket_events`. Each batch file covers a fixed event-time window
(`batch_interval_minutes`) and is a pure function of the seed and batch number:

- Tickets created in a batch have lifecycles bounded to `lifecycle_batches`, so a file depends only
  on a fixed lookback of earlier batches. Lifecycles follow ADR 0007's transition rules and
  replay with no rejected events. Streamed tickets use `STK-` IDs and reference the demo profile's
  customers and products.
- A `late_rate` share of events is delivered `late_delay_minutes` after its event time. A
  `duplicate_rate` share is re-delivered in the next file. Each file also carries
  `malformed_per_batch` unparseable lines and `invalid_per_batch` parseable records that Silver
  must quarantine: unknown customer, invalid priority, malformed timestamp, missing ticket ID, and
  unknown event type.
- Files are written under a hidden temporary name and renamed into place. The writer resumes
  after the highest existing batch number.

### Stream

- **Source.** The text file source reads whole lines with an explicit parse schema
  (`from_json`, `PERMISSIVE`). Every non-blank line lands in Bronze. Business fields stay strings, as
  in batch Bronze, and fields outside the schema are dropped. A line is malformed if the parser
  flags it, if it is not one braced object, or if it carries no schema field. The parser ignores
  trailing content, so `{}}` would otherwise become an all-null event. Malformed lines keep
  their raw text in `_corrupt_record`, with null business fields.
- **Trigger.** `availableNow` with `maxFilesPerTrigger` (default 1). Serverless compute supports
  only this trigger, so continuous ingestion means rerunning the job on a schedule against the
  same checkpoint at `<checkpoint_root>/<stream_name>`.
- **Deduplication.** `foreachBatch` performs an insert-only `MERGE` on `_record_hash`, the same
  key as batch Bronze. Identical records are therefore stored once across microbatches and restarts.
  A malformed line has no event identity, so its hash covers the source file and raw text. The
  same garbage in two files is two arrivals, while a replay of one file stays idempotent.
  A redelivered event with a changed payload is new raw evidence. Silver's `event_id` rule
  accepts the first valid one and rejects the rest as duplicates.
- **Event time and lateness.** The watermark for microbatch *k* is the highest event time seen in
  microbatches before *k*, minus `late_tolerance` (default 60 minutes, from
  `late_event_tolerance_minutes`). Rows with an earlier event time get `_is_late = true`. Rows
  whose time does not parse get null, and the first microbatch has no watermark. Event times
  after processing time do not advance the watermark, so one future-dated event cannot mark
  everything late. Late events are labelled, not dropped: ticket state recomputes from full history
  in event-time order (ADR 0007).
- **Metrics.** `ops.streaming_metrics` holds one row per `(stream_name, batch_id)`: rows read,
  written, duplicate, malformed and late, per-batch and running maximum event time, watermark, and
  Spark's input/processing rates and batch duration.

### Retry safety

Each microbatch tags its rows with `_pipeline_run_id = <stream_name>/<batch_id>` and writes its
metrics row last, as a completion marker. A replay of an uncommitted batch reads the same files.
Its Bronze merge inserts nothing new, counts derive from the tagged rows, and the metrics match a
clean run. A replay after the metrics commit returns immediately. The job run ID is kept in the
metrics row.

### Silver

The Silver job now validates `ticket_events` after the reference entities and tickets, before
rebuilding ticket state. It creates the Bronze events table if the stream has not run yet. A row
with `_corrupt_record` is quarantined with the reason `record: malformed source line`. Its
quarantine `raw_payload` includes the original text.

## Tradeoffs and limitations

- Lateness is an arrival property, so it is recorded in Bronze rather than Silver. Silver events
  do not yet carry `_is_late`; join on `_record_hash` if needed. Per-run late counts are in
  `ops.streaming_metrics`.
- The watermark depends on microbatch composition. Files are taken oldest first by modification
  time, and producer writes are sequential, so order follows batch number. Reprocessing the
  same files with different `maxFilesPerTrigger` can label a borderline event differently.
- The Bronze `MERGE` scans existing hashes on each microbatch. That is acceptable at demo volume. A
  bounded alternative is `dropDuplicatesWithinWatermark`, at the cost of forgetting old hashes.
- Resetting a checkpoint restarts batch IDs. Use a new `--stream-name`, which also selects a new
  checkpoint directory, rather than reusing a name whose metrics rows already exist.
