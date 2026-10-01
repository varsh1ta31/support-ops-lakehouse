# ADR 0014: Customer-event join performance experiment

Status: accepted; dev measurements in [performance results](../validation/performance-results.md).

## Decision

Generate the benchmark input inside Spark with `range` so every run is deterministic and no
million-row file upload is required. Of one million events, 35% belong to `customer_001` and
15% to `customer_002`; the remainder is spread over 998 customers. The account dimension has
one row per customer and supplies the `support_tier` used in the final aggregate. Every strategy
must return one million joined rows and the same event-ID checksum.

Databricks Free Edition runs this job on serverless Spark. It rejects setting the AQE and
automatic broadcast configuration keys and does not support DataFrame caching. The comparison
therefore holds the serverless optimizer environment fixed and uses plan hints where needed:

- `merge_baseline`: force a sort-merge join on `customer_id`.
- `adaptive_default`: let the serverless optimizer choose without a join hint.
- `broadcast`: explicitly broadcast the 1,000-row account dimension.
- `salted_merge`: split the two hot customer keys into 16 deterministic buckets, replicate their
  account rows, and force a sort-merge join on `(customer_id, salt)`.

This is a comparison of four join plans under serverless AQE, not an AQE-on versus AQE-off
controlled experiment. Timed actions include deterministic input projection and the downstream
support-tier aggregate. The benchmark measures customer-key hash-partition distribution
separately. It records formatted physical plans and SQL-plan counters when the runtime exposes
them. Free Edition may not expose shuffle, spill, and longest-task values to the Python job; such
cells must be marked unavailable in the report rather than estimated.

The run writes one row per strategy to `ops.spark_performance_runs`. Delta `replaceWhere` replaces
all four rows for a retried `run_id` atomically. The one-million-event starting size is retained
when skew is visible; scaling to five or ten million is unnecessary in that case.
