# ADR 0006: Durable validation decisions before Silver writes

- Status: accepted
- Date: 2026-09-29

## Decision

Run batch entities sequentially: products, accounts, contracts, then tickets. Perform customer,
product, and existing-key checks using Spark joins; execute the shared Python validator on workers.
Only bounded quality aggregates and the single completion metric return to the driver. No
DataFrame caching or driver-side reference collection is required.

Persist each entity's complete validation result in one atomic Delta merge into
`ops.silver_evaluations`, keyed by `(pipeline_run_id, source, _record_hash)`. Include a snapshot
marker even for empty input. The stage preserves typed ingestion metadata, normalized JSON, raw
payload, disposition, and quarantine details. Reuse this snapshot after any failure; new arrivals
are processed under a new run ID. Stage history currently remains available for audit and repair;
retention/vacuum policy is future operational work.

Write typed Silver rows by natural key, then quarantine rows by stable record ID, using insert-only
merges. Within a snapshot, choose the earliest valid candidate by Bronze ingestion timestamp,
then record hash. Invalid variants remain quarantined and never consume a valid natural key.
As in ADR 0005, valid changed records with an existing key are duplicates; this is a first-accepted
history loader, not an account/ticket update feed. Event-driven state changes belong in ticket state.

Finally insert one `ops.quality_metrics` row per `(pipeline_run_id, source)`, containing records read,
accepted, duplicate, and quarantine counts plus measurement time. These counts describe the frozen
validation decisions, not physical inserts in a retry. Their sum equals records read. The metric
also marks completion: an already completed entity returns its original metric immediately.

## Failure and concurrency behavior

The three output tables do not share a transaction. A failed run can expose partial outputs, but
its completion metric is absent. Consumers requiring complete runs must check that metric. Retry
with the same run ID repairs missing outputs without changing classification or multiplying rows.
Do not delete individual outputs from a completed run and expect that run ID to rebuild them.

The job permits one concurrent run and executes all four entities in a single task with retries.
Manual invocations must also obey the single-writer requirement. Catalog/schema bootstrap and
Bronze ingestion are prerequisites. Events are supported by the reusable adapter but are omitted
from the batch job until streaming Bronze events exist.

## Compatibility and costs

A scalar UDF reuses tested domain rules; distributed joins avoid unbounded Python reference sets.
This incurs Python serialization overhead and full Bronze scans per new run. Durable staging costs
storage, but supports the serverless runtime where DataFrame persistence is unavailable. Incremental
input selection and retention policies can be introduced later without weakening retry semantics.

The wheel version changes to 0.2.0 so serverless jobs do not reuse the previous package version.
The job uses the existing Free Edition target and introduces no paid service.

API references: [Spark UDF return schemas](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html)
and [Databricks serverless dependency management](https://docs.databricks.com/aws/en/compute/serverless/dependencies).
