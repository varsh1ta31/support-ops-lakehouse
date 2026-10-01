# Spark performance experiment results

- Environment: Databricks Free Edition `dev`, serverless Spark with Photon
- Date: 2026-09-30
- Dataset: 1,000,000 generated events, 1,000 account rows, 64 diagnostic hash partitions
- Runs: [forward order](https://dbc-aa6ccf23-381b.cloud.databricks.com/jobs/10111217004781/runs/354436517775596?o=7474645852800686), [reverse order](https://dbc-aa6ccf23-381b.cloud.databricks.com/jobs/10111217004781/runs/1054269642150760?o=7474645852800686)
- Stored evidence: `support_dev.ops.spark_performance_runs`; read-only comparison statement
  `01f1bd2d-2f32-1422-afa8-b50c36bda237`

The two deliberately large customers own 35% and 15% of events. A diagnostic
`repartition(64, customer_id)` put **361,050** rows in its largest partition and **3,000** in its
smallest (120× difference). The mean is 15,625. This is a measured skewed input, not an assumed
one. Every strategy joined exactly 1,000,000 events and returned the same event-ID checksum,
499,999,500,000.

| Strategy | Physical join | Forward | Reverse | Shuffle read/write | Spill | Longest task |
| --- | --- | ---: | ---: | --- | --- | --- |
| Forced merge baseline | SortMergeJoin | 1.694 s | 1.509 s | unavailable | unavailable | unavailable |
| Default optimizer | Photon BroadcastHashJoin | 0.771 s | 0.767 s | unavailable | unavailable | unavailable |
| Explicit broadcast | Photon BroadcastHashJoin | 1.180 s | 0.782 s | unavailable | unavailable | unavailable |
| Salted merge | SortMergeJoin | 1.662 s | 1.705 s | unavailable | unavailable | unavailable |

The timed action generates the deterministic rows, joins to the dimension, and groups by the
dimension's support tier. Query plans are stored in the result table. SQL-plan counters returned
`{}` through the serverless Spark Connect Python interface, so shuffle bytes, spill, and
longest-task duration are unavailable to this runner; no values were inferred. The job-run Spark
UI remains the place to inspect task-level behavior interactively. Run timing includes ordinary
runtime noise and does not prove that skew alone caused the baseline's entire delay.

**Decision:** use the default optimizer for this workload. In both orders it chose a Photon
broadcast hash join for the small account table and completed about twice as fast as the forced
sort-merge baseline. Explicit broadcast did not improve on the default plan, and salting added
complexity without a measured gain. The bottleneck in this experiment is the forced shuffle/sort
join path on a strongly skewed key; the observed broadcast plan avoids that path. The behavior is
already visible at one million events, so the dataset was not expanded to five or ten million.

The serverless runtime rejects changes to `spark.sql.autoBroadcastJoinThreshold` and does not
allow DataFrame caching. AQE could not be switched off for a strict on/off experiment. This is a
comparison of join plans under the serverless optimizer, as explained in
[ADR 0014](../architecture/0014-spark-performance-experiment.md).
