# Historical feature backfill and training-data validation

- Target: Databricks Free Edition `dev`, `support_dev.gold.ticket_features` and
  `support_dev.ml.training_dataset`
- Date: 2026-09-30
- [Backfill run](https://dbc-aa6ccf23-381b.cloud.databricks.com/jobs/710790026891478/runs/942416066140452?o=7474645852800686): succeeded with `before=2026-01-01T00:00:00Z`
- [Replay run](https://dbc-aa6ccf23-381b.cloud.databricks.com/jobs/710790026891478/runs/446187267316684?o=7474645852800686): succeeded with the same boundary
- [Training-data run](https://dbc-aa6ccf23-381b.cloud.databricks.com/jobs/59372977434413/runs/873358111439230?o=7474645852800686): succeeded with outcome cutoff `2026-09-30T00:00:00Z`

The backfill wrote **9,904 distinct historical ticket rows**, from `2025-01-01 02:00` through
`2025-12-31 23:00` UTC. The existing operational snapshot at `2026-01-01 05:00` retained its
**1,634 rows**. Read-only statement `01f1bd31-5db9-1ad0-9a2b-7b4f11d392ba` checked the
two ranges and distinct ticket counts.
After replay, read-only statement `01f1bd31-df94-1768-a384-b9f9e89aec7a` still found 9,904
rows, 9,904 distinct tickets and keys, no early labels, and no pre-event activity counts.

The rebuilt training table contains one row per eligible resolved ticket, split by the feature
timestamp. The outcome cutoff is later than every scoring point. Read-only statement
`01f1bd31-96dd-154f-b893-db6912ac90bc` checked counts, breach labels, timestamps, and
eligibility:

| Split | Scoring period UTC | Tickets | Breaches | Resolved before scoring | Overdue at scoring |
| --- | --- | ---: | ---: | ---: | ---: |
| Train | before 2025-10-01 | 6,293 | 768 | 0 | 0 |
| Validation | 2025-10-01 to 2025-11-01 | 735 | 83 | 0 | 0 |
| Test | 2025-11-01 to 2026-01-01 | 1,405 | 175 | 0 | 0 |

The historical feature path has a Spark/Delta fixture for next-hour eligibility, contract
SLA, peer resolution visibility, and future escalation exclusion. Ruff and mypy pass locally.
The local fixture cannot start on this machine because Java is not installed; the Databricks
job validates execution on real dev data. See [ADR 0016](../architecture/0016-historical-feature-backfill.md)
for availability assumptions.
