# Ticket feature snapshot validation

- Target: Databricks Free Edition `dev`, catalog `support_dev`
- Date: 2026-09-30
- Bundle validation: `databricks bundle validate --target dev --strict` passed
- Deployment: one Gold job created, zero existing resources changed or deleted
- Scoring point: `2026-01-01T05:00:00Z`
- [First job run](https://dbc-aa6ccf23-381b.cloud.databricks.com/jobs/818023496355765/runs/1022501921292971?o=7474645852800686): succeeded (126 seconds execution)
- [Replay run](https://dbc-aa6ccf23-381b.cloud.databricks.com/jobs/818023496355765/runs/193671244255956?o=7474645852800686): succeeded (56 seconds execution)

A read-only SQL check (`01f1bd27-4d94-1f74-8f57-407097f63619`) found 1,634 rows and 1,634
distinct ticket IDs at this scoring point. All 1,634 `sla_breached` labels were null, as required
for active tickets. The ticket-age range was 0 to 525,702 minutes. This matches the 1,634 open
tickets reported by the previously validated customer-health snapshot for the same point.

After replay, read-only SQL statement `01f1bd27-b1be-1ae6-a41b-867b2e3e3aa9` found the same
1,634 rows and 1,634 distinct ticket IDs, all stamped with the replay run ID. This demonstrates
that rerunning the scoring hour replaced its snapshot without duplicating it.

A final read-only check (`01f1bd27-d0a9-1805-a46d-1aaf8431d1d7`) found nonzero customer and
product ticket history and a defined customer breach rate for all 1,634 rows; no row had a
missing SLA value in this synthetic fixture.

Local static checks passed: Ruff formatting/lint and strict mypy. The 136 engine-neutral unit
tests passed. The added Spark/Delta fixture could not start locally because this machine has no
Java runtime; the dev job and read-only SQL query provide live Spark execution evidence. The
fixture remains in the repository for CI and Java-enabled development environments.
