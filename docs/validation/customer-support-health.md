# Customer support health — local validation

`gold.customer_support_health` is the second Phase 4 product. The known fixture uses
`2026-01-01T12:00:00Z` as its scoring point.

## Fixture result

| Metric for account c1 | Expected and observed |
| --- | --- |
| Open tickets / P1 open tickets | 1 / 1 |
| Tickets created in 30 days | 3 |
| SLA breach rate on resolved tickets in 90 days | 2/3 |
| Escalation rate on created tickets in 90 days | 1/4 |
| Reopen rate on created tickets in 90 days | 1/4 |
| Average resolution minutes in 90 days | 14,410 |

The account support tier and annual contract value are retained. An account with no tickets has
zero counts and null rates. A 91-day-old open P1 ticket contributes to backlog but not the
30-day or 90-day cohorts. A ticket created exactly at the scoring point is excluded. At the next
hour, that ticket enters while the ticket on the 30-day start boundary leaves; the count remains
three. The on-time ticket at the 90-day start boundary leaves, changing the breach rate to 1.

A rerun of the same scoring point preserves exactly one row per account. Rebuilding after an
account is removed deletes that stale row for the target snapshot and preserves the adjacent
snapshot. The fixture cleans its Silver rows before other integration tests run.

## Checks and deployment limit

Ruff formatting/lint, mypy, an isolated wheel build, wheel entry-point inspection, and offline
Databricks bundle schema validation pass. The Spark/Delta fixture exercises the job's real
aggregation and write path. The full regression suite passed: **144 tests** with **96.20% total
coverage** in 109.35 seconds.

Authenticated deployment and job execution are recorded in
[Gold live validation](./gold-live.md).
