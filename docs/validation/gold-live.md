# Gold operational products — Databricks Free Edition validation

- Workspace target: `dev`; catalog: `support_dev`
- Validation date: 2026-09-30
- Authentication: Databricks CLI profile `support-ops-free` through the OS keyring
- Bundle: `databricks bundle validate --target dev --strict` passed
- Deployment preview: three Gold jobs to add, zero existing resources to change or delete
- Deployment: three Gold jobs created, 13 files uploaded, four resources unchanged

The deployed wheel contains `support-ops-gold`, `support-ops-customer-health`, and
`support-ops-incident-signals`. All three jobs completed successfully against existing Silver
Delta tables. The historical window `2025-12-31 12:00 UTC` was a smoke check; it had no new
tickets and no signals. A read-only query found `2026-01-01 04:00 UTC` with concentrated ticket
creations, so the jobs were run again for a more useful check.

| Job | Active-hour run | Result |
| --- | --- | --- |
| Support operations | [run 757538892611756](https://dbc-aa6ccf23-381b.cloud.databricks.com/jobs/204843442642457/runs/757538892611756?o=7474645852800686) | Success |
| Customer health, as of 05:00 UTC | [run 795284642588856](https://dbc-aa6ccf23-381b.cloud.databricks.com/jobs/711439267915312/runs/795284642588856?o=7474645852800686) | Success |
| Incident signals | [run 703743259817063](https://dbc-aa6ccf23-381b.cloud.databricks.com/jobs/258358788178938/runs/703743259817063?o=7474645852800686) | Success |

A read-only SQL statement (`01f1bcfb-2833-1848-bb0c-2b4c8d51a998`) checked the declared grains
and counts at those times:

| Table | Rows | Distinct grains | Observed metric |
| --- | ---: | ---: | ---: |
| `gold.support_operations` | 63 | 63 | 76 tickets created |
| `gold.customer_support_health` | 100 | 100 | 1,634 open tickets |
| `gold.incident_signals` | 6 | 6 | 6 signals |

For the incident table, `PRD-006` had 20 new tickets from 17 customers, two P1 tickets, and one
escalated ticket. Its seven-hour baseline was 1/7 ticket per hour, and the signal was true. The
other five products also crossed the configured threshold in this synthetic event burst. The
read-only incident-row query was statement `01f1bcfb-0b4a-1d09-9726-ce01702d95ae`. A signal
indicates unusual support volume; this run does not establish a real production incident.

The earlier smoke check wrote 62 operations rows, 100 customer rows, and 6 incident rows. It
reported zero tickets created and zero incident signals at that hour. All writes were confined to
`support_dev.gold`; the jobs read from `support_dev.silver`. Local integration tests provide replay
and stale-row replacement evidence. No production target was deployed or run.
