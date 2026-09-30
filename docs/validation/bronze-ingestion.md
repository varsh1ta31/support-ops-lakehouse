# Bronze ingestion validation

- Workspace type: Databricks Free Edition
- Target: `dev`
- Catalog: `support_dev`
- Validation date: 2026-09-29

## First successful ingestion

Run ID: `1064672348874709`

| Table | Records read | Records written | Duplicates |
| --- | ---: | ---: | ---: |
| `bronze.products` | 6 | 6 | 0 |
| `bronze.accounts` | 100 | 100 | 0 |
| `bronze.contracts` | 400 | 400 | 0 |
| `bronze.tickets` | 10,000 | 10,000 | 0 |

Unity Catalog reports all four targets as managed Delta tables.

## Idempotency rerun

Run ID: `1118863990353971`

| Table | Records read | Records written | Duplicates |
| --- | ---: | ---: | ---: |
| `bronze.products` | 6 | 0 | 6 |
| `bronze.accounts` | 100 | 0 | 100 |
| `bronze.contracts` | 400 | 0 | 400 |
| `bronze.tickets` | 10,000 | 0 | 10,000 |

The rerun demonstrates that identical input does not create additional Bronze records.

## Serverless compatibility finding

The initial run exposed that DataFrame persistence (`cache`/`persist`) is unavailable on the Free
Edition serverless Spark Connect environment. The ingestion implementation was changed to avoid
persistence. This trades repeated evaluation of the small input plan for compatibility with the
project's required zero-cost runtime.
