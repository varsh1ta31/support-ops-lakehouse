# Hourly Gold support operations — local validation

The first Phase 4 component is `gold.support_operations`; ticket feature snapshots remain pending.

## Reproduction

Use the project virtual environment, Java 17+ (validated with Java 21), and the existing Spark 4.0 /
Delta 4.0 dependencies. Set `PYSPARK_PYTHON` to the absolute path of the virtual environment's
Python executable so executor imports resolve the installed project. Run:

```bash
python -m ruff format --check .
python -m ruff check .
python -m mypy
python -m pytest
python -m build --wheel
```

`SPARK_TEST_IVY` can point to an existing Delta dependency cache. Spark requires localhost socket
access. The test asserts UTC timestamps inside Spark to avoid Python's local-time conversion of
collected timestamp values.

## Check results

- Full suite: **143 passed** in 98.47 seconds, including Gold, Silver, and streaming integration tests.
- Total coverage: **96.86%**, above the 90% gate; Gold module coverage: 97%.
- Ruff format/lint, mypy, and `git diff --check`: passed.
- Deployment wheel: built successfully and contains the Gold module and CLI entry point.

## Observed fixture results

At hour `2026-01-01T12:00:00Z`, the known-contract cohort has:

| Metric | Expected and observed |
| --- | --- |
| Tickets created | 3 |
| Tickets resolved | 2 |
| Open backlog | 1 |
| P1 / P2 backlog | 1 / 0 |
| Median resolution minutes | 37.5 |
| SLA breach rate | 0.5 |
| Escalation rate | 1/3 |
| Reopen rate | 0 |

The exactly-on-deadline resolution is on time. A ticket resolving at 13:00 remains in 12:00
backlog; a ticket created at 13:00 is excluded from 12:00 creations. A future priority event does
not change the historical result. Missing-contract/account dimensions use `unknown`, with null
resolution metrics when there are no resolutions.

Replaying the same hour preserves two dimension rows without duplicates. Rebuilding the hour with
empty inputs removes both rows and preserves the two rows belonging to the adjacent hour.

The wheel build succeeds. The merged bundle configuration passes the installed Databricks CLI's
JSON schema offline. The subsequent authenticated workspace validation, deployment, and job runs are recorded in
[Gold live validation](./gold-live.md).
