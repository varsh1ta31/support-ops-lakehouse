# First leakage-safe training dataset validation

- Target: Databricks Free Edition `dev`, `support_dev.ml.training_dataset`
- Date: 2026-09-30
- [Job run](https://dbc-aa6ccf23-381b.cloud.databricks.com/jobs/59372977434413/runs/282326897568465?o=7474645852800686): succeeded
- Scoring data: existing `gold.ticket_features` snapshot at `2026-01-01 05:00 UTC`
- Outcome cutoff: `2026-09-30 00:00 UTC`
- Boundaries: train before 06:00, validation 06:00–07:00, test 07:00–08:00 UTC on 2026-01-01

The 1,634 active-ticket snapshots had 43 tickets with a later observed resolution. After
excluding already overdue, reopened, and other ineligible cases, the training table contained
**35 distinct tickets**, including **11 breaches**. All 35 belonged to the train period; the
validation and test periods were empty. Read-only query `01f1bd2f-1a89-1091-bab6-624c234c9698`
checked the split and class counts. Query `01f1bd2f-31d0-1d98-b166-201e1a33af9b` found no
duplicate tickets, resolutions before scoring, already overdue rows, or invalid labels.

This validates label construction and persistence, but does not support model comparison. More
historical feature snapshots across separate time periods are needed before training the
logistic-regression and gradient-boosted-tree candidates. The job deliberately does not turn
unresolved tickets into negative examples. See [ADR 0015](../architecture/0015-training-labels.md).

Ruff, mypy, and unit checks passed locally. A Spark/Delta fixture covers chronological splits,
earliest-snapshot selection, censoring, and reopen exclusion; its local execution requires Java,
which is not installed on this machine.
