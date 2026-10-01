# SLA-risk model comparison — dev validation

- Target: Databricks Free Edition `dev`
- Date: 2026-09-30
- [Training job run](https://dbc-aa6ccf23-381b.cloud.databricks.com/jobs/421073023401335/runs/839337913031788?o=7474645852800686): succeeded
- MLflow experiment: `/Users/varshitaravi@yahoo.com/support-ops-support_dev-sla-risk`
- Training table: `support_dev.ml.training_dataset`, Delta version **2**, outcome cutoff
  `2026-09-30T00:00:00Z`
- Chronological cohorts: 6,293 train, 735 validation, 1,405 test tickets

The fixed 0.50 threshold predicted no breaches for either candidate. The final run instead
selected each threshold by validation F1, then used it unchanged on the test period. PR-AUC and
ROC-AUC use predicted probabilities, independent of the threshold.

| Candidate | Period | Precision | Recall | F1 | PR-AUC | ROC-AUC |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Logistic regression | Validation | 0.1262 | 0.7711 | 0.2169 | 0.1172 | 0.5161 |
| Logistic regression | Test | 0.1335 | 0.7200 | 0.2252 | 0.1296 | 0.5191 |
| Gradient-boosted tree | Validation | 0.2093 | 0.2169 | 0.2130 | 0.1519 | 0.5238 |
| Gradient-boosted tree | Test | 0.1143 | 0.0914 | 0.1016 | 0.1168 | 0.4683 |

The tree had the higher validation PR-AUC and was selected for acceptance review. Its test
PR-AUC **0.1168** was below the test breach prevalence of **0.1246**. All four minimum test
criteria failed, so the job tagged its [MLflow run](https://dbc-aa6ccf23-381b.cloud.databricks.com/ml/experiments/2884441088037765/runs/e0d4a10d34b445139c0a1d3e572b97b5)
`deployment_decision=rejected`. No model was registered or used for risk scoring. The
[logistic run](https://dbc-aa6ccf23-381b.cloud.databricks.com/ml/experiments/2884441088037765/runs/5c581075b5434e88bde784589af034d3)
is retained as the baseline.

Read-only MLflow API inspection confirmed the selected run contains all ten validation/test
metrics, hyperparameters, the dataset version, rejection reasons, `training_manifest.json`,
and the model artifact. Local Ruff, mypy, and unit checks pass. The generator currently uses an
independent random breach draw (`ticket_records` in `src/support_ops/synthetic/generate.py`),
which explains the near-chance discrimination; see [ADR 0017](../architecture/0017-ml-model-comparison.md).
