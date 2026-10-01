# Synthetic risk-signal cohort and retraining — dev validation

- Date: 2026-09-30
- Isolated target/catalog: `risk_dev` / `support_risk_dev`
- Generator: `config/generation/risk_demo.toml`, seed 42, manifest version 2, 10,000 tickets
- [Bronze run](https://dbc-aa6ccf23-381b.cloud.databricks.com/jobs/147096408604741/runs/1023981278399675?o=7474645852800686): 6 products, 100 accounts, 400 contracts, 10,000 tickets ingested
- [Silver run](https://dbc-aa6ccf23-381b.cloud.databricks.com/jobs/1086684686261775/runs/137133908363504?o=7474645852800686): all 10,000 tickets accepted; no quarantine rows
- [Feature backfill](https://dbc-aa6ccf23-381b.cloud.databricks.com/jobs/380808955196979/runs/437816829603185?o=7474645852800686): succeeded
- [Training dataset](https://dbc-aa6ccf23-381b.cloud.databricks.com/jobs/582729076859289/runs/902349347397729?o=7474645852800686): succeeded
- [Model comparison](https://dbc-aa6ccf23-381b.cloud.databricks.com/jobs/837794626708831/runs/963655993639616?o=7474645852800686): succeeded

The ticket CSV checksum is `f4416de3c90d92036a0f4a51cc66b52a6b23a5df16b98eb16d5410e222ce26e0`.
Read-only statement `01f1bd45-e76a-1abf-a7d2-7b1839b59504` reconstructed breach labels from
Silver resolution timestamps and contract deadlines. Among resolved tickets, P1 had **228/430**
breaches (53.0%) and P4 had **81/2,230** (3.6%). Statement
`01f1bd46-a057-16a6-a728-84dd0c002b8a` found **9,924 distinct** historical scoring rows.

| Split | Tickets | Breaches |
| --- | ---: | ---: |
| Train | 6,307 | 905 |
| Validation | 736 | 119 |
| Test | 1,411 | 213 |

Read-only statement `01f1bd46-a145-17ba-b814-48171e6cb5bf` checked those split counts.
Both candidates used `support_risk_dev.ml.training_dataset` Delta version **1**, selected
thresholds on validation F1, and kept the test period untouched until evaluation.

| Candidate | Period | Precision | Recall | F1 | PR-AUC | ROC-AUC |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Logistic regression | Validation | 0.4438 | 0.6639 | 0.5320 | 0.4173 | 0.7935 |
| Logistic regression | Test | 0.4131 | 0.5915 | 0.4865 | 0.4115 | 0.8021 |
| Gradient-boosted tree | Validation | 0.4346 | 0.6975 | 0.5355 | 0.4104 | 0.7864 |
| Gradient-boosted tree | Test | 0.4136 | 0.6291 | 0.4991 | 0.3947 | 0.7819 |

Validation PR-AUC selected logistic regression. Its test PR-AUC **0.4115** is 2.73 times the
test breach prevalence of **0.1510**, and it passed every [acceptance criterion](../architecture/0017-ml-model-comparison.md).
The [selected MLflow run](https://dbc-aa6ccf23-381b.cloud.databricks.com/ml/experiments/3936596238837349/runs/c2a6322316134f5dbc28f33d8df0631c)
is tagged `deployment_decision=accepted_for_registration`. API inspection confirmed its model
artifact, training manifest, Delta version, threshold, and all test metrics. The
[tree run](https://dbc-aa6ccf23-381b.cloud.databricks.com/ml/experiments/3936596238837349/runs/a55cce3fde404df4bca50f9106cafd82)
is retained for comparison. Registration and batch scoring are the next lifecycle component.

This result is expected for the designed synthetic relationship. It establishes that the
point-in-time pipeline can detect an intended signal and that its acceptance gate distinguishes
this cohort from the original random-label cohort; it does not validate real-world forecasting.
