# Support Operations Lakehouse + AI Triage

A governed Databricks lakehouse for support-ticket operations, near-real-time ticket state,
SLA-breach prediction, operational analytics, and grounded AI investigation briefs.

The project is designed for Databricks Free Edition and has a total external-infrastructure
budget below $5.

## Status

Synthetic data generation and Bronze batch ingestion are deployed and verified in Databricks
Free Edition. The engine-neutral Silver validation layer is implemented and tested locally.
Silver/quarantine persistence and quality metrics are implemented with a retry-safe staging layer.
Ticket state is reconstructed from historical tickets plus ordered events, including point-in-time
state, with resolution-SLA fields from the contract in force at ticket creation. Ticket events
stream from incremental files into Bronze with checkpointing, deduplication, late-event labelling,
and malformed-line capture. Silver and streaming are verified live in Free Edition
([evidence](./docs/validation/silver-and-streaming.md)). Hourly Gold support metrics,
customer health snapshots, and product incident signals are deployed and verified in dev
([evidence](./docs/validation/gold-live.md)). Point-in-time ticket feature snapshots are
deployed and verified in dev ([evidence](./docs/validation/ticket-features.md)).
The [Spark performance experiment](./docs/validation/performance-results.md) is also deployed
and measured in dev. Phase 6 historical features and the leakage-safe training dataset are
deployed and verified with nonempty chronological splits
([evidence](./docs/validation/historical-backfill.md)). Both Phase 6 model candidates are
[trained and evaluated in MLflow](./docs/validation/model-training.md). The original random-label
cohort was rejected; an [isolated synthetic risk-signal cohort](./docs/validation/risk-signal-retraining.md)
passed the same out-of-time gate. Registration and batch scoring are next.

## Documentation

- [Product and technical specification](./Support%20Operations%20Lakehouse%20%2B%20AI%20Triage%20%E2%80%94%20Product%20%26%20Technical%20Specification.md)
- [Implementation plan](./IMPLEMENTATION_PLAN.md)
- [Guided Databricks demo](./docs/demo/README.md)
- [Architecture decisions](./docs/architecture/README.md)
- [Contributing guide](./CONTRIBUTING.md)
- [Security policy](./SECURITY.md)

## Planned stack

- Databricks Free Edition and serverless compute
- Apache Spark and PySpark
- Delta Lake and Unity Catalog
- Spark Structured Streaming
- MLflow
- Lakeflow Jobs
- Databricks Declarative Automation Bundles
- pytest and GitHub Actions

## Local development

Python 3.11 or 3.12 is recommended. Create an isolated environment and run the checks:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev,spark-test]'
make check
```

The full checks include local Spark/Delta integration tests and require Java 17 or newer
(Java 21 is used in CI). The first integration run downloads Delta JVM dependencies. For a fast
engine-neutral check without Java, install `.[dev]` and run `pytest tests/unit --no-cov`.

Configuration is layered from `config/base.toml` and `config/<environment>.toml`. Select a
logical environment with `SUPPORT_OPS_ENV=dev` or `SUPPORT_OPS_ENV=prod`; secrets are never part
of application configuration.

## Databricks bundle

The bundle defines separate `dev` and `prod` targets. Authenticate without putting credentials
in the repository, then validate the selected target:

```bash
databricks auth login --host <workspace-url>
databricks bundle validate --target dev
```

The deployment resource files are intentionally empty until their corresponding executable
component exists. This keeps deployed configuration honest rather than publishing placeholder
jobs that cannot run.

## Generate synthetic batch data

Generator profiles live in `config/generation`. The demo profile writes 100 customers, their
priority-specific contracts, six products, and 10,000 historical tickets:

```bash
support-ops-generate \
  --profile config/generation/demo.toml \
  --output data/generated/demo
```

Use `--overwrite` to intentionally replace generator-managed files. Generated outputs are ignored
by Git and include a manifest containing row counts, configuration, and SHA-256 checksums.

Available profiles:

- `demo.toml`: quick representative dataset with intentional customer and product skew
- `risk_demo.toml`: isolated 2025 ML cohort with a documented priority/tier risk pattern
- `quality.toml`: small dataset plus isolated known-invalid tickets for quarantine tests
- `scale.toml`: one-million-ticket performance and ingestion profile

`events_per_ticket` is recorded now so the same profiles can drive the event generator in the
streaming component; this batch component does not produce ticket events yet.

## Run Bronze batch ingestion

Generate data locally, copy each CSV into its corresponding governed landing directory, and run
the bundle job:

```text
/Volumes/support_dev/raw/landing/products/
/Volumes/support_dev/raw/landing/accounts/
/Volumes/support_dev/raw/landing/contracts/
/Volumes/support_dev/raw/landing/tickets/
```

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run --target dev batch_ingestion
```

The first job task idempotently creates the catalog, schemas, and managed Volumes. Four parallel
tasks then ingest the reference and historical CSV files into Bronze Delta tables. Source values
remain strings in Bronze so malformed values are preserved for Silver validation. Each record
also receives its ingestion time, source file, pipeline run ID, and deterministic content hash.

Rerunning the job is safe: Delta `MERGE` inserts only previously unseen record hashes.

## Run Silver validation and persistence

After a successful Bronze batch job, run:

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run --target dev silver_transformations
```

The job validates products, accounts, contracts, tickets, then streamed ticket events; writes typed Silver rows and
`ops.invalid_records`; and records one `ops.quality_metrics` row per entity and job run. It then
rebuilds `silver.ticket_state` as of the run start, including SLA deadline and minutes remaining
(see [ticket state](./docs/architecture/0007-ticket-state.md) and
[SLA selection](./docs/architecture/0008-contract-sla.md)).
Retries reuse durable decisions in `ops.silver_evaluations`. New runs process new Bronze snapshots.
Accepted natural keys are insert-only: changed records with an existing key count as duplicates.
Keep manual runs serialized with the scheduled job. See [the persistence decision](./docs/architecture/0006-silver-persistence.md)
for recovery behavior, metric definitions, and the first-accepted record policy.

## Stream ticket events

The producer appends deterministic event files to the `raw.ticket_events` Volume. The stream job
lands every new file in `bronze.ticket_events`, then stops (`availableNow`). Run it on a schedule
for continuous ingestion; it resumes from its checkpoint.

```bash
databricks bundle run --target dev event_producer
databricks bundle run --target dev event_stream_ingestion
databricks bundle run --target dev silver_transformations
```

Locally, `support-ops-generate-events --profile config/generation/stream.toml --output <dir>
--batches 4` writes the same files. Per-microbatch counts, watermarks, and rates are in
`ops.streaming_metrics`. See [the streaming decision](./docs/architecture/0009-event-streaming.md)
for duplicate, late, and malformed handling and for checkpoint resets.

## Run hourly Gold support metrics

After bootstrap and Silver complete, build and deploy the updated wheel, then supply an explicit
UTC hour to aggregate (the interval includes its start and excludes the next hour):

```bash
python -m build --wheel
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run --target dev gold_support_operations --params hour=2026-01-01T12:00:00Z
```

The output is `gold.support_operations`, grouped by hour, product, contract support tier, and
customer segment. It includes creations, resolutions, end-of-hour backlog, priority counts,
median resolution time, and SLA/escalation/reopen rates. Historical metrics reconstruct state
from Silver history. Rerunning an hour atomically replaces its rows, including stale groups;
other hours are preserved. Replay affected hours after late events arrive. See
[metric definitions and limitations](./docs/architecture/0010-hourly-support-operations.md).
The job is unscheduled; its dev workspace run is recorded in the
[Gold live validation](./docs/validation/gold-live.md).
[Local verification evidence](./docs/validation/gold-support-operations.md) records the fixture
results and full regression checks.

## Run customer support health

After Silver finishes, provide the UTC snapshot hour:

```bash
databricks bundle run --target dev gold_customer_health --params as_of=2026-01-01T12:00:00Z
```

`gold.customer_support_health` contains one row per accepted account at that scoring point.
It reports active backlog, 30-day ticket volume, 90-day breach/escalation/reopen rates, and mean
resolution time. Rebuilding an `as_of` replaces that complete snapshot and leaves neighboring
snapshots intact. See [metric contracts and limitations](./docs/architecture/0011-customer-support-health.md).
The job is unscheduled; its dev workspace run is recorded in the
[Gold live validation](./docs/validation/gold-live.md).
[Local verification evidence](./docs/validation/customer-support-health.md) records the
fixture and regression results.

## Run product incident signals

After Silver finishes, supply the UTC hour to evaluate:

```bash
databricks bundle run --target dev gold_incident_signals --params hour=2026-01-01T12:00:00Z
```

`gold.incident_signals` reports created-ticket volume, affected customers, P1 tickets,
escalations, a mean of the seven previous matching UTC hours, and the deviation. The signal
requires at least five tickets, three customers, and double the baseline. Rebuilding an hour
replaces all its product rows while preserving other hours. See
[baseline and signal definitions](./docs/architecture/0012-product-incident-signals.md).
The job is unscheduled; its dev workspace run is recorded in the
[Gold live validation](./docs/validation/gold-live.md).
[Local verification evidence](./docs/validation/product-incident-signals.md) records the
fixture and regression results.

## Build ticket feature snapshots

Run `gold_ticket_features` after Silver with an explicit UTC scoring hour, for example:

```bash
databricks bundle run --target dev gold_ticket_features --params as_of=2026-01-01T12:00:00Z
```

`gold.ticket_features` contains one row per active ticket at that point. It includes ticket age,
SLA time remaining, observed event counts, and customer/product peer rates from history strictly
before the scoring hour. The final `sla_breached` label remains null until training joins a later
observed outcome. Rerunning an hour replaces its complete snapshot. See the
[feature contract](./docs/architecture/0013-ticket-feature-snapshots.md) for cohort definitions.

## Run the Spark performance experiment

```bash
databricks bundle run --target dev spark_performance --params events=1000000,order=forward
```

The job generates skewed events in Spark, compares forced merge, default adaptive planning,
explicit broadcast, and salted merge joins, and stores elapsed times and physical plans in
`ops.spark_performance_runs`. Use `order=reverse` for an order-bias check. The
[measured results](./docs/validation/performance-results.md) explain why the default broadcast
plan is preferred for this synthetic workload.

## Build the labeled training dataset

Backfill the pre-event ticket history, then build the separate training dataset:

```bash
databricks bundle run --target dev ml_historical_features --params before=2026-01-01T00:00:00Z
databricks bundle run --target dev ml_training_dataset --params label_as_of=2026-09-30T00:00:00Z,train_end=2025-10-01T00:00:00Z,validation_end=2025-11-01T00:00:00Z,test_end=2026-01-01T00:00:00Z
```

The backfill writes one next-hour snapshot per still-active historical ticket in 2025, before
ticket events begin. The training job uses only features visible at scoring and an outcome
observed later. Unresolved, reopened, and already overdue tickets are excluded. The dev dataset
has 6,293 train, 735 validation, and 1,405 test tickets; see the
[validation note](./docs/validation/historical-backfill.md).

## Compare SLA-risk models

```bash
databricks bundle run --target dev ml_train_models --params label_as_of=2026-09-30T00:00:00Z
```

The job fits logistic regression and a gradient-boosted tree, logs each complete model pipeline
and evaluation to MLflow, and chooses a candidate using validation PR-AUC. Test performance is
an acceptance gate. The current dev data fails that gate, so no model is registered or scored;
see the [measured results](./docs/validation/model-training.md).

For the deliberately learnable cohort, use the isolated `risk_dev` bundle target and
`config/generation/risk_demo.toml`. Generate its CSVs, place them in the matching
`support_risk_dev.raw.landing` Volume directories, then run `batch_ingestion`,
`silver_transformations`, `ml_historical_features`, `ml_training_dataset`, and
`ml_train_models` with `--target risk_dev` and the same timestamps shown above. Its selected
model [passed the acceptance gate](./docs/validation/risk-signal-retraining.md); the result is
specific to synthetic data, and it has not yet been registered or used for scoring.

## Repository layout

```text
config/                  Environment configuration
docs/architecture/       Architecture decision records
resources/               Databricks bundle resources
src/support_ops/          Testable application package
tests/unit/               Fast engine-neutral tests
tests/integration/        Spark and pipeline integration tests
```

Generated data, model artifacts, local checkpoints, and credentials must not be committed.

## Cost policy

Development defaults to no-cost local tooling and Databricks Free Edition. Paid integrations
must be explicitly approved, tightly bounded, and recorded against the project budget.
