# Implementation Plan

## Working principles

- Keep all business logic in testable Python modules; Databricks entry points remain thin.
- Make each pipeline idempotent and rerunnable before adding downstream consumers.
- Develop and test locally with small deterministic datasets, then validate Delta, streaming,
  MLflow, and job behavior in Databricks Free Edition.
- Treat ingestion and AI investigation as independent failure domains.
- Default to zero-cost services. Any paid LLM use is opt-in and capped at $1.
- Complete and verify one component before presenting its architectural walkthrough.

## Phase 0 — Resolve contracts and scaffold the repository

Status: complete; bundle deployment and Bronze validation are recorded in
`docs/validation/bronze-ingestion.md`.

Deliverables:

- Python package, dependency groups, lint/test configuration, and initial README
- Typed configuration for local, `dev`, and `prod` environments
- Central source/target schemas and table-name conventions
- Databricks bundle skeleton and resource-file layout
- Architecture decision log

Decisions to record:

- Use `support_<env>.ops.invalid_records` as the canonical quarantine table; expose a
  compatibility view in `silver` only if useful.
- Store timestamps in UTC and make the event lateness tolerance configurable.
- Use deterministic IDs/seeds for generated data and pipeline-run IDs for traceability.
- Separate reusable transforms from Spark/Databricks job entry points.

Exit gate:

- Package installs, static checks pass, tests can run locally, and the Databricks bundle
  validates once workspace authentication is available.

## Phase 1 — Synthetic data and batch Bronze ingestion

Status: complete; deterministic data generation and Bronze batch ingestion are deployed and
verified in Databricks Free Edition. Silver transformations are tracked in Phase 2.

Deliverables:

- Configurable, deterministic generators for products, accounts, contracts, and tickets
- Referentially consistent sample and scale profiles
- Injected-invalid-data profile for quality testing
- Catalog/schema/bootstrap code
- Incremental Bronze ingestion with ingestion metadata and record hashes
- Bronze table creation for tickets, accounts, contracts, and products

Exit gate:

- Reprocessing identical input produces no duplicate logical records.
- Generated relationships and configured distributions pass tests.
- Raw source values and provenance remain inspectable.

## Phase 2 — Silver entities, quality, and ticket state

Status: complete; the live Silver job is verified in Databricks Free Edition
(`docs/validation/silver-and-streaming.md`). Shared normalization/validation for all five
source entities and the accepted/duplicate/quarantined result model are implemented and verified
locally. Canonical
quarantine rows preserve raw payloads and source metadata, with stable record IDs. See
`docs/architecture/0005-silver-validation.md` for policies and limitations. Retry-safe Silver,
quarantine, and quality-metric persistence (ADR 0006) passes local Spark 4.0/Delta 4.0
integration tests covering replay, partial failure, and empty snapshots. Event ordering, ticket
transitions, and historical/point-in-time state reconstruction are implemented and verified
locally against Delta (ADR 0007). Ticket state carries resolution-SLA fields from the contract in
force at ticket creation for the current priority (ADR 0008), verified locally against Delta.

Deliverables:

- Reusable validation result model: accepted, rejected duplicate, or quarantined
- Normalization and validation for tickets, accounts, contracts, products, and events
- Quarantine persistence with reason, payload, run ID, and detection timestamp
- Ticket-event ordering and deduplication
- Current ticket-state reconstruction from historical tickets plus ordered events
- Active-contract/SLA selection based on the relevant event or ticket timestamp
- Data-quality metric output

Exit gate:

- Unit tests cover every stated quality rule and ticket transition.
- A known event sequence produces the expected current ticket state.
- Invalid input is retained and queryable without blocking valid records.

## Phase 3 — File-based streaming ingestion

Status: complete (ADR 0009). The deterministic event-file producer, the `availableNow`
Structured Streaming Bronze job, the per-microbatch metrics, and Silver quarantine of malformed
lines are verified live in Databricks Free Edition. That covers checkpoint restart, duplicates, late
labelling, and malformed capture (`docs/validation/silver-and-streaming.md`). They also pass local
Spark 4.0/Delta 4.0 tests. Those tests cover checkpoint restart,
a crash between the Bronze and metrics writes, cross-microbatch duplicates, late labelling
against the watermark, and future-dated events. The Silver job now validates streamed events
before rebuilding ticket state.

Deliverables:

- Configurable incremental event-file generator
- Structured Streaming Bronze ingestion with explicit schema and checkpointing
- Event-time watermarking, late-event classification, malformed-record capture, and
  cross-microbatch deduplication
- Restart/recovery integration test
- Streaming progress metrics

Exit gate:

- Batch A is processed once, the stream restarts, and batch B is processed without
  duplicating A.
- Duplicate, malformed, and late events follow documented handling rules.
- Downstream failure does not stop raw event ingestion.

## Phase 4 — Gold operational data products

Status: complete. The first slice implements hourly `gold.support_operations`, with
point-in-time reconstruction, explicit metric contracts, atomic per-hour replacement, and a
parameterized bundle job (ADR 0010). Customer health snapshots (ADR 0011) and incident signals
with a seven-day matching-hour baseline (ADR 0012) are also implemented. All three Gold jobs are
deployed and verified in the dev workspace (`docs/validation/gold-live.md`). The full local suite
passed 145 tests with 95.59% coverage before the feature slice. Point-in-time ticket feature
snapshots are implemented (ADR 0013), deployed and verified in dev
(`docs/validation/ticket-features.md`). A Spark/Delta fixture covers historical boundaries and
replay; its local run needs Java, which is unavailable in the current environment.

Deliverables:

- Hourly support-operations metrics
- Customer-support-health metrics
- Product/time-window incident signals with an explicit baseline method
- Point-in-time ticket feature snapshots
- Data-product contracts and integration fixtures

Key implementation requirement:

- Hourly backlog and historical ML features must be computed as-of the metric/scoring
  timestamp, not from today's latest ticket state. This avoids incorrect historical metrics
  and target leakage.

Exit gate:

- Known fixtures reproduce expected aggregate, rolling-window, SLA, and feature values.
- Gold writes are idempotent at their declared grains.

## Phase 5 — Spark performance experiment

Status: complete. The one-million-event dev benchmark compared four join plans in forward and
reverse order; it measured severe key skew and found the serverless default broadcast plan about
twice as fast as a forced sort-merge join. See `docs/validation/performance-results.md` and
ADR 0014. Serverless does not expose task/shuffle/spill counters to this Python runner or allow
an AQE-off configuration.

Deliverables:

- Skew-configurable event generator and benchmark runner
- Baseline, AQE, broadcast, and salted join configurations
- Captured query plans and available runtime/shuffle/task/spill metrics
- `performance-results.md` populated from observed runs

Exit gate:

- The report identifies a measured bottleneck and supports the chosen optimization with
  before/after evidence. Dataset size stops growing once the behavior is observable.

## Phase 6 — SLA-risk ML lifecycle

Status: in progress. `ml.training_dataset` joins earlier Gold features to later Silver outcomes
(ADR 0015). Historical feature backfill (ADR 0016) supplies 9,904 point-in-time feature rows
and 8,433 labeled examples across chronological train, validation, and test periods. Both model
candidates are trained and tracked in MLflow (ADR 0017). The original random-label cohort failed
the out-of-time gate. An isolated synthetic risk-signal cohort (ADR 0018) in `support_risk_dev`
passed it with test PR-AUC 0.4115 and ROC-AUC 0.8021
(`docs/validation/risk-signal-retraining.md`). Unity Catalog registration and batch scoring are
the next dependencies. This demonstrates a designed synthetic relationship, not real-world
model validity.

Deliverables:

- Leakage-safe, point-in-time training dataset
- Time-based train/validation/test split
- Logistic-regression baseline and gradient-boosted-tree candidate
- MLflow parameters, dataset/version, metrics, and artifacts
- Acceptance policy and Unity Catalog registration where Free Edition supports it
- Batch scorer and `gold.ticket_risk_scores`

Exit gate:

- Both candidates are evaluated using precision, recall, F1, PR-AUC, and ROC-AUC.
- Every prediction carries the model version and scoring timestamp.
- Rejected or unavailable new models leave the last successful scoring path intact.

## Phase 7 — AI investigation briefs

Deliverables:

- High-risk-ticket context builder with bounded recent activity
- Versioned prompt and validated structured response schema
- Clear separation of observed facts, predicted risk, hypotheses, and recommendations
- Provider abstraction plus a deterministic fake provider for tests
- Persisted investigation results and graceful failure behavior

Exit gate:

- Only eligible high-risk tickets are submitted.
- Invalid model output is rejected or safely normalized.
- AI failure cannot block ticket data, scoring, or downstream operations.

## Phase 8 — Orchestration, monitoring, and CI/CD

Deliverables:

- Lakeflow jobs for batch, transforms, Gold, scoring, AI, and separate training
- Retry and dependency policies matching failure-domain boundaries
- Pipeline-run, quality, streaming, Spark, and ML monitoring tables/views
- Unit, integration, recovery, and deployment-validation test suites
- GitHub Actions for lint, tests, and bundle validation
- Controlled `dev` and `prod` bundle targets and deployment workflow
- Cost ledger and cleanup instructions
- Databricks-native demo dashboard for backlog, SLA trends, incident signals, customer health,
  ticket risk, and investigation briefs as their tables become available
- Guided, non-engineer walkthrough linking each dashboard result to its Gold table, Silver state,
  Bronze source, job run, and quality/monitoring evidence

Exit gate:

- A clean environment can be deployed reproducibly from version-controlled configuration.
- A deliberate failure is observable, scoped, and recoverable.
- All specification acceptance criteria are mapped to automated evidence or a documented
  Databricks demonstration.
- A person unfamiliar with the code can open the dashboard without editing SQL, identify a
  support issue, trace its data through Bronze, Silver, and Gold, and explain which Databricks
  job produced each result.

## Delivery rhythm

For each component:

1. Implement the smallest complete vertical slice.
2. Run its unit/integration checks and retain evidence.
3. Update acceptance-criteria status and architecture decisions.
4. Provide the requested post-build walkthrough covering purpose, behavior, design choices,
   tradeoffs, verification, and the next dependency.

The next implementation slice is Phase 6 model registration and batch scoring. Keep the
[guided demo](./docs/demo/README.md) current as each later product is delivered; finish and
validate the dashboard in Phase 8.
