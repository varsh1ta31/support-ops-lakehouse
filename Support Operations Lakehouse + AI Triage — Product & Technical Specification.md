# Support Operations Lakehouse + AI Triage

## 1. Overview

### Product

Support Operations Lakehouse + AI Triage

### Objective

Build a data and AI platform that consolidates support ticket activity, customer data, contracts, product information, and incident signals into a governed lakehouse.

The system provides:

- near-real-time support operations data
- current ticket state
- SLA-risk prediction
- operational analytics
- AI-assisted incident investigation

The implementation uses Databricks, Delta Lake, PySpark, Structured Streaming, MLflow, Unity Catalog, Lakeflow Jobs, and CI/CD.

### Cost constraint

Total infrastructure spend must remain below:

**$5**

The primary development environment is **Databricks Free Edition**.

AWS services are optional and should only be used where they demonstrate a capability that cannot reasonably be demonstrated within the free environment.

---

# 2. Problem

A B2B software company operates a customer-support organization handling thousands of tickets across multiple products and customer tiers.

Information needed to investigate a support issue is distributed across:

- support ticket history
- ticket event activity
- customer/account information
- support contracts and SLAs
- product ownership information
- operational incident data

Support teams need to quickly identify:

1. which tickets are at risk of SLA breach
2. which issues require escalation
3. whether multiple tickets indicate a broader product incident
4. which customers and services are affected
5. what evidence an engineer should investigate next

Existing reporting is largely batch-oriented and fragmented across operational systems.

---

# 3. Users

## Support Engineer

Needs:

- current ticket context
- SLA status
- customer context
- risk indicators
- related incident signals
- investigation recommendations

## Support Operations Manager

Needs:

- backlog visibility
- SLA-breach trends
- escalation trends
- product-level issue patterns
- customer-level support health

## Data / ML Platform Team

Needs:

- reliable ingestion
- governed datasets
- reusable transformations
- data-quality controls
- reproducible ML training
- pipeline monitoring
- automated deployment

---

# 4. Functional Requirements

The platform must:

### FR-01 — Batch ingestion

Ingest historical and reference datasets incrementally.

Sources:

- historical tickets
- accounts
- contracts
- product metadata

---

### FR-02 — Streaming ingestion

Process continuously arriving support-ticket events.

Examples:

- ticket created
- ticket assigned
- priority changed
- customer replied
- agent replied
- status changed
- engineering escalation
- ticket resolved
- ticket reopened

---

### FR-03 — Lakehouse storage

Persist datasets as Delta tables organized into:

- Bronze
- Silver
- Gold

---

### FR-04 — Data transformation

Use PySpark for:

- filtering
- joins
- aggregations
- window functions
- deduplication
- state reconstruction
- feature engineering

---

### FR-05 — Data quality

Detect and appropriately handle:

- invalid IDs
- duplicates
- malformed records
- impossible timestamps
- invalid priorities
- missing required fields
- referential-integrity failures

---

### FR-06 — Operational analytics

Generate Gold datasets supporting:

- ticket backlog
- ticket volume
- SLA breach rate
- escalation rate
- resolution time
- reopen rate
- product incident volume
- customer support health

---

### FR-07 — Risk prediction

Train an ML model predicting:

> Probability that an open ticket will breach its SLA.

---

### FR-08 — AI investigation

Generate structured investigation briefs for high-risk tickets using an LLM.

The LLM must separate:

- observed facts
- model-generated risk factors
- hypotheses
- recommended checks

---

### FR-09 — Orchestration

Schedule and coordinate batch, transformation, feature-generation, training, and scoring workflows.

---

### FR-10 — Monitoring

Capture:

- pipeline failures
- pipeline duration
- data-quality results
- streaming progress
- Spark performance
- model metrics

---

### FR-11 — CI/CD

Automatically validate, test, and deploy the project from Git.

---

# 5. Non-Functional Requirements

## Reliability

Pipelines must:

- tolerate duplicate input
- recover from interruption
- support reruns without corrupting data
- retain invalid records for diagnosis
- isolate raw ingestion from downstream transformation failures

## Maintainability

Transformation code should be modular and testable outside notebooks.

## Observability

Failures must provide sufficient information to determine:

- which component failed
- which run failed
- when it failed
- what data was affected

## Reproducibility

ML experiments and deployed resources must be reproducible from version-controlled code and configuration.

## Cost

Infrastructure spend must remain below $5.

---

# 6. Architecture

```text
                  SOURCE DATA
                       │
       ┌───────────────┴────────────────┐
       │                                │
       │                                │
 Historical / Reference Data      Ticket Event Generator
       │                                │
       │                         incremental event files
       │                                │
       ▼                                ▼
 Databricks Volume                Databricks Volume
 / landing storage               / stream source
       │                                │
       │                         Structured Streaming
       │                                │
       └──────────────┬─────────────────┘
                      ▼

                 BRONZE DELTA
         ┌───────────────────────────┐
         │ bronze.tickets            │
         │ bronze.ticket_events      │
         │ bronze.accounts           │
         │ bronze.contracts          │
         │ bronze.products           │
         └──────────────┬────────────┘
                        │
                        │ PySpark
                        ▼
                  SILVER DELTA
         ┌───────────────────────────┐
         │ silver.tickets            │
         │ silver.ticket_events      │
         │ silver.accounts           │
         │ silver.contracts          │
         │ silver.ticket_state       │
         │ silver.invalid_records    │
         └──────────────┬────────────┘
                        │
               joins / windows /
                 aggregations
                        │
                        ▼
                   GOLD DELTA
         ┌───────────────────────────┐
         │ gold.support_operations   │
         │ gold.ticket_features      │
         │ gold.customer_health      │
         │ gold.incident_signals     │
         └──────────────┬────────────┘
                        │
              ┌─────────┴─────────┐
              │                   │
              ▼                   ▼
            MLflow          AI Investigation
              │                   │
       SLA Risk Model              │
              │                   │
              └─────────┬─────────┘
                        ▼
                Ticket Triage
```

---

# 7. Technology Stack

| Capability             | Technology                                                |
| ---------------------- | --------------------------------------------------------- |
| Compute                | Databricks Serverless                                     |
| Distributed processing | Apache Spark / PySpark                                    |
| Table format           | Delta Lake                                                |
| Governance             | Unity Catalog                                             |
| Batch ingestion        | PySpark / Auto Loader where supported                     |
| Streaming              | Spark Structured Streaming                                |
| Storage                | Databricks managed storage / Volumes                      |
| Workflow orchestration | Lakeflow Jobs                                             |
| ML tracking            | MLflow                                                    |
| Model registry         | Unity Catalog                                             |
| AI investigation       | Databricks-hosted model endpoint or low-cost external API |
| Source control         | GitHub                                                    |
| Testing                | pytest                                                    |
| CI                     | GitHub Actions                                            |
| Deployment             | Databricks Declarative Automation Bundles                 |

Databricks Free Edition currently provides serverless compute rather than configurable traditional clusters. citeturn665762search3turn665762search5

---

# 8. Data Domains

## 8.1 Historical Tickets

Grain:

**one record per historical support ticket**

Schema:

```text
ticket_id STRING
customer_id STRING
product_id STRING
created_at TIMESTAMP
closed_at TIMESTAMP
priority STRING
channel STRING
category STRING
subject STRING
description STRING
resolution STRING
final_status STRING
escalated BOOLEAN
sla_breached BOOLEAN
agent_id STRING
```

Target generated volume:

**1–3 million tickets**

---

# 9. Ticket Events

Grain:

**one record per change or interaction associated with a ticket**

Schema:

```text
event_id STRING
ticket_id STRING
customer_id STRING
product_id STRING
event_type STRING
event_time TIMESTAMP
agent_id STRING
old_value STRING
new_value STRING
payload STRING
```

Event types:

```text
ticket_created
agent_assigned
priority_changed
status_changed
customer_replied
agent_replied
engineering_escalated
ticket_resolved
ticket_reopened
```

---

# 10. Accounts

Schema:

```text
customer_id STRING
customer_name STRING
segment STRING
region STRING
industry STRING
annual_contract_value DOUBLE
support_tier STRING
account_status STRING
```

Segments:

```text
SMB
Mid-Market
Enterprise
Strategic
```

---

# 11. Contracts

Schema:

```text
contract_id STRING
customer_id STRING
support_tier STRING
priority STRING
response_sla_minutes INTEGER
resolution_sla_minutes INTEGER
effective_from DATE
effective_to DATE
```

---

# 12. Products

Schema:

```text
product_id STRING
product_name STRING
product_family STRING
service_owner STRING
criticality STRING
```

---

# 13. Synthetic Data Generation

A Python generator creates realistic operational data.

The generator should support configurable:

```text
number_of_customers
number_of_tickets
events_per_ticket
date_range
customer_distribution
product_distribution
sla_breach_rate
escalation_rate
```

Relationships must remain logically consistent.

For example:

```text
ticket.customer_id → accounts.customer_id
ticket.product_id → products.product_id
contract.customer_id → accounts.customer_id
event.ticket_id → tickets.ticket_id
```

---

# 14. Streaming Simulation

Ticket events will be produced continuously by a local or Databricks Python process.

Rather than introducing a paid event broker, the producer periodically writes small event batches into a streaming source directory.

Example:

```text
/Volumes/support_dev/raw/ticket_events/
```

Every interval:

```text
events_000001.json
events_000002.json
events_000003.json
...
```

Structured Streaming continuously detects and processes new input.

Architecture:

```text
Python Event Generator
        │
        ▼
incremental JSON files
        │
        ▼
Spark Structured Streaming
        │
        ▼
Bronze Delta
```

This stream must demonstrate:

- continuously arriving data
- checkpointing
- microbatch execution
- deduplication
- event-time processing
- late-arriving events
- malformed records
- restart/recovery

---

# 15. Bronze Layer

Catalog/schema:

```text
support_dev.bronze
```

Tables:

```text
bronze.tickets
bronze.ticket_events
bronze.accounts
bronze.contracts
bronze.products
```

Bronze preserves source data with minimal modification.

Additional ingestion metadata:

```text
_ingested_at
_source_file
_pipeline_run_id
_record_hash
```

Invalid raw input should remain recoverable.

---

# 16. Silver Layer

Catalog/schema:

```text
support_dev.silver
```

Silver represents validated operational entities.

---

## silver.ticket\_events

Processing:

1. parse payload
2. validate schema
3. cast types
4. normalize event types
5. remove duplicate `event_id`
6. validate timestamps
7. quarantine malformed records
8. classify late events

---

## silver.accounts

Processing:

- deduplicate customer IDs
- standardize segment
- standardize region
- validate support tier
- validate contract value

---

## silver.contracts

Processing:

- validate effective-date ranges
- validate SLA values
- normalize priority
- identify active contract

---

## silver.ticket\_state

Represents the current operational state of each ticket.

Schema:

```text
ticket_id
customer_id
product_id
status
priority
assigned_agent
created_at
last_updated_at
support_tier
resolution_sla_minutes
sla_deadline
minutes_open
minutes_to_sla
message_count
reopen_count
escalation_count
```

Ticket state is reconstructed from historical state plus subsequent events.

---

# 17. Data Quality Rules

Examples:

```text
ticket_id IS NOT NULL

event_id IS NOT NULL

customer_id IS NOT NULL

priority IN ('P1', 'P2', 'P3', 'P4')

event_time IS NOT NULL

created_at <= last_updated_at

resolution_sla_minutes > 0

annual_contract_value >= 0
```

Failures fall into three categories.

## Reject

Duplicate event already processed.

## Quarantine

Examples:

- unknown customer
- invalid event type
- future timestamp outside tolerance
- invalid priority

Store in:

```text
support_dev.silver.invalid_records
```

Schema:

```text
record_id
source
raw_payload
failure_reason
detected_at
pipeline_run_id
```

## Pipeline failure

Reserved for conditions indicating structural corruption, such as an unreadable source or incompatible required schema.

---

# 18. Gold Layer

Catalog/schema:

```text
support_dev.gold
```

Gold tables represent reusable business data products.

---

# 19. Gold: Support Operations

Table:

```text
gold.support_operations
```

Grain:

```text
hour
product_id
support_tier
customer_segment
```

Metrics:

```text
tickets_created
tickets_resolved
open_backlog
p1_ticket_count
p2_ticket_count
median_resolution_minutes
sla_breach_rate
escalation_rate
reopen_rate
```

---

# 20. Gold: Ticket Features

Table:

```text
gold.ticket_features
```

Grain:

**one record per ticket at a particular scoring point**

Features:

```text
ticket_id

ticket_age_minutes
minutes_to_sla

priority
support_tier
customer_segment

message_count
customer_message_count
agent_message_count

reopen_count
priority_change_count
previous_escalation_count

customer_ticket_count_30d
customer_breach_rate_90d
customer_escalation_rate_90d

product_ticket_count_24h
product_escalation_rate_24h
product_breach_rate_7d
```

Label:

```text
sla_breached
```

---

# 21. Gold: Customer Support Health

Table:

```text
gold.customer_support_health
```

Schema:

```text
customer_id
support_tier
annual_contract_value

open_ticket_count
p1_ticket_count

ticket_count_30d
breach_rate_90d
escalation_rate_90d
reopen_rate_90d
average_resolution_minutes
```

---

# 22. Gold: Incident Signals

Table:

```text
gold.incident_signals
```

Purpose:

Identify unusual concentrations of support issues.

Example grain:

```text
product_id
time_window
```

Metrics:

```text
ticket_count
unique_customers
p1_count
escalation_count
ticket_volume_baseline
volume_deviation
incident_signal
```

Example:

```text
Payments API

normal:
12 tickets/hour

current:
48 tickets/hour

affected customers:
19

incident_signal:
TRUE
```

---

# 23. PySpark Transformation Requirements

The implementation must include meaningful examples of:

## Filters

```python
open_tickets = tickets.filter(...)
```

## Aggregations

```python
events.groupBy(...).agg(...)
```

## Joins

Examples:

```text
tickets × accounts

tickets × contracts

ticket_events × tickets

tickets × products
```

## Window functions

Use windows for:

- latest ticket status
- ticket-event ordering
- rolling ticket counts
- prior events
- customer historical metrics

## Conditional transformations

Use for:

- SLA state
- priority normalization
- risk-feature creation

---

# 24. Spark Performance Experiment

A dedicated workload must demonstrate diagnosis and optimization of a distributed Spark performance problem.

## Problem

Customer-level event processing experiences data skew.

Synthetic event generation intentionally creates several large customers.

Example:

```text
customer_001 → 35% of events
customer_002 → 15%
remaining customers → 50%
```

A large event table is joined against customer/account data using:

```text
customer_id
```

---

# 25. Performance Dataset

Start at:

```text
1 million events
```

Increase progressively:

```text
1M
5M
10M
```

Stop increasing when the performance behavior becomes observable.

The dataset should not be made larger solely to create an impressive volume number.

---

# 26. Baseline Join

Run:

```python
events.join(accounts, "customer_id")
```

Record:

```text
execution time
shuffle read
shuffle write
partition distribution
task duration
longest task
spill, when observable
```

Inspect available Spark execution metrics and query plans.

---

# 27. Optimization Experiments

Compare at least three configurations.

## Baseline

Standard join.

## Adaptive Query Execution

Observe Spark's automatic optimization behavior.

## Broadcast Join

Broadcast the account dataset when sufficiently small.

```python
events.join(broadcast(accounts), "customer_id")
```

## Salting

When demonstrating manual skew handling:

```text
customer_001

becomes

customer_001_0
customer_001_1
customer_001_2
...
```

Corresponding dimension records are replicated across salt values.

---

# 28. Performance Results

Produce a benchmark artifact:

| Strategy  | Runtime | Shuffle Read | Shuffle Write | Spill | Longest Task |
| --------- | ------: | -----------: | ------------: | ----: | -----------: |
| Baseline  |         |              |               |       |              |
| AQE       |         |              |               |       |              |
| Broadcast |         |              |               |       |              |
| Salted    |         |              |               |       |              |

The selected optimization must be supported by observed measurements.

---

# 29. ML Use Case

Model:

**SLA breach risk prediction**

Question:

> Given the current state and history of a ticket, what is the probability that the ticket will breach its resolution SLA?

Output:

```text
ticket_id
breach_probability
prediction_timestamp
model_version
```

Example:

```text
TKT-48192
0.84
2026-09-22T18:00
v3
```

---

# 30. Training Dataset

Source:

```text
gold.ticket_features
```

Training data must avoid leakage.

Features available only after ticket resolution cannot be used to predict earlier SLA risk.

Use a time-based train/test split.

Example:

```text
training:
Jan–Jun

validation:
Jul

test:
Aug
```

---

# 31. Model Candidates

Initial models:

1. Logistic Regression
2. Gradient-boosted tree model

The simpler model establishes a baseline.

The second model tests whether nonlinear relationships materially improve performance.

---

# 32. Model Metrics

Track:

```text
precision
recall
F1
PR-AUC
ROC-AUC
```

Primary evaluation should emphasize:

- recall
- precision
- PR-AUC

because SLA breaches may represent a minority class.

---

# 33. MLflow

For every experiment, MLflow records:

```text
run_id
model_type
hyperparameters
feature_set
training_period
dataset/version
precision
recall
F1
PR-AUC
ROC-AUC
model_artifact
```

Best acceptable models are registered through Unity Catalog where supported.

---

# 34. Batch Model Scoring

To remain inside the cost budget, the primary deployment pattern is:

```text
Gold ticket features
       ↓
scheduled scoring job
       ↓
MLflow model
       ↓
prediction table
```

Output:

```text
gold.ticket_risk_scores
```

Schema:

```text
ticket_id
breach_probability
risk_level
model_version
scored_at
```

Risk levels:

```text
LOW
MEDIUM
HIGH
```

---

# 35. AI Investigation Layer

Only tickets above the configured risk threshold are sent to the LLM.

Example:

```text
breach_probability >= 0.70
```

Context:

```text
ticket
current ticket state
customer information
SLA information
risk score
feature values
recent ticket activity
product incident signals
```

---

# 36. AI Output Contract

Output must follow a structured schema.

```json
{
  "summary": "",
  "observations": [],
  "risk_factors": [],
  "possible_causes": [],
  "recommended_checks": [],
  "confidence": ""
}
```

Definitions:

### Observations

Facts directly supported by available data.

### Risk factors

Data contributing to elevated model risk.

### Possible causes

LLM-generated hypotheses.

### Recommended checks

Actions the support engineer could use to validate hypotheses.

---

# 37. AI Grounding Rules

The system must not present hypotheses as established facts.

Generated investigation results should clearly distinguish:

```text
OBSERVED
PREDICTED
HYPOTHESIZED
RECOMMENDED
```

The LLM may not modify ticket status or trigger an escalation automatically.

---

# 38. Workflow Orchestration

Use Lakeflow Jobs.

Workflow:

```text
batch_ingestion
      ↓
silver_transformations
      ↓
gold_transformation
      ↓
feature_generation
      ↓
risk_scoring
      ↓
AI investigation
```

Model training runs separately:

```text
feature_generation
      ↓
model_training
      ↓
evaluation
      ↓
model_registration
```

Raw streaming ingestion runs independently.

This prevents a downstream analytics failure from stopping ingestion.

---

# 39. Scheduling

Example:

### Streaming

Continuous while demo/testing is active.

### Reference ingestion

Daily.

### Gold aggregation

Hourly.

### Risk scoring

Hourly.

### Model training

Manual or weekly.

### AI investigation

After scoring high-risk tickets.

These frequencies represent production configuration rather than a requirement to leave workloads running continuously during development.

---

# 40. Failure Handling

## Streaming failure

Recover from checkpoint.

## Duplicate event

Ignore based on:

```text
event_id
```

## Invalid event

Write to quarantine.

## Transformation failure

Fail the relevant job and retain upstream data.

## Temporary task failure

Retry according to configured workflow policy.

## Model failure

Do not generate a new score; retain the last successful model version.

## LLM failure

Ticket remains usable without AI investigation.

The LLM is not on the critical path for data processing.

---

# 41. Monitoring

## Pipeline metrics

Track:

```text
run_status
run_duration
records_read
records_written
failed_records
```

## Streaming metrics

Track where available:

```text
input_rows_per_second
processed_rows_per_second
batch_duration
event_time_delay
```

## Data-quality metrics

Track:

```text
invalid_record_count
invalid_record_rate
duplicate_count
unknown_customer_count
late_event_count
```

## Spark metrics

Track where available:

```text
runtime
shuffle volume
task duration
partition behavior
spill
```

## ML metrics

Track:

```text
precision
recall
PR-AUC
prediction distribution
```

---

# 42. Catalog Structure

Development:

```text
support_dev
├── bronze
├── silver
├── gold
├── ml
└── ops
```

Examples:

```text
support_dev.bronze.ticket_events

support_dev.silver.ticket_state

support_dev.gold.ticket_features

support_dev.gold.ticket_risk_scores

support_dev.ops.invalid_records
```

---

# 43. Environment Strategy

Logical environments:

```text
dev
prod
```

For the <$5 implementation, both may share the same Free Edition workspace while remaining separated through catalog/schema configuration.

Example:

```text
support_dev
support_prod
```

Configuration determines the target environment.

---

# 44. Repository Structure

```text
support-ops-lakehouse/
│
├── README.md
├── pyproject.toml
├── databricks.yml
│
├── resources/
│   ├── ingestion.yml
│   ├── transformations.yml
│   ├── scoring.yml
│   └── training.yml
│
├── src/
│   └── support_ops/
│       │
│       ├── ingestion/
│       │   ├── batch.py
│       │   └── streaming.py
│       │
│       ├── transformations/
│       │   ├── tickets.py
│       │   ├── accounts.py
│       │   └── events.py
│       │
│       ├── quality/
│       │   └── rules.py
│       │
│       ├── features/
│       │   └── ticket_features.py
│       │
│       ├── ml/
│       │   ├── train.py
│       │   └── score.py
│       │
│       └── ai/
│           └── investigate.py
│
├── scripts/
│   ├── generate_dataset.py
│   └── generate_events.py
│
├── tests/
│   ├── unit/
│   └── integration/
│
└── .github/
    └── workflows/
        └── ci.yml
```

---

# 45. CI Pipeline

On pull request:

```text
checkout
   ↓
install dependencies
   ↓
lint
   ↓
unit tests
   ↓
bundle validation
```

Databricks Declarative Automation Bundles are intended to keep code and Databricks resource definitions together and support source control, testing and CI/CD. citeturn665762search0

---

# 46. CD Pipeline

On approved merge:

```text
GitHub
   ↓
GitHub Actions
   ↓
Databricks authentication
   ↓
bundle validate
   ↓
bundle deploy
   ↓
deployment validation
```

Bundle configuration defines separate:

```text
dev
prod
```

targets.

Databricks supports development and production deployment modes through bundle configuration. citeturn665762search2

---

# 47. Testing

## Unit tests

Cover:

```text
SLA calculations
priority normalization
ticket state transitions
deduplication
feature calculations
quality rules
```

---

## Integration tests

Input:

```text
known ticket dataset
+
known event sequence
```

Validate expected:

```text
Silver ticket state
Gold aggregates
feature values
risk-scoring schema
```

---

## Pipeline recovery test

1. Process event batch A.
2. Stop stream.
3. Add event batch B.
4. Restart stream.
5. Confirm processing resumes from checkpoint.
6. Confirm batch A is not duplicated.

---

## Data-quality test

Inject:

```text
duplicate event
unknown customer
invalid priority
malformed timestamp
missing ticket ID
```

Verify expected quarantine/drop/failure behavior.

---

# 48. Performance Test

Generate intentionally skewed data.

Run baseline transformation.

Record metrics.

Apply alternative optimization.

Record metrics.

Create:

```text
performance-results.md
```

including:

- hypothesis
- baseline
- evidence
- diagnosis
- optimization
- benchmark
- conclusion

---

# 49. Deployment Model

The application is deployed through Databricks configuration rather than manual notebook setup.

Deployment artifacts include:

```text
Python package
jobs
pipelines
configuration
environment variables
tests
ML resources
```

Declarative Automation Bundles support defining and deploying Databricks jobs, pipelines, source files and related resources as a single project. citeturn665762search0turn665762search1

---

# 50. Security

Secrets must never be stored in source code.

Access to data should be mediated through Unity Catalog.

Where credentials are required:

```text
environment variables
Databricks secrets
service identities
```

should be used instead of hard-coded credentials.

---

# 51. Cost Architecture

## Databricks

Use:

**Databricks Free Edition**

Target:

```text
$0
```

Free Edition currently provides no-cost access subject to usage quotas and uses serverless compute. citeturn665762search3

---

## Storage

Use Databricks-managed storage for primary datasets.

Target:

```text
$0
```

Do not maintain large synthetic benchmark datasets after testing.

---

## Event streaming

Use file-based Structured Streaming rather than a continuously running AWS Kinesis stream.

Target:

```text
$0
```

---

## ML

Use MLflow within Databricks.

Target:

```text
$0
```

---

## LLM

Limit investigation generation to a small test dataset.

Example:

```text
20–50 investigations
```

Use a free Databricks-hosted option where available.

Otherwise impose:

```text
maximum AI API budget: $1
```

---

## GitHub

Use free GitHub repository and available GitHub Actions allowance.

Target:

```text
$0
```

---

## Optional AWS Deployment Validation

Budget:

```text
$0–$3
```

Optional final architecture exercise:

```text
small S3 dataset
+
short-lived AWS integration test
```

Resources must be deleted immediately after validation.

---

# 52. Budget

| Component                  |  Budget |
| -------------------------- | ------: |
| Databricks                 |      $0 |
| Spark compute              |      $0 |
| Delta storage              |      $0 |
| MLflow                     |      $0 |
| GitHub                     |      $0 |
| Streaming simulation       |      $0 |
| LLM usage                  |   $0–$1 |
| Optional AWS validation    |   $0–$3 |
| **Maximum expected total** | **<$5** |

---

# 53. Project Milestones

## Milestone 1 — Foundation

Deliver:

- repository
- Databricks workspace
- catalog/schema structure
- data generator
- Delta tables
- initial batch ingestion

---

## Milestone 2 — Medallion Pipeline

Deliver:

```text
Bronze
↓
Silver
↓
Gold
```

including:

- cleansing
- joins
- aggregations
- data quality
- quarantine

---

## Milestone 3 — Streaming Pipeline

Deliver:

- event generator
- Structured Streaming ingestion
- checkpoints
- deduplication
- late-data handling
- restart recovery

---

## Milestone 4 — Spark Performance

Deliver:

- skewed dataset
- baseline benchmark
- execution analysis
- optimization experiments
- benchmark report

---

## Milestone 5 — ML

Deliver:

- feature pipeline
- baseline model
- second model
- MLflow experiments
- evaluation
- model registration
- batch scoring

---

## Milestone 6 — AI Investigation

Deliver:

- investigation context builder
- structured LLM prompt
- JSON response contract
- high-risk ticket investigation output

---

## Milestone 7 — Reliability & Monitoring

Deliver:

- failure scenarios
- retries
- quarantine
- DQ metrics
- pipeline metrics
- recovery tests

---

## Milestone 8 — CI/CD

Deliver:

- unit tests
- integration tests
- Databricks bundle
- GitHub Actions validation
- dev/prod configuration
- automated deployment

---

# 54. Acceptance Criteria

The project is complete when all of the following can be demonstrated.

### Data engineering

- [ ] Batch data is ingested.
- [ ] Streaming data is ingested.
- [ ] Data is stored as Delta tables.
- [ ] Bronze/Silver/Gold layers are implemented.
- [ ] PySpark joins are used.
- [ ] Window functions are used.
- [ ] Aggregations are used.
- [ ] Incremental processing works.

### Streaming

- [ ] Events arrive continuously.
- [ ] Checkpointing is configured.
- [ ] Duplicate events are handled.
- [ ] Late events are demonstrated.
- [ ] Stream recovery is demonstrated.

### Data quality

- [ ] Quality rules exist.
- [ ] Invalid records are detected.
- [ ] Invalid records can be inspected.
- [ ] Quality metrics are captured.

### Spark performance

- [ ] A measurable performance problem is reproduced.
- [ ] Execution evidence identifies the cause.
- [ ] Multiple optimization strategies are tested.
- [ ] Before/after performance is documented.

### ML

- [ ] Features are generated using Spark.
- [ ] At least two models are evaluated.
- [ ] Experiments are tracked in MLflow.
- [ ] Model metrics are recorded.
- [ ] Model version is associated with predictions.

### AI

- [ ] High-risk tickets receive investigation briefs.
- [ ] AI output uses a structured contract.
- [ ] Facts and hypotheses are explicitly separated.

### Operations

- [ ] Workflow orchestration exists.
- [ ] A pipeline failure is demonstrated.
- [ ] Recovery is demonstrated.
- [ ] Monitoring data is available.

### Software engineering

- [ ] Business logic exists outside notebooks.
- [ ] Git is used.
- [ ] Unit tests exist.
- [ ] Integration tests exist.
- [ ] Deployment configuration is version controlled.
- [ ] CI automatically validates changes.
- [ ] Databricks resources can be deployed reproducibly.

### Cost

- [ ] Total external infrastructure spend remains below $5.
