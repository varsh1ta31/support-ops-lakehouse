# Support Operations Lakehouse + AI Triage

A governed Databricks lakehouse for support-ticket operations, near-real-time ticket state,
SLA-breach prediction, operational analytics, and grounded AI investigation briefs.

The project is designed for Databricks Free Edition and has a total external-infrastructure
budget below $5.

## Status

The project is in its foundation phase. The product and technical specification is complete,
and implementation is proceeding in verified vertical slices.

## Documentation

- [Product and technical specification](./Support%20Operations%20Lakehouse%20%2B%20AI%20Triage%20%E2%80%94%20Product%20%26%20Technical%20Specification.md)
- [Implementation plan](./IMPLEMENTATION_PLAN.md)
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

## Repository layout

The source package, deployment resources, tests, and project tooling will be added during the
foundation component. Generated data, model artifacts, and credentials must not be committed.

## Cost policy

Development defaults to no-cost local tooling and Databricks Free Edition. Paid integrations
must be explicitly approved, tightly bounded, and recorded against the project budget.
