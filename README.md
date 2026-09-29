# Support Operations Lakehouse + AI Triage

A governed Databricks lakehouse for support-ticket operations, near-real-time ticket state,
SLA-breach prediction, operational analytics, and grounded AI investigation briefs.

The project is designed for Databricks Free Edition and has a total external-infrastructure
budget below $5.

## Status

The repository foundation is implemented. Synthetic data generation and Bronze ingestion are
the next delivery slice.

## Documentation

- [Product and technical specification](./Support%20Operations%20Lakehouse%20%2B%20AI%20Triage%20%E2%80%94%20Product%20%26%20Technical%20Specification.md)
- [Implementation plan](./IMPLEMENTATION_PLAN.md)
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
python -m pip install -e '.[dev]'
make check
```

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
