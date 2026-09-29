# ADR 0001: Testable package and thin Databricks entry points

- Status: accepted
- Date: 2026-09-22

## Context

The system runs on Databricks, but transformations must remain modular and testable outside
notebooks. Coupling business logic directly to workspace notebooks makes local tests and reuse
difficult.

## Decision

Business rules live in the installable `support_ops` package. Databricks job entry points will
only parse runtime parameters, obtain a Spark session, call package functions, and report run
results. Engine-neutral contracts do not import PySpark.

## Consequences

Core tests remain fast and require no cluster. Spark adapters need their own integration tests,
and packaging becomes an explicit deployment step.
