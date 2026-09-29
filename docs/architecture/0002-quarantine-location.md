# ADR 0002: Canonical quarantine location

- Status: accepted
- Date: 2026-09-22

## Context

The specification names both `silver.invalid_records` and `ops.invalid_records`. Quarantined
input is operational evidence rather than a validated business entity.

## Decision

`support_<env>.ops.invalid_records` is the canonical table. A Silver compatibility view may be
added only if a consumer requires the alternate name.

## Consequences

Silver contains validated entities, while operational failures and metrics share one governed
area. Consumers following the earlier Silver name must use the view or migrate.
