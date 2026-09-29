# ADR 0003: UTC and environment isolation

- Status: accepted
- Date: 2026-09-22

## Context

Event ordering, lateness, SLA deadlines, and time-based ML splits become ambiguous if stored
timestamps use different time zones. The cost constraint permits dev and prod to share a Free
Edition workspace, so data isolation cannot depend on separate workspaces.

## Decision

Persist timestamps in UTC and localize only at presentation boundaries. Separate logical
environments with `support_dev` and `support_prod` catalogs selected through configuration.

## Consequences

Temporal calculations have consistent semantics. Deployment code must select the correct target,
and production access controls must prevent developers from accidentally writing to its catalog.
