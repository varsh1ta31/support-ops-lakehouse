# ADR 0004: Unity Catalog Volume namespace

- Status: accepted
- Date: 2026-09-29

## Context

Unity Catalog Volume paths contain catalog, schema, and volume components. The specification's
landing path `/Volumes/support_dev/raw/landing` therefore implies a `raw` schema even though the
initial catalog diagram lists only Bronze, Silver, Gold, ML, and Ops.

## Decision

Create a `raw` schema for non-tabular landing Volumes. Tables remain confined to Bronze and later
layers. Create `raw.landing`, `raw.ticket_events`, and `ops.checkpoints` as managed Volumes.

## Consequences

Landing files have governed Unity Catalog paths without being mistaken for Bronze tables. The
catalog contains one additional schema beyond the simplified diagram in the specification.
