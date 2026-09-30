# ADR 0005: Engine-neutral Silver validation

- Status: accepted
- Date: 2026-09-29

## Decision

Validate source records through `support_ops.quality.validation.validate_record` before Silver
writes. The caller supplies reference customer/product IDs, an aware clock, future-time tolerance
(default five minutes), and previously accepted entity IDs. The function never changes inputs.

Required fields follow the central schemas. Normalize priority, segment, region, support tier,
and event type; cast numeric, boolean, date, and timestamp fields. Reject non-finite amounts,
negative contract values, non-positive SLAs, inverted date ranges, unknown references, malformed
JSON event payloads, unsupported events/priorities, and timestamps beyond the configured tolerance.
Timestamp input must include a timezone; accepted timestamps are converted to UTC.

Return accepted, rejected_duplicate, or quarantined. Validate before checking duplicates so a
malformed replay remains inspectable. Processed IDs are supplied by the caller and must only
include accepted records. A valid changed payload with an existing ID is currently a duplicate;
entity update/version semantics must be defined before implementing an upsert writer.

Quarantine rows match `ops.invalid_records`, retaining the entire original payload (including
Bronze provenance), reasons, detection time, and run ID. Their stable ID hashes entity plus raw
payload. The writer must choose retry/update semantics explicitly. No table writes occur here.

## Tradeoffs and remaining work

Pure Python keeps business rules fast to test without Spark. The eventual Spark adapter must
reuse these rules without collecting entire tables or large reference sets to the driver.
Event ordering, late-arrival classification, ticket transition rules, active-contract selection,
quality metric persistence, and Delta retry behavior remain separate implementation slices.

## Verification

The quality-profile fixture accepts all 604 generated reference/history records, rejects their
valid replays, and quarantines all eight injected-invalid tickets. Focused fixtures cover UTC
conversion, future-time boundaries, required fields, canonical values, numeric/date rules,
unknown references, JSON payloads, and quarantine provenance. Full checks: 76 tests pass,
94% overall coverage and 100% validation-module coverage. No external services or costs.
