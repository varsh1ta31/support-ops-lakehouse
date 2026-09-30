# Product incident signals — local validation

The third Phase 4 product is `gold.incident_signals`, evaluated at
`2026-01-01T12:00:00Z` in the known fixture.

## Known fixture

| Product | Current tickets | Customers | Prior seven-hour total | Baseline per hour | Signal |
| --- | ---: | ---: | ---: | ---: | --- |
| p1 | 5 | 3 | 6 | 6/7 | true |
| p2 | 4 | 4 | 0 | 0 | false |
| p3 | 0 | 0 | 0 | 0 | false |

For p1, two current tickets are P1, one has an escalation, and volume deviation is `5 - 6/7`.
The six comparison tickets occupy six of the previous seven matching UTC hours; the seventh
hour has zero tickets. An eight-day-old ticket and a ticket in the wrong hour do not enter the
baseline. A ticket created at the following hour boundary belongs to that next hour.

Replaying the target hour preserves exactly one row per product. Removing a product and
rebuilding the hour deletes its stale row, while the adjacent hour remains intact. The fixture
cleans its Silver rows before downstream integration tests.

## Checks and deployment limit

The Spark/Delta fixture, Ruff formatting/lint, mypy, isolated wheel build, CLI entry-point
inspection, and offline Databricks bundle schema validation pass. The full suite passed
**145 tests** with **95.59% total coverage** in 135.18 seconds. Authenticated deployment and job execution are recorded in
[Gold live validation](./gold-live.md).
