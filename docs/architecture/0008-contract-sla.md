# ADR 0008: Resolution SLA from the contract in force at ticket creation

- Status: accepted
- Date: 2026-09-29

## Decision

`silver.ticket_state` carries `contract_id`, `support_tier`, `resolution_sla_minutes`,
`sla_deadline`, and `minutes_to_sla`, computed by `support_ops.transformations.ticket_state`
(`select_contract`, `sla_fields`) from the reconstructed state, the customer's Silver contracts,
and the same `as_of` used for ticket state.

- **Clock start.** The resolution-SLA clock starts at `created_at` and is not restarted by
  reopens. `sla_deadline = created_at + resolution_sla_minutes`.
- **Governing contract.** The contract with the ticket's customer and *current* priority that is
  in force at `created_at`. A priority change re-targets the deadline from creation, so a
  downgrade can move the deadline later and an upgrade can make a ticket already overdue.
  Contract renewals after creation do not change an open ticket's terms.
- **Effective dates.** `effective_from` and `effective_to` are whole UTC days, both inclusive; a
  null `effective_to` is open-ended. Validation already rejects `effective_to < effective_from`.
- **Overlaps.** The latest `effective_from` wins, since a renewal supersedes its predecessor. The
  highest `contract_id` breaks remaining ties. Overlaps are not quarantined because each
  contract row is individually valid.
- **Remaining time.** `minutes_to_sla` is measured to `resolved_at`, or to `as_of` while
  unresolved, and floored to whole minutes. It is negative exactly when the deadline was missed,
  so `minutes_to_sla < 0` is the breach flag for state and point-in-time features.
- **No contract.** All five fields are null. The job summary reports the count as
  `without_sla`, alongside `sla_breached` for the evaluated `as_of`.

The account's `support_tier` is not used: the contract is the SLA source of truth, and the two
can legitimately differ during a tier change.

## Persistence

Selection runs in a second executor-side UDF after the state fold, joined on the
*reconstructed* `customer_id`. Event-only tickets learn their customer inside the fold, so
joining contracts before it would need to duplicate fold logic in Spark. Contracts are grouped per
customer (four priorities times a few versions), so the join stays small. DATE values cross the UDF
boundary unchanged; timestamps keep the UTC epoch-microsecond contract from ADR 0007. The SLA
columns are part of the same atomic `INSERT OVERWRITE`, so retries remain idempotent.

## Tradeoffs and limitations

- Silver contracts are insert-only and not bitemporal. A contract loaded retroactively applies to
  tickets created in its effective range when state is rebuilt, including point-in-time rebuilds.
  That is correct for SLA accounting, but Phase 6 must treat contract load time as a feature
  availability boundary if retroactive contracts become common.
- Response SLA (first agent reply) is not modeled; the spec's state and ML target use resolution
  SLA only. `response_sla_minutes` remains available in `silver.contracts`.
- Time is wall-clock. Business hours, holidays, and paused `pending_customer` time are out of scope
  and would change the deadline definition rather than the selection rule.
- The SLA columns ship with ticket state's first release, so no migration is planned. The
  overwrite is positional against an existing table: a workspace that ran the pre-SLA schema
  must drop `silver.ticket_state` once, and later schema changes need an explicit migration.
