# ADR 0015: Observed outcomes for training snapshots

Status: accepted; [first dev dataset validation](../validation/training-dataset.md) completed.

`ml.training_dataset` joins `gold.ticket_features` to ticket state reconstructed from Silver at an
explicit `label_as_of` cutoff. The only model inputs are columns already present in the feature
snapshot. `resolved_at` and the eventual SLA deadline are used only to produce the integer
`label`, never as features. The label is 1 when resolution was strictly later than the SLA
deadline and 0 otherwise.

An example is eligible only if it was active and had time remaining at scoring, resolved after
the scoring time and by the label cutoff, has an SLA deadline, and has no reopen. Unresolved rows
are censored and omitted; they are not negative examples. Reopened episodes are omitted because
a single final resolution timestamp does not describe their earlier outcome unambiguously. The
earliest eligible snapshot per ticket is kept so one ticket cannot leak across train, validation,
and test splits. Explicit UTC hour boundaries assign those splits chronologically by `as_of`.

A single Delta `replaceWhere` write replaces the complete dataset for a `label_as_of` cutoff,
including stale rows. Reruns should use stable Silver and Gold inputs. Later corrections can
change labels, so a model run must record its training-table version and cutoff. The first dev
feature snapshot is too small for model selection; candidate training waits for enough labeled
snapshots and nonempty chronological splits.
