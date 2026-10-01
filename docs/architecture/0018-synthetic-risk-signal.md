# ADR 0018: Isolated synthetic risk signal for the ML demonstration

Status: accepted; [dev validation](../validation/risk-signal-retraining.md) completed.

The original demo profile samples a resolved ticket's SLA breach independently of its scoring
features. It remains unchanged for the operational walkthrough and as a useful negative control
for the model acceptance gate. A separate `risk_demo.toml` profile enables a deliberately
learnable `priority_tier` pattern and runs in the `support_risk_dev` catalog. This keeps the
earlier `support_dev` tables, job runs, and rejected MLflow results intact.

For resolved tickets, breach probability is `sla_breach_rate` multiplied by a priority factor
(P1 4.0, P2 2.5, P3 0.7, P4 0.25) and a support-tier factor (Standard 1.15, Enhanced 1.0,
Premium 0.85), capped at 0.85. Priority and tier exist at ticket creation and in the Gold
scoring features. A breached ticket resolves strictly after its contract's resolution deadline;
other resolved tickets finish by the deadline. Open tickets remain unlabeled for training.
The profile's `sla_breach_rate` is a **base probability**, not a promised overall breach rate.
The manifest identifies this generator variant as version 2. The original random profile
retains version 1 and its previous output behavior.

This pattern tests the pipeline's ability to learn and reject using time-separated data. It is
not evidence that real support-ticket breaches are this predictable. Risk scores from this
cohort must be presented as synthetic demonstration results, not operational forecasts.
