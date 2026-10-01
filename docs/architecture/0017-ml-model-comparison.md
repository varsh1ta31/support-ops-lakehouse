# ADR 0017: Chronological model comparison and acceptance gate

Status: accepted; verified in the dev workspace (see [model evaluation](../validation/model-training.md)).

The Phase 6 training job reads a specific Delta version of `ml.training_dataset`, selecting
only its 17 point-in-time feature columns as model inputs. It fits a logistic-regression baseline
and a histogram gradient-boosted-tree candidate on the train period. Numeric missing values are
imputed from training data and standardized; categorical values are imputed and one-hot encoded.
The complete preprocessing pipeline travels with each model artifact, so inference applies the
same transformations. This small dataset is collected to the job driver with a 100,000-row cap;
larger datasets require distributed fitting or a deliberate capacity review.

For each candidate, a decision threshold maximizes F1 on the validation period. The test period
does not influence fitting, threshold choice, or candidate selection. Validation PR-AUC chooses
the candidate, with recall and precision as tie-breakers. Precision, recall, F1, PR-AUC, and
ROC-AUC are then reported on both periods. MLflow records both model artifacts, hyperparameters,
feature list, time ranges, dataset version, and metrics in a workspace experiment.

The selected candidate is eligible for registration only if its untouched test PR-AUC is at
least 1.2 times breach prevalence, ROC-AUC is at least 0.60, precision exceeds prevalence, and
recall is at least 0.30. These are modest minimums, not a claim of operational usefulness.
Failing any gate tags the run as rejected and prevents registration and scoring. There is no
previous production scoring path yet, so the job leaves `gold.ticket_risk_scores` untouched.

The current synthetic generator draws breach status independently for each resolved ticket.
Consequently, the available point-in-time features have little reliable signal for this label.
The dev experiment rejected the best validation candidate on the later test period. The next
data slice should add an explicit, documented causal risk pattern to newly generated synthetic
tickets, rebuild the dev dataset, and repeat the same out-of-time gate without editing old
evaluation results.
