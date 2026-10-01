"""Guard chronological cohort quality and validation-only model selection."""

import pytest

from support_ops.model_training import acceptance_failures, choose_candidate, validate_cohorts


def test_cohorts_require_both_classes_and_unique_tickets() -> None:
    rows = [
        {"ticket_id": f"{split}-{label}", "split": split, "label": label}
        for split in ("train", "validation", "test")
        for label in (0, 1)
    ]
    assert validate_cohorts(rows)["test"] == {"rows": 2, "breaches": 1}
    with pytest.raises(ValueError, match="duplicate"):
        validate_cohorts([*rows, rows[0]])
    with pytest.raises(ValueError, match="validation"):
        validate_cohorts([row for row in rows if row["ticket_id"] != "validation-1"])


def test_selection_ignores_test_score() -> None:
    results = {
        "baseline": {
            "validation": {"pr_auc": 0.8, "recall": 0.4, "precision": 0.5},
            "test": {"pr_auc": 0.1},
        },
        "tree": {
            "validation": {"pr_auc": 0.7, "recall": 0.9, "precision": 0.9},
            "test": {"pr_auc": 0.99},
        },
    }
    assert choose_candidate(results) == "baseline"


def test_acceptance_requires_out_of_time_lift_and_useful_recall() -> None:
    weak = {"pr_auc": 0.11, "roc_auc": 0.48, "precision": 0.11, "recall": 0.09}
    assert len(acceptance_failures(weak, breach_rate=0.125)) == 4
    useful = {"pr_auc": 0.22, "roc_auc": 0.7, "precision": 0.25, "recall": 0.5}
    assert acceptance_failures(useful, breach_rate=0.125) == []
