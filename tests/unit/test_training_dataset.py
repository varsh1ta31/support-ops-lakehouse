"""Validate explicit chronological training boundaries."""

from datetime import UTC, datetime, timedelta

import pytest

from support_ops.training_dataset import validate_periods

START = datetime(2026, 1, 1, tzinfo=UTC)


def test_valid_boundaries() -> None:
    dates = validate_periods(
        train_end=START,
        validation_end=START + timedelta(hours=1),
        test_end=START + timedelta(hours=2),
        label_as_of=START + timedelta(hours=3),
    )
    assert dates[1:] == (START, START + timedelta(hours=1), START + timedelta(hours=2))


@pytest.mark.parametrize(
    "offsets",
    [(3, 1, 3, 2), (3, 2, 2, 3), (2, 1, 2, 3)],
)
def test_invalid_boundaries(offsets: tuple[int, int, int, int]) -> None:
    cutoff, train, validation, test = offsets
    with pytest.raises(ValueError):
        validate_periods(
            label_as_of=START + timedelta(hours=cutoff),
            train_end=START + timedelta(hours=train),
            validation_end=START + timedelta(hours=validation),
            test_end=START + timedelta(hours=test),
        )
