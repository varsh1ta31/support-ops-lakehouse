"""Configuration and contract checks for the Spark performance experiment."""

import pytest

from support_ops.performance import STRATEGIES, validate_size


def test_strategy_contract() -> None:
    assert STRATEGIES == ("merge_baseline", "adaptive_default", "broadcast", "salted_merge")
    validate_size(1_000_000, 1_000, 64, 16)


@pytest.mark.parametrize(
    ("events", "customers", "partitions", "buckets"),
    [(0, 1000, 64, 16), (100, 2, 64, 16), (100, 1000, 1, 16), (100, 1000, 64, 1)],
)
def test_invalid_size(events: int, customers: int, partitions: int, buckets: int) -> None:
    with pytest.raises(ValueError):
        validate_size(events, customers, partitions, buckets)
