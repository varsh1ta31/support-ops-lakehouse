import csv
from datetime import date, datetime
from pathlib import Path
from typing import cast

import pytest

from support_ops.synthetic.config import BreachPattern, Distribution, GenerationConfig
from support_ops.synthetic.generate import WeightedPool, breach_probability, generate_dataset


def config(**changes: object) -> GenerationConfig:
    values: dict[str, object] = {
        "number_of_customers": 12,
        "number_of_products": 4,
        "number_of_tickets": 200,
        "events_per_ticket": 3,
        "start_date": date(2025, 1, 1),
        "end_date": date(2025, 1, 31),
        "customer_distribution": Distribution.SKEWED,
        "product_distribution": Distribution.SKEWED,
        "sla_breach_rate": 0.25,
        "escalation_rate": 0.10,
        "open_ticket_rate": 0.10,
        "seed": 7,
    }
    values.update(changes)
    return GenerationConfig(**values)  # type: ignore[arg-type]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_generated_dataset_has_expected_counts_and_relationships(tmp_path: Path) -> None:
    manifest = generate_dataset(config(), tmp_path)
    accounts = read_csv(tmp_path / "accounts.csv")
    products = read_csv(tmp_path / "products.csv")
    contracts = read_csv(tmp_path / "contracts.csv")
    tickets = read_csv(tmp_path / "tickets.csv")

    assert manifest["files"]["accounts.csv"]["rows"] == 12  # type: ignore[index]
    assert len(products) == 4
    assert len(contracts) == 12 * 4
    assert len(tickets) == 200

    customer_ids = {row["customer_id"] for row in accounts}
    product_ids = {row["product_id"] for row in products}
    assert {row["customer_id"] for row in contracts} <= customer_ids
    assert {row["customer_id"] for row in tickets} <= customer_ids
    assert {row["product_id"] for row in tickets} <= product_ids


def test_ticket_lifecycle_and_sla_labels_are_consistent(tmp_path: Path) -> None:
    generate_dataset(config(), tmp_path)
    contracts = read_csv(tmp_path / "contracts.csv")
    tickets = read_csv(tmp_path / "tickets.csv")
    sla = {
        (row["customer_id"], row["priority"]): int(row["resolution_sla_minutes"])
        for row in contracts
    }

    for ticket in tickets:
        if ticket["final_status"] == "open":
            assert ticket["closed_at"] == ""
            assert ticket["resolution"] == ""
            assert ticket["sla_breached"] == "false"
            continue
        created = datetime.fromisoformat(ticket["created_at"].replace("Z", "+00:00"))
        closed = datetime.fromisoformat(ticket["closed_at"].replace("Z", "+00:00"))
        duration_minutes = (closed - created).total_seconds() / 60
        expected = duration_minutes > sla[(ticket["customer_id"], ticket["priority"])]
        assert (ticket["sla_breached"] == "true") is expected


def test_generation_is_byte_deterministic(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"

    first_manifest = generate_dataset(config(), first)
    second_manifest = generate_dataset(config(), second)

    assert first_manifest == second_manifest
    files = cast(dict[str, object], first_manifest["files"])
    for filename in files:
        assert (first / filename).read_bytes() == (second / filename).read_bytes()


def test_existing_managed_files_require_explicit_overwrite(tmp_path: Path) -> None:
    generate_dataset(config(number_of_tickets=2), tmp_path)

    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        generate_dataset(config(number_of_tickets=2), tmp_path)

    generate_dataset(config(number_of_tickets=3), tmp_path, overwrite=True)
    assert len(read_csv(tmp_path / "tickets.csv")) == 3


def test_invalid_records_are_isolated_from_valid_data(tmp_path: Path) -> None:
    manifest = generate_dataset(config(invalid_record_count=5), tmp_path)
    invalid = read_csv(tmp_path / "invalid_tickets.csv")

    assert len(invalid) == 5
    assert manifest["files"]["invalid_tickets.csv"]["rows"] == 5  # type: ignore[index]
    assert any(not row["ticket_id"] for row in invalid)
    assert any(row["customer_id"] == "CUS-UNKNOWN" for row in invalid)
    assert any(row["priority"] == "URGENT" for row in invalid)


def test_weighted_pool_validates_inputs() -> None:
    with pytest.raises(ValueError, match="same non-zero length"):
        WeightedPool([], [])
    with pytest.raises(ValueError, match="positive total"):
        WeightedPool(["a"], [0])


def test_risk_pattern_uses_creation_time_priority_and_tier() -> None:
    patterned = config(breach_pattern=BreachPattern.PRIORITY_TIER, sla_breach_rate=0.12)
    high = breach_probability(patterned, priority="P1", support_tier="Standard")
    low = breach_probability(patterned, priority="P4", support_tier="Premium")
    assert high == pytest.approx(0.552)
    assert low == pytest.approx(0.0255)
    assert high > 10 * low
    assert breach_probability(config(), priority="P1", support_tier="Standard") == 0.25


def test_risk_pattern_has_distinct_manifest_version(tmp_path: Path) -> None:
    manifest = generate_dataset(
        config(number_of_tickets=10, breach_pattern=BreachPattern.PRIORITY_TIER), tmp_path
    )
    assert manifest["generator_version"] == 2
