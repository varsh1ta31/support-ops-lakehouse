from datetime import date
from pathlib import Path

import pytest

from support_ops.synthetic.config import Distribution, GenerationConfig, load_generation_config

PROJECT_ROOT = Path(__file__).parents[3]


def test_demo_profile_loads_typed_values() -> None:
    config = load_generation_config(PROJECT_ROOT / "config/generation/demo.toml")

    assert config.number_of_tickets == 10_000
    assert config.start_date == date(2025, 1, 1)
    assert config.customer_distribution is Distribution.SKEWED


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"number_of_customers": 0}, "number_of_customers"),
        ({"events_per_ticket": -1}, "events_per_ticket"),
        ({"start_date": date(2025, 2, 1), "end_date": date(2025, 1, 1)}, "start_date"),
        ({"sla_breach_rate": 1.01}, "sla_breach_rate"),
        ({"escalation_rate": -0.01}, "escalation_rate"),
        ({"open_ticket_rate": 2.0}, "open_ticket_rate"),
        ({"invalid_record_count": -1}, "invalid_record_count"),
    ],
)
def test_invalid_configuration_is_rejected(change: dict[str, object], message: str) -> None:
    values: dict[str, object] = {
        "number_of_customers": 2,
        "number_of_products": 2,
        "number_of_tickets": 2,
        "events_per_ticket": 1,
        "start_date": date(2025, 1, 1),
        "end_date": date(2025, 1, 2),
    }
    values.update(change)

    with pytest.raises(ValueError, match=message):
        GenerationConfig(**values)  # type: ignore[arg-type]


def test_missing_profile_is_explicit(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="Generation profile"):
        load_generation_config(tmp_path / "missing.toml")


def test_profile_requires_date_range(tmp_path: Path) -> None:
    profile = tmp_path / "bad.toml"
    profile.write_text("number_of_customers = 1\n", encoding="utf-8")

    with pytest.raises(ValueError, match="date_range"):
        load_generation_config(profile)
