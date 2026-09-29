"""Configuration contracts for synthetic datasets."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import Any


class Distribution(StrEnum):
    """Supported entity-selection distributions."""

    UNIFORM = "uniform"
    SKEWED = "skewed"


@dataclass(frozen=True)
class GenerationConfig:
    """Validated inputs for a reproducible generated dataset."""

    number_of_customers: int
    number_of_products: int
    number_of_tickets: int
    events_per_ticket: int
    start_date: date
    end_date: date
    customer_distribution: Distribution = Distribution.UNIFORM
    product_distribution: Distribution = Distribution.UNIFORM
    sla_breach_rate: float = 0.12
    escalation_rate: float = 0.08
    open_ticket_rate: float = 0.15
    invalid_record_count: int = 0
    seed: int = 42

    def __post_init__(self) -> None:
        positive = {
            "number_of_customers": self.number_of_customers,
            "number_of_products": self.number_of_products,
            "number_of_tickets": self.number_of_tickets,
        }
        for name, value in positive.items():
            if value <= 0:
                raise ValueError(f"{name} must be greater than zero")
        if self.events_per_ticket < 0:
            raise ValueError("events_per_ticket must be non-negative")
        if self.invalid_record_count < 0:
            raise ValueError("invalid_record_count must be non-negative")
        if self.start_date > self.end_date:
            raise ValueError("start_date must not be after end_date")
        rates: dict[str, float] = {
            "sla_breach_rate": self.sla_breach_rate,
            "escalation_rate": self.escalation_rate,
            "open_ticket_rate": self.open_ticket_rate,
        }
        for rate_name, rate_value in rates.items():
            if not 0 <= rate_value <= 1:
                raise ValueError(f"{rate_name} must be between 0 and 1")


def load_generation_config(path: Path | str) -> GenerationConfig:
    """Load a generator profile from TOML."""

    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(f"Generation profile not found: {source}")
    with source.open("rb") as handle:
        values: dict[str, Any] = tomllib.load(handle)

    date_range = values.pop("date_range", None)
    if not isinstance(date_range, dict):
        raise ValueError("date_range must be a TOML table")
    customer_distribution = Distribution(values.pop("customer_distribution", "uniform"))
    product_distribution = Distribution(values.pop("product_distribution", "uniform"))

    return GenerationConfig(
        **values,
        start_date=date.fromisoformat(str(date_range["start"])),
        end_date=date.fromisoformat(str(date_range["end"])),
        customer_distribution=customer_distribution,
        product_distribution=product_distribution,
    )
