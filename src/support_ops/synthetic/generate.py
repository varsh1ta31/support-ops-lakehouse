"""Generate logically consistent support-domain records using only the standard library."""

from __future__ import annotations

import bisect
import csv
import hashlib
import json
import random
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from typing import TypeAlias

from support_ops.schemas import ACCOUNTS, CONTRACTS, PRODUCTS, TICKETS, RecordSchema
from support_ops.synthetic.config import Distribution, GenerationConfig

Scalar: TypeAlias = str | int | float | bool | None
Record: TypeAlias = dict[str, Scalar]

SEGMENTS = ("SMB", "Mid-Market", "Enterprise", "Strategic")
REGIONS = ("North America", "Europe", "Asia Pacific", "Latin America")
INDUSTRIES = ("Financial Services", "Healthcare", "Retail", "Technology", "Logistics")
CHANNELS = ("email", "web", "chat", "api")
CATEGORIES = ("availability", "authentication", "billing", "configuration", "performance")
PRIORITIES = ("P1", "P2", "P3", "P4")

PRODUCT_CATALOG = (
    ("Payments API", "Payments", "payments-platform", "critical"),
    ("Identity Gateway", "Identity", "identity-platform", "critical"),
    ("Analytics Studio", "Analytics", "analytics-experience", "high"),
    ("Workflow Engine", "Automation", "workflow-platform", "high"),
    ("Notification Hub", "Messaging", "messaging-platform", "medium"),
    ("Admin Console", "Platform", "admin-experience", "medium"),
)

TIER_SLA_MINUTES: dict[str, dict[str, tuple[int, int]]] = {
    "Standard": {
        "P1": (120, 480),
        "P2": (480, 1440),
        "P3": (1440, 4320),
        "P4": (2880, 10080),
    },
    "Enhanced": {
        "P1": (60, 240),
        "P2": (240, 720),
        "P3": (720, 2880),
        "P4": (1440, 7200),
    },
    "Premium": {
        "P1": (30, 120),
        "P2": (120, 480),
        "P3": (480, 1440),
        "P4": (720, 4320),
    },
}


@dataclass(frozen=True)
class Customer:
    customer_id: str
    support_tier: str


@dataclass(frozen=True)
class Product:
    product_id: str
    product_name: str


class WeightedPool:
    """Efficient repeated deterministic selection from a fixed population."""

    def __init__(self, values: Sequence[str], weights: Sequence[float]) -> None:
        if not values or len(values) != len(weights):
            raise ValueError("values and weights must have the same non-zero length")
        if any(weight < 0 for weight in weights) or sum(weights) <= 0:
            raise ValueError("weights must be non-negative with a positive total")
        self._values = tuple(values)
        total = sum(weights)
        running = 0.0
        cumulative: list[float] = []
        for weight in weights:
            running += weight / total
            cumulative.append(running)
        cumulative[-1] = 1.0
        self._cumulative = tuple(cumulative)

    def choose(self, rng: random.Random) -> str:
        index = bisect.bisect_left(self._cumulative, rng.random())
        return self._values[index]


def _entity_weights(count: int, distribution: Distribution) -> list[float]:
    if distribution is Distribution.UNIFORM or count == 1:
        return [1.0] * count
    if count == 2:
        return [0.70, 0.30]
    return [0.35, 0.15, *([0.50 / (count - 2)] * (count - 2))]


def _support_tier(segment: str, rng: random.Random) -> str:
    tiers = {
        "SMB": (("Standard", "Enhanced", "Premium"), (0.80, 0.18, 0.02)),
        "Mid-Market": (("Standard", "Enhanced", "Premium"), (0.45, 0.45, 0.10)),
        "Enterprise": (("Standard", "Enhanced", "Premium"), (0.10, 0.55, 0.35)),
        "Strategic": (("Standard", "Enhanced", "Premium"), (0.02, 0.28, 0.70)),
    }
    values, weights = tiers[segment]
    return rng.choices(values, weights=weights, k=1)[0]


def _iso_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


class DatasetGenerator:
    """Stateful seeded generator whose output order is part of the data contract."""

    def __init__(self, config: GenerationConfig) -> None:
        self.config = config
        self.rng = random.Random(config.seed)
        self.customers: list[Customer] = []
        self.products: list[Product] = []

    def product_records(self) -> Iterator[Record]:
        for index in range(1, self.config.number_of_products + 1):
            product_id = f"PRD-{index:03d}"
            if index <= len(PRODUCT_CATALOG):
                name, family, owner, criticality = PRODUCT_CATALOG[index - 1]
            else:
                name = f"Platform Service {index:03d}"
                family = "Platform"
                owner = f"platform-team-{((index - 1) % 8) + 1:02d}"
                criticality = ("medium", "high", "low")[index % 3]
            self.products.append(Product(product_id, name))
            yield {
                "product_id": product_id,
                "product_name": name,
                "product_family": family,
                "service_owner": owner,
                "criticality": criticality,
            }

    def account_records(self) -> Iterator[Record]:
        segment_weights = (0.48, 0.30, 0.17, 0.05)
        acv_ranges = {
            "SMB": (5_000, 50_000),
            "Mid-Market": (50_000, 250_000),
            "Enterprise": (250_000, 1_500_000),
            "Strategic": (1_500_000, 8_000_000),
        }
        for index in range(1, self.config.number_of_customers + 1):
            customer_id = f"CUS-{index:06d}"
            segment = self.rng.choices(SEGMENTS, weights=segment_weights, k=1)[0]
            tier = _support_tier(segment, self.rng)
            minimum, maximum = acv_ranges[segment]
            acv = round(self.rng.uniform(minimum, maximum), 2)
            self.customers.append(Customer(customer_id, tier))
            yield {
                "customer_id": customer_id,
                "customer_name": f"Customer {index:06d}",
                "segment": segment,
                "region": self.rng.choice(REGIONS),
                "industry": self.rng.choice(INDUSTRIES),
                "annual_contract_value": acv,
                "support_tier": tier,
                "account_status": self.rng.choices(
                    ("active", "at_risk", "suspended"), weights=(0.94, 0.05, 0.01), k=1
                )[0],
            }

    def contract_records(self) -> Iterator[Record]:
        effective_from = self.config.start_date - timedelta(days=365)
        for customer in self.customers:
            for priority in PRIORITIES:
                response_sla, resolution_sla = TIER_SLA_MINUTES[customer.support_tier][priority]
                yield {
                    "contract_id": f"CTR-{customer.customer_id[4:]}-{priority}",
                    "customer_id": customer.customer_id,
                    "support_tier": customer.support_tier,
                    "priority": priority,
                    "response_sla_minutes": response_sla,
                    "resolution_sla_minutes": resolution_sla,
                    "effective_from": effective_from.isoformat(),
                    "effective_to": None,
                }

    def ticket_records(self) -> Iterator[Record]:
        if not self.customers or not self.products:
            raise RuntimeError("accounts and products must be generated before tickets")
        customer_by_id = {customer.customer_id: customer for customer in self.customers}
        product_by_id = {product.product_id: product for product in self.products}
        customer_pool = WeightedPool(
            tuple(customer_by_id),
            _entity_weights(len(customer_by_id), self.config.customer_distribution),
        )
        product_pool = WeightedPool(
            tuple(product_by_id),
            _entity_weights(len(product_by_id), self.config.product_distribution),
        )
        start = datetime.combine(self.config.start_date, time.min, tzinfo=UTC)
        end = datetime.combine(self.config.end_date, time.max, tzinfo=UTC)
        seconds = int((end - start).total_seconds())

        for index in range(1, self.config.number_of_tickets + 1):
            customer = customer_by_id[customer_pool.choose(self.rng)]
            product = product_by_id[product_pool.choose(self.rng)]
            priority = self.rng.choices(PRIORITIES, weights=(0.05, 0.20, 0.50, 0.25), k=1)[0]
            created_at = start + timedelta(seconds=self.rng.randint(0, seconds))
            is_open = self.rng.random() < self.config.open_ticket_rate
            breached = not is_open and self.rng.random() < self.config.sla_breach_rate
            resolution_sla = TIER_SLA_MINUTES[customer.support_tier][priority][1]
            if breached:
                resolution_minutes = self.rng.randint(resolution_sla + 1, resolution_sla * 3)
            else:
                resolution_minutes = self.rng.randint(5, max(5, resolution_sla))
            closed_at = None if is_open else created_at + timedelta(minutes=resolution_minutes)
            escalated = self.rng.random() < self.config.escalation_rate
            category = self.rng.choice(CATEGORIES)

            yield {
                "ticket_id": f"TKT-{index:09d}",
                "customer_id": customer.customer_id,
                "product_id": product.product_id,
                "created_at": _iso_timestamp(created_at),
                "closed_at": None if closed_at is None else _iso_timestamp(closed_at),
                "priority": priority,
                "channel": self.rng.choice(CHANNELS),
                "category": category,
                "subject": f"{product.product_name}: {category} issue",
                "description": f"Customer reported a {category} issue in {product.product_name}.",
                "resolution": None if is_open else f"Resolved {category} issue.",
                "final_status": "open" if is_open else "resolved",
                "escalated": escalated,
                "sla_breached": breached,
                "agent_id": None
                if is_open and self.rng.random() < 0.15
                else f"AGT-{self.rng.randint(1, 80):04d}",
            }

    def invalid_ticket_records(self) -> Iterator[Record]:
        """Produce isolated known-bad rows for downstream quarantine tests."""

        failure_patterns: tuple[tuple[str, Scalar], ...] = (
            ("ticket_id", None),
            ("customer_id", "CUS-UNKNOWN"),
            ("priority", "URGENT"),
            ("created_at", "not-a-timestamp"),
        )
        for index in range(1, self.config.invalid_record_count + 1):
            record: Record = {
                "ticket_id": f"BAD-{index:06d}",
                "customer_id": self.customers[0].customer_id,
                "product_id": self.products[0].product_id,
                "created_at": f"{self.config.start_date.isoformat()}T00:00:00Z",
                "closed_at": None,
                "priority": "P3",
                "channel": "web",
                "category": "configuration",
                "subject": "Intentional quality-test record",
                "description": "This record is expected to be quarantined.",
                "resolution": None,
                "final_status": "open",
                "escalated": False,
                "sla_breached": False,
                "agent_id": None,
            }
            field, value = failure_patterns[(index - 1) % len(failure_patterns)]
            record[field] = value
            yield record


def _csv_value(value: Scalar) -> Scalar:
    if isinstance(value, bool):
        return str(value).lower()
    return value


def _write_csv(path: Path, schema: RecordSchema, records: Iterable[Record]) -> int:
    fieldnames = [field.name for field in schema.fields]
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    count = 0
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=fieldnames, extrasaction="raise", lineterminator="\n"
        )
        writer.writeheader()
        for record in records:
            if set(record) != set(fieldnames):
                raise ValueError(f"Record keys do not match the {schema.name} schema")
            writer.writerow({key: _csv_value(value) for key, value in record.items()})
            count += 1
    temporary.replace(path)
    return count


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def generate_dataset(
    config: GenerationConfig,
    output_dir: Path | str,
    *,
    overwrite: bool = False,
) -> dict[str, object]:
    """Write a complete dataset and deterministic manifest to ``output_dir``."""

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    managed_names = [
        "products.csv",
        "accounts.csv",
        "contracts.csv",
        "tickets.csv",
        "manifest.json",
    ]
    if config.invalid_record_count:
        managed_names.append("invalid_tickets.csv")
    existing = [name for name in managed_names if (destination / name).exists()]
    if existing and not overwrite:
        raise FileExistsError(f"Refusing to overwrite generated files: {', '.join(existing)}")

    generator = DatasetGenerator(config)
    outputs: list[tuple[str, RecordSchema, Iterable[Record]]] = [
        ("products.csv", PRODUCTS, generator.product_records()),
        ("accounts.csv", ACCOUNTS, generator.account_records()),
        ("contracts.csv", CONTRACTS, generator.contract_records()),
        ("tickets.csv", TICKETS, generator.ticket_records()),
    ]
    if config.invalid_record_count:
        outputs.append(("invalid_tickets.csv", TICKETS, generator.invalid_ticket_records()))

    files: dict[str, dict[str, Scalar]] = {}
    for filename, schema, records in outputs:
        path = destination / filename
        row_count = _write_csv(path, schema, records)
        files[filename] = {"rows": row_count, "sha256": _sha256(path)}

    manifest: dict[str, object] = {
        "generator_version": 1,
        "configuration": {
            **asdict(config),
            "start_date": config.start_date.isoformat(),
            "end_date": config.end_date.isoformat(),
        },
        "files": files,
    }
    manifest_path = destination / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )
    return manifest
