"""Deterministic incremental ticket-event files for the file-based streaming simulation."""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import re
import tomllib
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from support_ops.schemas import TICKET_EVENTS
from support_ops.synthetic.generate import PRIORITIES, Record, _iso_timestamp

BATCH_FILE = re.compile(r"^events_(\d{6})\.json$")


@dataclass(frozen=True)
class EventStreamConfig:
    """Stream-simulation inputs. Customer and product counts must match the batch profile."""

    number_of_customers: int
    number_of_products: int
    start: datetime
    batch_interval_minutes: int = 15
    tickets_per_batch: int = 20
    lifecycle_batches: int = 16
    duplicate_rate: float = 0.02
    late_rate: float = 0.02
    late_delay_minutes: int = 180
    malformed_per_batch: int = 0
    invalid_per_batch: int = 0
    seed: int = 42

    def __post_init__(self) -> None:
        if self.start.tzinfo is None or self.start.utcoffset() is None:
            raise ValueError("start must be timezone-aware")
        for name in (
            "number_of_customers",
            "number_of_products",
            "batch_interval_minutes",
            "lifecycle_batches",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be greater than zero")
        for name in (
            "tickets_per_batch",
            "late_delay_minutes",
            "malformed_per_batch",
            "invalid_per_batch",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must be non-negative")
        for name in ("duplicate_rate", "late_rate"):
            if not 0 <= getattr(self, name) <= 1:
                raise ValueError(f"{name} must be between 0 and 1")

    @property
    def interval(self) -> timedelta:
        return timedelta(minutes=self.batch_interval_minutes)

    @property
    def lookback_batches(self) -> int:
        """Batches whose tickets can still deliver events: lifecycle, lateness, one duplicate."""
        return (
            self.lifecycle_batches
            + math.ceil(self.late_delay_minutes / self.batch_interval_minutes)
            + 1
        )

    def batch_of(self, moment: datetime) -> int:
        """1-based batch whose event-time window contains ``moment``."""
        return int((moment - self.start) / self.interval) + 1


def load_event_stream_config(path: Path | str) -> EventStreamConfig:
    source = Path(path)
    if not source.is_file():
        raise FileNotFoundError(f"Event stream profile not found: {source}")
    with source.open("rb") as handle:
        values: dict[str, Any] = tomllib.load(handle)
    start = datetime.fromisoformat(str(values.pop("start")).replace("Z", "+00:00"))
    return EventStreamConfig(**values, start=start)


@dataclass(frozen=True)
class _Delivery:
    batch: int
    record: Record


def _event(
    ticket: dict[str, str], sequence: int, event_type: str, event_time: datetime, **values: str
) -> Record:
    record: Record = dict.fromkeys(field.name for field in TICKET_EVENTS.fields)
    record.update(
        {
            "event_id": f"EVT-{ticket['ticket_id'][4:]}-{sequence:02d}",
            "ticket_id": ticket["ticket_id"],
            "customer_id": ticket["customer_id"],
            "product_id": ticket["product_id"],
            "event_type": event_type,
            "event_time": _iso_timestamp(event_time),
            **values,
        }
    )
    return record


def ticket_lifecycle(config: EventStreamConfig, batch: int, index: int) -> list[Record]:
    """Events for one streamed ticket created in ``batch``; a pure function of its inputs.

    Lifecycles are bounded to ``lifecycle_batches`` so a batch file depends on a fixed lookback.
    Tickets whose lifecycle is cut by the bound stay open.
    """
    rng = random.Random(f"{config.seed}:ticket:{batch}:{index}")
    window_start = config.start + config.interval * (batch - 1)
    created = window_start + timedelta(seconds=rng.uniform(0, config.interval.total_seconds()))
    horizon = created + config.interval * config.lifecycle_batches - timedelta(microseconds=1)
    ticket = {
        "ticket_id": f"STK-{batch:06d}-{index:04d}",
        "customer_id": f"CUS-{rng.randint(1, config.number_of_customers):06d}",
        "product_id": f"PRD-{rng.randint(1, config.number_of_products):03d}",
    }
    priority = rng.choices(PRIORITIES, weights=(0.05, 0.20, 0.50, 0.25), k=1)[0]
    agent = f"AGT-{rng.randint(1, 80):04d}"
    steps: list[tuple[str, dict[str, str]]] = [("agent_assigned", {"agent_id": agent})]
    steps.append(("agent_replied", {"agent_id": agent}))
    if rng.random() < 0.4:
        steps += [
            ("status_changed", {"old_value": "open", "new_value": "pending_customer"}),
            ("customer_replied", {}),
        ]
    if rng.random() < 0.15:
        upgraded = PRIORITIES[max(0, PRIORITIES.index(priority) - 1)]
        steps.append(("priority_changed", {"old_value": priority, "new_value": upgraded}))
    if rng.random() < 0.08:
        steps.append(("engineering_escalated", {"agent_id": agent}))
    steps.append(("ticket_resolved", {"agent_id": agent}))
    if rng.random() < 0.1:
        steps += [("ticket_reopened", {}), ("ticket_resolved", {"agent_id": agent})]
    if rng.random() < 0.5:
        steps.append(("status_changed", {"old_value": "resolved", "new_value": "closed"}))

    payload = json.dumps({"channel": rng.choice(("email", "web", "chat", "api"))})
    events = [_event(ticket, 1, "ticket_created", created, new_value=priority, payload=payload)]
    moment = created
    for sequence, (event_type, values) in enumerate(steps, start=2):
        moment += timedelta(minutes=rng.expovariate(1 / 20))
        if moment > horizon:
            break
        events.append(_event(ticket, sequence, event_type, moment, **values))
    return events


def _deliveries(config: EventStreamConfig, batch: int, index: int) -> Iterator[_Delivery]:
    rng = random.Random(f"{config.seed}:delivery:{batch}:{index}")
    late = timedelta(minutes=config.late_delay_minutes)
    for record in ticket_lifecycle(config, batch, index):
        event_time = datetime.fromisoformat(str(record["event_time"]).replace("Z", "+00:00"))
        arrival = config.batch_of(
            event_time + late if rng.random() < config.late_rate else event_time
        )
        yield _Delivery(arrival, record)
        if rng.random() < config.duplicate_rate:
            yield _Delivery(arrival + 1, record)


def _noise(config: EventStreamConfig, batch: int) -> list[str]:
    """Injected quality failures: unparseable lines, then parseable but invalid records."""
    rng = random.Random(f"{config.seed}:noise:{batch}")
    lines = [
        rng.choice(('{"event_id": "EVT-TRUNCATED', "not json", "[1, 2, 3]", "{}}"))
        for _ in range(config.malformed_per_batch)
    ]
    ticket = {
        "ticket_id": f"STK-{batch:06d}-9999",
        "customer_id": "CUS-000001",
        "product_id": "PRD-001",
    }
    moment = config.start + config.interval * (batch - 1)
    failures: tuple[tuple[str, str | None], ...] = (
        ("customer_id", "CUS-UNKNOWN"),
        ("new_value", "URGENT"),
        ("event_time", "not-a-timestamp"),
        ("ticket_id", None),
        ("event_type", "teleported"),
    )
    for index in range(config.invalid_per_batch):
        field, value = failures[index % len(failures)]
        record = _event(ticket, 50 + index, "priority_changed", moment, new_value="P2")
        record["event_id"] = f"BAD-{batch:06d}-{index:04d}"
        record[field] = value
        lines.append(json.dumps(record, sort_keys=True))
    return lines


def batch_lines(config: EventStreamConfig, batch: int) -> list[str]:
    """JSON Lines for one batch file, identical every time the batch is generated.

    Events arrive in the batch covering their event time, or later when chosen as late; a
    duplicate is re-delivered in the following batch. Lines are shuffled so a file is not in
    event-time order.
    """
    if batch < 1:
        raise ValueError("batch must be at least 1")
    records = [
        delivery.record
        for created in range(max(1, batch - config.lookback_batches), batch + 1)
        for index in range(1, config.tickets_per_batch + 1)
        for delivery in _deliveries(config, created, index)
        if delivery.batch == batch
    ]
    lines = [json.dumps(record, sort_keys=True) for record in records] + _noise(config, batch)
    random.Random(f"{config.seed}:order:{batch}").shuffle(lines)
    return lines


def existing_batches(directory: Path) -> list[int]:
    if not directory.is_dir():
        return []
    return sorted(
        int(match.group(1))
        for match in (BATCH_FILE.fullmatch(path.name) for path in directory.iterdir())
        if match
    )


def write_event_batches(
    config: EventStreamConfig, output_dir: Path | str, *, batches: int
) -> list[Path]:
    """Append ``batches`` files after the highest existing batch number.

    Each file is written under a hidden temporary name and renamed into place, so the stream
    source (which ignores names starting with ``.``) never reads a partial file.
    """
    if batches < 0:
        raise ValueError("batches must be non-negative")
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    first = (existing_batches(directory) or [0])[-1] + 1
    written = []
    for batch in range(first, first + batches):
        path = directory / f"events_{batch:06d}.json"
        temporary = directory / f".{path.name}.tmp"
        temporary.write_text("".join(f"{line}\n" for line in batch_lines(config, batch)))
        os.replace(temporary, path)
        written.append(path)
    return written


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", type=Path, required=True, help="Event stream TOML profile")
    parser.add_argument("--output", type=Path, required=True, help="Stream source directory")
    parser.add_argument("--batches", type=int, default=1, help="Number of files to append")
    arguments = parser.parse_args(argv)
    config = load_event_stream_config(arguments.profile)
    for path in write_event_batches(config, arguments.output, batches=arguments.batches):
        print(path)
    return 0
