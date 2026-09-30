from __future__ import annotations

import json
from collections import Counter
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from support_ops.quality.validation import Disposition, ValidationContext, validate_record
from support_ops.synthetic.events import (
    EventStreamConfig,
    batch_lines,
    existing_batches,
    load_event_stream_config,
    main,
    ticket_lifecycle,
    write_event_batches,
)
from support_ops.transformations.ticket_state import TicketEvent, reconstruct

START = datetime(2026, 1, 1, tzinfo=UTC)
CONFIG = EventStreamConfig(
    number_of_customers=10,
    number_of_products=3,
    start=START,
    tickets_per_batch=5,
    duplicate_rate=0.1,
    late_rate=0.1,
    late_delay_minutes=60,
    malformed_per_batch=1,
    invalid_per_batch=5,
)
CONTEXT = ValidationContext(
    {f"CUS-{index:06d}" for index in range(1, 11)},
    {f"PRD-{index:03d}" for index in range(1, 4)},
    datetime(2027, 1, 1, tzinfo=UTC),
)


def parsed(lines: list[str]) -> list[dict[str, object]]:
    records = []
    for line in lines:
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict):
            records.append(value)
    return records


def test_batches_are_reproducible_and_seed_dependent() -> None:
    assert batch_lines(CONFIG, 7) == batch_lines(CONFIG, 7)
    assert batch_lines(CONFIG, 7) != batch_lines(replace(CONFIG, seed=1), 7)
    with pytest.raises(ValueError, match="at least 1"):
        batch_lines(CONFIG, 0)


def test_stream_events_validate_and_replay_without_rejected_transitions() -> None:
    horizon = 60
    dispositions: Counter[str] = Counter()
    events: dict[str, list[TicketEvent]] = {}
    seen: set[str] = set()
    malformed = 0
    for batch in range(1, horizon + 1):
        lines = batch_lines(CONFIG, batch)
        records = parsed(lines)
        malformed += len(lines) - len(records)
        for raw in records:
            result = validate_record("ticket_events", raw, CONTEXT, processed_ids=seen)
            dispositions[result.disposition] += 1
            if result.disposition is Disposition.ACCEPTED:
                record = result.record
                seen.add(str(record["event_id"]))
                events.setdefault(str(record["ticket_id"]), []).append(
                    TicketEvent(
                        **{  # type: ignore[arg-type]
                            name: record[name] for name in TicketEvent.__dataclass_fields__
                        }
                    )
                )
    assert malformed == horizon * CONFIG.malformed_per_batch
    assert dispositions[Disposition.QUARANTINED] == horizon * CONFIG.invalid_per_batch
    assert dispositions[Disposition.DUPLICATE] > 0
    complete = [
        reconstruct(None, ticket_events, CONTEXT.now)
        for ticket_id, ticket_events in events.items()
        if int(ticket_id[4:10]) <= horizon - CONFIG.lookback_batches
    ]
    assert len(complete) == (horizon - CONFIG.lookback_batches) * CONFIG.tickets_per_batch
    assert all(state is not None and state.rejected_event_count == 0 for state in complete)
    statuses = Counter(state.status for state in complete if state is not None)
    assert statuses["resolved"] + statuses["closed"] > len(complete) * 0.8


def test_events_arrive_on_time_late_or_duplicated_within_the_lookback() -> None:
    arrivals: dict[str, list[int]] = {}
    for batch in range(1, 40):
        for record in parsed(batch_lines(CONFIG, batch)):
            arrivals.setdefault(str(record["event_id"]), []).append(batch)
    late = duplicated = 0
    for event_id, batches in arrivals.items():
        if event_id.startswith("BAD-"):
            continue
        ticket, sequence = event_id[4:15], int(event_id[16:])
        created = int(ticket[:6])
        expected = ticket_lifecycle(CONFIG, created, int(ticket[7:]))[sequence - 1]
        on_time = CONFIG.batch_of(
            datetime.fromisoformat(str(expected["event_time"]).replace("Z", "+00:00"))
        )
        # A 60-minute delay is exactly four 15-minute batches.
        assert batches[0] in (on_time, on_time + 4)
        assert batches[0] - created <= CONFIG.lookback_batches
        late += batches[0] != on_time
        duplicated += len(batches) > 1
        if len(batches) > 1:
            assert batches == [batches[0], batches[0] + 1]
    assert late and duplicated


def test_lifecycle_is_bounded_and_starts_with_creation() -> None:
    for index in range(1, 30):
        events = ticket_lifecycle(CONFIG, 3, index)
        assert events[0]["event_type"] == "ticket_created"
        assert events[0]["new_value"] in ("P1", "P2", "P3", "P4")
        times = [datetime.fromisoformat(str(event["event_time"])) for event in events]
        assert times == sorted(times)
        assert CONFIG.batch_of(times[0]) == 3
        assert times[-1] - times[0] < CONFIG.interval * CONFIG.lifecycle_batches


def test_writer_appends_atomically_named_files(tmp_path: Path) -> None:
    output = tmp_path / "events"
    first = write_event_batches(CONFIG, output, batches=2)
    (output / ".events_000003.json.tmp").write_text("partial")
    second = write_event_batches(CONFIG, output, batches=1)
    assert [path.name for path in [*first, *second]] == [
        "events_000001.json",
        "events_000002.json",
        "events_000003.json",
    ]
    assert existing_batches(output) == [1, 2, 3]
    assert second[0].read_text() == "".join(f"{line}\n" for line in batch_lines(CONFIG, 3))
    assert not (output / ".events_000003.json.tmp").exists()
    assert existing_batches(tmp_path / "missing") == []
    with pytest.raises(ValueError, match="non-negative"):
        write_event_batches(CONFIG, output, batches=-1)


def test_profile_and_cli(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    config = load_event_stream_config("config/generation/stream.toml")
    assert config.start == START and config.number_of_customers == 100
    assert main(["--profile", "config/generation/stream.toml", "--output", str(tmp_path)]) == 0
    assert capsys.readouterr().out.strip().endswith("events_000001.json")
    with pytest.raises(FileNotFoundError):
        load_event_stream_config(tmp_path / "missing.toml")


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"start": datetime(2026, 1, 1)}, "timezone-aware"),
        ({"batch_interval_minutes": 0}, "greater than zero"),
        ({"invalid_per_batch": -1}, "non-negative"),
        ({"late_rate": 1.5}, "between 0 and 1"),
    ],
)
def test_invalid_configuration_is_rejected(changes: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        replace(CONFIG, **changes)  # type: ignore[arg-type]


def test_lookback_covers_lifecycle_lateness_and_duplicate() -> None:
    assert CONFIG.lookback_batches == 16 + 4 + 1
    assert CONFIG.batch_of(START + timedelta(minutes=14, seconds=59)) == 1
    assert CONFIG.batch_of(START + timedelta(minutes=15)) == 2
