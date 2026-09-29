"""Typed, environment-aware application configuration."""

from __future__ import annotations

import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any


class Environment(StrEnum):
    """Supported logical deployment environments."""

    DEV = "dev"
    PROD = "prod"


@dataclass(frozen=True)
class StorageSettings:
    """Unity Catalog Volume paths used by pipelines."""

    landing_path: str
    event_stream_path: str
    checkpoint_path: str


@dataclass(frozen=True)
class Settings:
    """Validated settings shared by local code and Databricks jobs."""

    environment: Environment
    catalog: str
    timezone: str
    late_event_tolerance_minutes: int
    risk_threshold: float
    ai_max_investigations_per_run: int
    pipeline_run_id: str | None
    storage: StorageSettings

    def __post_init__(self) -> None:
        if self.timezone != "UTC":
            raise ValueError("Stored timestamps must use UTC")
        if self.late_event_tolerance_minutes < 0:
            raise ValueError("late_event_tolerance_minutes must be non-negative")
        if not 0 <= self.risk_threshold <= 1:
            raise ValueError("risk_threshold must be between 0 and 1")
        if self.ai_max_investigations_per_run < 0:
            raise ValueError("ai_max_investigations_per_run must be non-negative")
        if not self.catalog.startswith("support_"):
            raise ValueError("catalog must start with 'support_'")


def _read_toml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    with path.open("rb") as handle:
        return tomllib.load(handle)


def _deep_merge(base: dict[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        current = result.get(key)
        if isinstance(current, dict) and isinstance(value, Mapping):
            result[key] = _deep_merge(current, value)
        else:
            result[key] = value
    return result


def load_settings(
    environment: Environment | str | None = None,
    *,
    config_dir: Path | str | None = None,
    environ: Mapping[str, str] | None = None,
) -> Settings:
    """Load base and environment TOML files, then apply runtime metadata.

    Environment variables select an environment and provide ephemeral values; secrets are
    intentionally not represented in this object.
    """

    values = os.environ if environ is None else environ
    selected = Environment(environment or values.get("SUPPORT_OPS_ENV", Environment.DEV))
    directory = Path(config_dir or values.get("SUPPORT_OPS_CONFIG_DIR", "config"))
    merged = _deep_merge(
        _read_toml(directory / "base.toml"), _read_toml(directory / f"{selected}.toml")
    )

    configured_environment = Environment(str(merged["environment"]))
    if configured_environment is not selected:
        raise ValueError(
            f"Environment mismatch: selected {selected.value}, file declares "
            f"{configured_environment.value}"
        )

    catalog = str(merged["catalog"])
    storage_values = merged["storage"]
    if not isinstance(storage_values, Mapping):
        raise ValueError("storage configuration must be a mapping")

    def path(name: str) -> str:
        return str(storage_values[name]).format(catalog=catalog)

    return Settings(
        environment=selected,
        catalog=catalog,
        timezone=str(merged["timezone"]),
        late_event_tolerance_minutes=int(merged["late_event_tolerance_minutes"]),
        risk_threshold=float(merged["risk_threshold"]),
        ai_max_investigations_per_run=int(merged["ai_max_investigations_per_run"]),
        pipeline_run_id=values.get("SUPPORT_OPS_PIPELINE_RUN_ID"),
        storage=StorageSettings(
            landing_path=path("landing_path"),
            event_stream_path=path("event_stream_path"),
            checkpoint_path=path("checkpoint_path"),
        ),
    )
