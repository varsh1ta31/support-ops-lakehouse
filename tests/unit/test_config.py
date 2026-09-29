from pathlib import Path

import pytest

from support_ops.config import Environment, Settings, StorageSettings, load_settings

PROJECT_ROOT = Path(__file__).parents[2]


@pytest.mark.parametrize(
    ("environment", "catalog", "checkpoint_suffix"),
    [(Environment.DEV, "support_dev", "/dev"), (Environment.PROD, "support_prod", "/prod")],
)
def test_load_settings(environment: Environment, catalog: str, checkpoint_suffix: str) -> None:
    settings = load_settings(environment, config_dir=PROJECT_ROOT / "config", environ={})

    assert settings.environment is environment
    assert settings.catalog == catalog
    assert settings.storage.landing_path == f"/Volumes/{catalog}/raw/landing"
    assert settings.storage.checkpoint_path.endswith(checkpoint_suffix)


def test_environment_can_come_from_environment_mapping() -> None:
    settings = load_settings(
        config_dir=PROJECT_ROOT / "config",
        environ={"SUPPORT_OPS_ENV": "prod", "SUPPORT_OPS_PIPELINE_RUN_ID": "run-42"},
    )

    assert settings.environment is Environment.PROD
    assert settings.pipeline_run_id == "run-42"


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("timezone", "America/New_York", "UTC"),
        ("late_event_tolerance_minutes", -1, "non-negative"),
        ("risk_threshold", 1.1, "between 0 and 1"),
        ("ai_max_investigations_per_run", -1, "non-negative"),
        ("catalog", "other", "support_"),
    ],
)
def test_settings_reject_invalid_values(field: str, value: object, message: str) -> None:
    values: dict[str, object] = {
        "environment": Environment.DEV,
        "catalog": "support_dev",
        "timezone": "UTC",
        "late_event_tolerance_minutes": 60,
        "risk_threshold": 0.7,
        "ai_max_investigations_per_run": 20,
        "pipeline_run_id": None,
        "storage": StorageSettings("landing", "events", "checkpoints"),
    }
    values[field] = value

    with pytest.raises(ValueError, match=message):
        Settings(**values)  # type: ignore[arg-type]


def test_missing_configuration_is_explicit(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match=r"base\.toml"):
        load_settings(config_dir=tmp_path, environ={})
