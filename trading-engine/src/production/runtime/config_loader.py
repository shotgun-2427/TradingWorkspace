from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml

from common.model import Config
from common.utils import read_config_yaml

VALID_PROFILES = {"paper", "live"}
SRC_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_CONFIG_DIR = SRC_ROOT / "production" / "pipeline" / "configs"
SIMULATION_OVERRIDES_PATH = SRC_ROOT / "production" / "simulations" / "overrides.yaml"


def execution_config_path(profile: str) -> Path:
    """Return the canonical execution config path for a profile."""
    _validate_profile(profile)
    return PIPELINE_CONFIG_DIR / f"{profile}.yaml"


def simulation_overrides_path() -> Path:
    """Return the canonical simulation overrides config path."""
    return SIMULATION_OVERRIDES_PATH


def load_execution_profile_config(profile: str) -> Config:
    """Load execution config as a typed Config object for a profile."""
    path = execution_config_path(profile)
    return read_config_yaml(str(path))


def load_execution_profile_config_dict(profile: str) -> dict[str, Any]:
    """Load execution config as a raw dictionary for a profile."""
    path = execution_config_path(profile)
    return _load_yaml_dict(path)


def load_simulation_overrides(
    overrides_path: Path | None = None,
) -> dict[str, Any]:
    """Load simulation override dictionary from the configured path."""
    path = overrides_path or simulation_overrides_path()
    if not path.exists():
        raise FileNotFoundError(f"Simulation overrides file not found: {path}")
    return _load_yaml_dict(path)


def load_simulation_profile_config(
    profile: str,
    overrides_path: Path | None = None,
) -> Config:
    """Load execution config and apply simulation overrides for one profile."""
    _validate_profile(profile)
    execution_config = load_execution_profile_config_dict(profile)
    overrides = load_simulation_overrides(overrides_path=overrides_path)
    profile_overrides = (overrides.get("profiles") or {}).get(profile, {})
    merged = _deep_merge(execution_config, profile_overrides)
    return Config(**merged)


def _validate_profile(profile: str) -> None:
    """Validate that the supplied profile is supported."""
    if profile not in VALID_PROFILES:
        raise ValueError(
            f"Invalid profile '{profile}'. Expected one of: {sorted(VALID_PROFILES)}."
        )


def _load_yaml_dict(path: Path) -> dict[str, Any]:
    """Load YAML content and enforce a top-level dictionary payload."""
    with path.open("r", encoding="utf-8") as file:
        loaded = yaml.safe_load(file) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"YAML file must contain a dictionary at top level: {path}")
    return loaded


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively merge override values into a copied base dictionary."""
    merged = copy.deepcopy(base)
    for key, value in (override or {}).items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged
