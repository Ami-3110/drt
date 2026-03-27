"""Credential and profile management — dbt profiles.yml pattern.

Credentials never live in drt_project.yml (which is Git-safe).
They live in ~/.drt/profiles.yml (outside version control).

Resolution order for secret values:
  explicit value → env var → raise

Example ~/.drt/profiles.yml:
    dev:
      type: bigquery
      project: my-gcp-project
      dataset: analytics
      method: application_default

    prod:
      type: bigquery
      project: my-gcp-project
      dataset: analytics
      method: keyfile
      keyfile: ~/.config/gcloud/service_account.json
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import yaml


@dataclass
class ProfileConfig:
    """Resolved source profile — credentials ready to use."""

    type: Literal["bigquery"]
    project: str
    dataset: str
    method: Literal["application_default", "keyfile"] = "application_default"
    keyfile: str | None = None


def _config_dir(override: Path | None = None) -> Path:
    return override if override is not None else Path.home() / ".drt"


def load_profile(profile_name: str, config_dir: Path | None = None) -> ProfileConfig:
    """Load a named profile from ~/.drt/profiles.yml.

    Args:
        profile_name: Key in profiles.yml (e.g. "dev", "prod").
        config_dir: Override ~/.drt for testing.

    Raises:
        FileNotFoundError: profiles.yml does not exist.
        KeyError: profile_name not found in profiles.yml.
        ValueError: Required fields missing or type unsupported.
    """
    profiles_path = _config_dir(config_dir) / "profiles.yml"
    if not profiles_path.exists():
        raise FileNotFoundError(
            f"profiles.yml not found at {profiles_path}. "
            "Run `drt init` to create it, or create it manually."
        )

    with profiles_path.open() as f:
        data = yaml.safe_load(f) or {}

    if profile_name not in data:
        available = ", ".join(data.keys()) or "(none)"
        raise KeyError(
            f"Profile '{profile_name}' not found in {profiles_path}. "
            f"Available profiles: {available}"
        )

    raw = data[profile_name]
    source_type = raw.get("type")
    if source_type != "bigquery":
        raise ValueError(f"Unsupported source type '{source_type}'. Currently only 'bigquery' is supported.")

    return ProfileConfig(
        type="bigquery",
        project=raw["project"],
        dataset=raw["dataset"],
        method=raw.get("method", "application_default"),
        keyfile=raw.get("keyfile"),
    )


def save_profile(
    profile_name: str,
    profile: ProfileConfig,
    config_dir: Path | None = None,
) -> Path:
    """Append or update a profile in ~/.drt/profiles.yml.

    Returns the path to the profiles file.
    """
    dir_path = _config_dir(config_dir)
    dir_path.mkdir(parents=True, exist_ok=True)
    profiles_path = dir_path / "profiles.yml"

    data: dict = {}
    if profiles_path.exists():
        with profiles_path.open() as f:
            data = yaml.safe_load(f) or {}

    entry: dict = {
        "type": profile.type,
        "project": profile.project,
        "dataset": profile.dataset,
        "method": profile.method,
    }
    if profile.keyfile:
        entry["keyfile"] = profile.keyfile

    data[profile_name] = entry

    with profiles_path.open("w") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True)

    return profiles_path


def resolve_env(value: str | None, env_var: str | None) -> str | None:
    """Resolve a secret value: explicit value takes priority, then env var.

    Returns None if both are unset.
    """
    if value is not None:
        return value
    if env_var is not None:
        return os.environ.get(env_var)
    return None
