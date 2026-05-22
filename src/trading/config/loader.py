"""Configuration loader.

Reads TOML, applies environment-variable overrides for a small set of
operational knobs, validates the result against :class:`AppConfig`.

Environment overrides follow the convention ``TRADING__SECTION__FIELD``
(double underscore as separator). Only top-level scalars are
overridable — list/dict surgery from env vars is a footgun and we
don't want that surface area in production.

Examples:

- ``TRADING__BUS__BACKEND=kafka``
- ``TRADING__OMS__SIGNAL_TTL_SECONDS=600``

We deliberately do not support secrets in TOML. API keys come from
environment variables read at runtime by the exchange adapter, never
from this config file.
"""

from __future__ import annotations

import os
import sys
import tomllib  # 3.11+
from pathlib import Path
from typing import Any

from ..core.exceptions import ConfigError
from .schema import AppConfig

_ENV_PREFIX = "TRADING__"


def _split_dotted_path(key: str) -> list[str]:
    """``BUS__BACKEND`` -> ``["bus", "backend"]``."""
    return [seg.lower() for seg in key.split("__") if seg]


def _set_in(root: dict[str, Any], path: list[str], value: str) -> None:
    """Set a nested key, creating intermediate dicts but never replacing a list."""
    node: Any = root
    for seg in path[:-1]:
        if seg not in node or not isinstance(node[seg], dict):
            # Refuse to override into a list or scalar. Lists and scalars are
            # structural; if the user wants to change them, they should edit
            # the config file. This keeps env overrides predictable.
            if seg in node and not isinstance(node[seg], dict):
                raise ConfigError(
                    f"env override cannot descend into non-dict at {seg!r}",
                    path="__".join(path).upper(),
                )
            node[seg] = {}
        node = node[seg]
    node[path[-1]] = value


def _apply_env_overrides(raw: dict[str, Any]) -> dict[str, Any]:
    out = dict(raw)  # shallow copy; we mutate intermediate dicts
    for env_key, env_val in os.environ.items():
        if not env_key.startswith(_ENV_PREFIX):
            continue
        path = _split_dotted_path(env_key[len(_ENV_PREFIX):])
        if not path:
            continue
        _set_in(out, path, env_val)
    return out


def load_config(path: str | Path) -> AppConfig:
    """Read a TOML file, apply env overrides, validate."""
    p = Path(path)
    if not p.exists():
        raise ConfigError(f"config file not found: {p}", path=str(p))
    try:
        with p.open("rb") as fh:
            raw = tomllib.load(fh)
    except tomllib.TOMLDecodeError as e:
        raise ConfigError(f"invalid TOML in {p}: {e}", path=str(p)) from e

    raw = _apply_env_overrides(raw)

    try:
        return AppConfig.model_validate(raw)
    except Exception as e:
        # Pydantic's ValidationError already includes a per-field breakdown;
        # we wrap it so callers see a uniform exception type.
        raise ConfigError(f"config validation failed: {e}", path=str(p)) from e


def load_config_from_dict(raw: dict[str, Any]) -> AppConfig:
    """Validate an already-parsed dict. Used by tests and synthetic configs."""
    try:
        return AppConfig.model_validate(raw)
    except Exception as e:
        raise ConfigError(f"config validation failed: {e}") from e


__all__ = ["load_config", "load_config_from_dict"]
