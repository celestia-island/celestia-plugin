"""Configuration loading — env-first, optional JSON file, stdlib only.

Policy alignment (workspace §3.7): bootstrap roots (DSNs, secrets) live in
per-instance 0600 env files — never in a repo tree, never in a database.
This loader reads an env prefix (primary) and, optionally, one JSON file
(deploy-shape convenience); env wins on conflict. It performs no silent
fallbacks: a required key that is missing raises.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

log = logging.getLogger("celestia_plugin.config")


class ConfigError(Exception):
    pass


def load_config(env_prefix: str = "CELESTIA_PLUGIN_", file_path: str | None = None) -> dict[str, Any]:
    """Overlay order: file (if given) → env (env wins). Keys from the env
    are lower-cased with the prefix stripped; values are JSON-parsed when
    they parse, raw strings otherwise."""
    cfg: dict[str, Any] = {}
    if file_path is not None:
        cfg.update(_load_file(file_path))
    for key, value in os.environ.items():
        if not key.startswith(env_prefix) or len(key) <= len(env_prefix):
            continue
        short = key[len(env_prefix) :].lower()
        cfg[short] = _parse_scalar(value)
    return cfg


def require(cfg: dict[str, Any], key: str, expected_type: type | tuple[type, ...]) -> Any:
    if key not in cfg:
        raise ConfigError(f"missing required config key: {key}")
    value = cfg[key]
    if not isinstance(value, expected_type):
        raise ConfigError(
            f"config key {key} has wrong type: {type(value).__name__} (expected "
            f"{expected_type if isinstance(expected_type, type) else '/'.join(t.__name__ for t in expected_type)})"
        )
    return value


def _load_file(file_path: str) -> dict[str, Any]:
    path = Path(file_path)
    if not path.is_file():
        raise ConfigError(f"config file not found: {file_path}")
    mode = path.stat().st_mode & 0o777
    if mode & 0o077:
        log.warning(
            "config file %s is group/world-readable (%o) — bootstrap secrets belong in 0600 files",
            file_path,
            mode,
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"config file {file_path} is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ConfigError(f"config file {file_path} must contain a JSON object")
    return payload


def _parse_scalar(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw
