"""Database configuration loading."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from sqlbackup.constants import (
    CONFIG_DIR,
    CONFIG_EXT,
    ERR_CONFIG_INVALID_JSON,
    ERR_CONFIG_MISSING_KEYS,
    ERR_CONFIG_NOT_FOUND,
    REQUIRED_CONFIG_KEYS,
)
from sqlbackup.exceptions import ConfigError

CONFIG_BASE_DIR = Path.cwd()


@dataclass(frozen=True)
class DbConfig:
    """Database connection configuration."""

    host: str
    port: int
    user: str
    password: str
    database: str


def load_config(name: str, *, config_dir: Path | None = None) -> DbConfig:
    """Load a database config from a JSON file.

    Args:
        name: Config name (without .json extension).
        config_dir: Override config directory. Defaults to ./config/.
    """
    if config_dir is None:
        config_dir = CONFIG_BASE_DIR / CONFIG_DIR

    path = config_dir / f"{name}{CONFIG_EXT}"

    if not path.exists():
        raise ConfigError(ERR_CONFIG_NOT_FOUND.format(path=path))

    text = path.read_text(encoding="utf-8")
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ConfigError(ERR_CONFIG_INVALID_JSON.format(path=path)) from exc

    missing = REQUIRED_CONFIG_KEYS - set(data.keys())
    if missing:
        raise ConfigError(ERR_CONFIG_MISSING_KEYS.format(keys=", ".join(sorted(missing))))

    return DbConfig(
        host=data["host"],
        port=int(data["port"]),
        user=data["user"],
        password=data["password"],
        database=data["database"],
    )
