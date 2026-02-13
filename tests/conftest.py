"""Shared test fixtures."""

import json
from pathlib import Path

import pytest


@pytest.fixture()
def tmp_config_dir(tmp_path: Path) -> Path:
    """Create a temporary config directory."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    return config_dir


@pytest.fixture()
def sample_config() -> dict[str, object]:
    """Return a valid sample config dict."""
    return {
        "host": "localhost",
        "port": 3306,
        "user": "testuser",
        "password": "testpass",
        "database": "testdb",
    }


@pytest.fixture()
def sample_config_file(tmp_config_dir: Path, sample_config: dict[str, object]) -> Path:
    """Write a sample config JSON file and return its path."""
    config_file = tmp_config_dir / "test.json"
    config_file.write_text(json.dumps(sample_config))
    return config_file
