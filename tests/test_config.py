"""Tests for config module."""

import json
from pathlib import Path

import pytest

from sqlbackup.config import DbConfig, load_config
from sqlbackup.exceptions import ConfigError


class TestDbConfig:
    def test_creates_frozen_dataclass(self) -> None:
        cfg = DbConfig(host="localhost", port=3306, user="root", password="secret", database="mydb")
        assert cfg.host == "localhost"
        assert cfg.port == 3306
        assert cfg.user == "root"
        assert cfg.password == "secret"
        assert cfg.database == "mydb"

    def test_is_frozen(self) -> None:
        cfg = DbConfig(host="localhost", port=3306, user="root", password="secret", database="mydb")
        with pytest.raises(AttributeError):
            cfg.host = "other"  # type: ignore[misc]


class TestLoadConfig:
    def test_loads_valid_config(
        self, tmp_config_dir: Path, sample_config: dict[str, object]
    ) -> None:
        config_file = tmp_config_dir / "mydb.json"
        config_file.write_text(json.dumps(sample_config))

        cfg = load_config("mydb", config_dir=tmp_config_dir)
        assert cfg.host == "localhost"
        assert cfg.port == 3306
        assert cfg.user == "testuser"
        assert cfg.password == "testpass"
        assert cfg.database == "testdb"

    def test_missing_file_raises(self, tmp_config_dir: Path) -> None:
        with pytest.raises(ConfigError, match="Config file not found"):
            load_config("nonexistent", config_dir=tmp_config_dir)

    def test_invalid_json_raises(self, tmp_config_dir: Path) -> None:
        bad_file = tmp_config_dir / "bad.json"
        bad_file.write_text("not json{{{")
        with pytest.raises(ConfigError, match="Invalid JSON"):
            load_config("bad", config_dir=tmp_config_dir)

    def test_missing_keys_raises(self, tmp_config_dir: Path) -> None:
        incomplete = tmp_config_dir / "incomplete.json"
        incomplete.write_text(json.dumps({"host": "localhost"}))
        with pytest.raises(ConfigError, match="Missing required keys"):
            load_config("incomplete", config_dir=tmp_config_dir)

    def test_default_config_dir(
        self,
        tmp_path: Path,
        sample_config: dict[str, object],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        config_dir = tmp_path / "configs"
        config_dir.mkdir()
        config_file = config_dir / "test.json"
        config_file.write_text(json.dumps(sample_config))

        monkeypatch.setattr("sqlbackup.config.CONFIG_BASE_DIR", tmp_path)
        cfg = load_config("test")
        assert cfg.database == "testdb"

    def test_extra_keys_ignored(
        self, tmp_config_dir: Path, sample_config: dict[str, object]
    ) -> None:
        sample_config["extra_key"] = "ignored"
        config_file = tmp_config_dir / "extra.json"
        config_file.write_text(json.dumps(sample_config))

        cfg = load_config("extra", config_dir=tmp_config_dir)
        assert cfg.database == "testdb"
