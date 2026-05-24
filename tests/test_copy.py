"""Tests for copy module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from sqlbackup.config import DbConfig
from sqlbackup.copy import copy_database
from sqlbackup.exceptions import PushError


@pytest.fixture()
def source_cfg() -> DbConfig:
    return DbConfig(host="src", port=3306, user="u", password="p", database="src_db")


@pytest.fixture()
def target_cfg() -> DbConfig:
    return DbConfig(host="tgt", port=3306, user="u", password="p", database="tgt_db")


class TestCopyDatabase:
    def test_calls_backup_then_push(
        self, source_cfg: DbConfig, target_cfg: DbConfig
    ) -> None:
        observed: dict[str, object] = {}

        def fake_backup(cfg: DbConfig, path: Path, **kwargs: object) -> Path:
            path.write_text("-- dump\n", encoding="utf-8")
            observed["backup_cfg"] = cfg
            observed["backup_path"] = path
            observed["backup_kwargs"] = kwargs
            return path

        def fake_push(cfg: DbConfig, path: Path, *, force: bool = False) -> None:
            observed["push_cfg"] = cfg
            observed["push_path"] = path
            observed["push_force"] = force
            observed["push_exists_at_call"] = path.exists()

        with (
            patch("sqlbackup.copy.backup_database", side_effect=fake_backup),
            patch("sqlbackup.copy.push_database", side_effect=fake_push),
        ):
            copy_database(source_cfg, target_cfg)

        assert observed["backup_cfg"] is source_cfg
        assert observed["push_cfg"] is target_cfg
        assert observed["backup_path"] == observed["push_path"]
        assert observed["push_exists_at_call"] is True
        assert observed["push_force"] is False
        # Temp file cleaned up after push.
        assert not Path(str(observed["backup_path"])).exists()

    def test_passes_filters_and_force(
        self, source_cfg: DbConfig, target_cfg: DbConfig
    ) -> None:
        captured: dict[str, object] = {}

        def fake_backup(cfg: DbConfig, path: Path, **kwargs: object) -> Path:
            path.write_text("-- dump\n", encoding="utf-8")
            captured["backup_kwargs"] = kwargs
            return path

        def fake_push(cfg: DbConfig, path: Path, *, force: bool = False) -> None:
            captured["force"] = force

        with (
            patch("sqlbackup.copy.backup_database", side_effect=fake_backup),
            patch("sqlbackup.copy.push_database", side_effect=fake_push),
        ):
            copy_database(
                source_cfg,
                target_cfg,
                includes=["users"],
                excludes=None,
                force=True,
            )

        assert captured["backup_kwargs"] == {"includes": ["users"], "excludes": None}
        assert captured["force"] is True

    def test_cleans_temp_file_when_push_fails(
        self, source_cfg: DbConfig, target_cfg: DbConfig
    ) -> None:
        captured_path: dict[str, Path] = {}

        def fake_backup(cfg: DbConfig, path: Path, **kwargs: object) -> Path:
            path.write_text("-- dump\n", encoding="utf-8")
            captured_path["p"] = path
            return path

        def fake_push(cfg: DbConfig, path: Path, *, force: bool = False) -> None:
            raise PushError("boom")

        with (
            patch("sqlbackup.copy.backup_database", side_effect=fake_backup),
            patch("sqlbackup.copy.push_database", side_effect=fake_push),
            pytest.raises(PushError, match="boom"),
        ):
            copy_database(source_cfg, target_cfg)

        assert not captured_path["p"].exists()

    def test_cleans_temp_file_when_backup_fails(
        self, source_cfg: DbConfig, target_cfg: DbConfig
    ) -> None:
        from sqlbackup.exceptions import BackupError

        def fake_backup(cfg: DbConfig, path: Path, **kwargs: object) -> Path:
            raise BackupError("dump fail")

        seen: list[Path] = []

        def fake_push(cfg: DbConfig, path: Path, *, force: bool = False) -> None:
            seen.append(path)

        with (
            patch("sqlbackup.copy.backup_database", side_effect=fake_backup),
            patch("sqlbackup.copy.push_database", side_effect=fake_push),
            pytest.raises(BackupError, match="dump fail"),
        ):
            copy_database(source_cfg, target_cfg)

        assert seen == []
