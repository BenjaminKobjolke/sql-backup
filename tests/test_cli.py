"""Tests for CLI module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sqlbackup.cli import main


class TestCLI:
    def test_backup_dispatches(self, tmp_path: Path) -> None:
        config = MagicMock()
        output = tmp_path / "dump.sql"

        with (
            patch("sqlbackup.cli.load_config", return_value=config) as mock_load,
            patch("sqlbackup.cli.backup_database", return_value=output) as mock_backup,
            patch(
                "sys.argv",
                ["sqlbackup", "--backup", "--config", "mydb", "--path", str(output)],
            ),
        ):
            main()

        mock_load.assert_called_once_with("mydb")
        mock_backup.assert_called_once_with(
            config, output, incremental=None, zip=False, includes=None, excludes=None
        )

    def test_push_dispatches(self, tmp_path: Path) -> None:
        config = MagicMock()
        sql_file = tmp_path / "dump.sql"

        with (
            patch("sqlbackup.cli.load_config", return_value=config) as mock_load,
            patch("sqlbackup.cli.push_database") as mock_push,
            patch(
                "sys.argv",
                ["sqlbackup", "--push", "--config", "mydb", "--path", str(sql_file)],
            ),
        ):
            main()

        mock_load.assert_called_once_with("mydb")
        mock_push.assert_called_once_with(config, sql_file, force=False)

    def test_no_action_exits(self) -> None:
        with (
            patch("sys.argv", ["sqlbackup", "--config", "mydb", "--path", "dump.sql"]),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 2

    def test_both_actions_exits(self) -> None:
        with (
            patch(
                "sys.argv",
                ["sqlbackup", "--backup", "--push", "--config", "mydb", "--path", "dump.sql"],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 2

    def test_missing_config_exits(self) -> None:
        with (
            patch("sys.argv", ["sqlbackup", "--backup", "--path", "dump.sql"]),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 2

    def test_missing_path_exits(self) -> None:
        with (
            patch("sys.argv", ["sqlbackup", "--backup", "--config", "mydb"]),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 2

    def test_config_error_exits(self) -> None:
        from sqlbackup.exceptions import ConfigError

        with (
            patch("sqlbackup.cli.load_config", side_effect=ConfigError("bad config")),
            patch(
                "sys.argv",
                ["sqlbackup", "--backup", "--config", "bad", "--path", "dump.sql"],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 1

    def test_backup_error_exits(self) -> None:
        from sqlbackup.exceptions import BackupError

        with (
            patch("sqlbackup.cli.load_config", return_value=MagicMock()),
            patch(
                "sqlbackup.cli.backup_database",
                side_effect=BackupError("backup failed"),
            ),
            patch(
                "sys.argv",
                ["sqlbackup", "--backup", "--config", "mydb", "--path", "dump.sql"],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 1

    def test_push_error_exits(self) -> None:
        from sqlbackup.exceptions import PushError

        with (
            patch("sqlbackup.cli.load_config", return_value=MagicMock()),
            patch(
                "sqlbackup.cli.push_database",
                side_effect=PushError("push failed"),
            ),
            patch(
                "sys.argv",
                ["sqlbackup", "--push", "--config", "mydb", "--path", "dump.sql"],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 1

    def test_incremental_passes_to_backup(self, tmp_path: Path) -> None:
        config = MagicMock()
        output = tmp_path / "dump.sql"

        with (
            patch("sqlbackup.cli.load_config", return_value=config),
            patch("sqlbackup.cli.backup_database", return_value=output) as mock_backup,
            patch(
                "sys.argv",
                [
                    "sqlbackup",
                    "--backup",
                    "--config",
                    "mydb",
                    "--path",
                    str(output),
                    "--incremental",
                    "5",
                ],
            ),
        ):
            main()

        mock_backup.assert_called_once_with(
            config, output, incremental=5, zip=False, includes=None, excludes=None
        )

    def test_zip_passes_to_backup(self, tmp_path: Path) -> None:
        config = MagicMock()
        output = tmp_path / "dump.sql"

        with (
            patch("sqlbackup.cli.load_config", return_value=config),
            patch("sqlbackup.cli.backup_database", return_value=output) as mock_backup,
            patch(
                "sys.argv",
                [
                    "sqlbackup",
                    "--backup",
                    "--config",
                    "mydb",
                    "--path",
                    str(output),
                    "--zip",
                ],
            ),
        ):
            main()

        mock_backup.assert_called_once_with(
            config, output, incremental=None, zip=True, includes=None, excludes=None
        )

    def test_zip_with_push_rejected(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "dump.sql"
        with (
            patch(
                "sys.argv",
                [
                    "sqlbackup",
                    "--push",
                    "--config",
                    "mydb",
                    "--path",
                    str(sql_file),
                    "--zip",
                ],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 2

    def test_include_tables_passes_to_backup(self, tmp_path: Path) -> None:
        config = MagicMock()
        output = tmp_path / "dump.sql"

        with (
            patch("sqlbackup.cli.load_config", return_value=config),
            patch("sqlbackup.cli.backup_database", return_value=output) as mock_backup,
            patch(
                "sys.argv",
                [
                    "sqlbackup",
                    "--backup",
                    "--config",
                    "mydb",
                    "--path",
                    str(output),
                    "--include-table",
                    "users",
                    "--include-table",
                    "posts",
                ],
            ),
        ):
            main()

        mock_backup.assert_called_once_with(
            config,
            output,
            incremental=None,
            zip=False,
            includes=["users", "posts"],
            excludes=None,
        )

    def test_exclude_tables_passes_to_backup(self, tmp_path: Path) -> None:
        config = MagicMock()
        output = tmp_path / "dump.sql"

        with (
            patch("sqlbackup.cli.load_config", return_value=config),
            patch("sqlbackup.cli.backup_database", return_value=output) as mock_backup,
            patch(
                "sys.argv",
                [
                    "sqlbackup",
                    "--backup",
                    "--config",
                    "mydb",
                    "--path",
                    str(output),
                    "--exclude-table",
                    "logs",
                ],
            ),
        ):
            main()

        mock_backup.assert_called_once_with(
            config,
            output,
            incremental=None,
            zip=False,
            includes=None,
            excludes=["logs"],
        )

    def test_include_and_exclude_rejected(self, tmp_path: Path) -> None:
        with (
            patch(
                "sys.argv",
                [
                    "sqlbackup",
                    "--backup",
                    "--config",
                    "mydb",
                    "--path",
                    "dump.sql",
                    "--include-table",
                    "a",
                    "--exclude-table",
                    "b",
                ],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 2

    def test_force_passes_to_push(self, tmp_path: Path) -> None:
        config = MagicMock()
        sql_file = tmp_path / "dump.sql"

        with (
            patch("sqlbackup.cli.load_config", return_value=config),
            patch("sqlbackup.cli.push_database") as mock_push,
            patch(
                "sys.argv",
                [
                    "sqlbackup",
                    "--push",
                    "--config",
                    "mydb",
                    "--path",
                    str(sql_file),
                    "--force",
                ],
            ),
        ):
            main()

        mock_push.assert_called_once_with(config, sql_file, force=True)

    def test_force_with_backup_rejected(self, tmp_path: Path) -> None:
        with (
            patch(
                "sys.argv",
                [
                    "sqlbackup",
                    "--backup",
                    "--config",
                    "mydb",
                    "--path",
                    "dump.sql",
                    "--force",
                ],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 2

    def test_filters_with_push_rejected(self, tmp_path: Path) -> None:
        with (
            patch(
                "sys.argv",
                [
                    "sqlbackup",
                    "--push",
                    "--config",
                    "mydb",
                    "--path",
                    "dump.sql",
                    "--include-table",
                    "users",
                ],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 2


class TestCopyCLI:
    def test_copy_dispatches(self) -> None:
        src = MagicMock()
        tgt = MagicMock()

        def load(name: str) -> MagicMock:
            return src if name == "prod" else tgt

        with (
            patch("sqlbackup.cli.load_config", side_effect=load) as mock_load,
            patch("sqlbackup.cli.copy_database") as mock_copy,
            patch(
                "sys.argv",
                ["sqlbackup", "--copy", "--source", "prod", "--target", "test"],
            ),
        ):
            main()

        assert mock_load.call_count == 2
        mock_copy.assert_called_once_with(
            src, tgt, includes=None, excludes=None, force=False
        )

    def test_copy_missing_source_exits(self) -> None:
        with (
            patch("sys.argv", ["sqlbackup", "--copy", "--target", "test"]),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 2

    def test_copy_missing_target_exits(self) -> None:
        with (
            patch("sys.argv", ["sqlbackup", "--copy", "--source", "prod"]),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 2

    def test_copy_rejects_config_or_path(self) -> None:
        with (
            patch(
                "sys.argv",
                [
                    "sqlbackup",
                    "--copy",
                    "--source",
                    "prod",
                    "--target",
                    "test",
                    "--config",
                    "x",
                ],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 2

    def test_copy_passes_filters_and_force(self) -> None:
        src = MagicMock()
        tgt = MagicMock()

        def load(name: str) -> MagicMock:
            return src if name == "prod" else tgt

        with (
            patch("sqlbackup.cli.load_config", side_effect=load),
            patch("sqlbackup.cli.copy_database") as mock_copy,
            patch(
                "sys.argv",
                [
                    "sqlbackup",
                    "--copy",
                    "--source",
                    "prod",
                    "--target",
                    "test",
                    "--exclude-table",
                    "logs",
                    "--force",
                ],
            ),
        ):
            main()

        mock_copy.assert_called_once_with(
            src, tgt, includes=None, excludes=["logs"], force=True
        )
