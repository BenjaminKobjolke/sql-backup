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
            patch("sqlbackup.cli.backup_database") as mock_backup,
            patch(
                "sys.argv",
                ["sqlbackup", "--backup", "--config", "mydb", "--path", str(output)],
            ),
        ):
            main()

        mock_load.assert_called_once_with("mydb")
        mock_backup.assert_called_once_with(config, output)

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
        mock_push.assert_called_once_with(config, sql_file)

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
