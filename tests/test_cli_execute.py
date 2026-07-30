"""Tests for the --execute CLI dispatch (kept separate from test_cli.py for size)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sqlbackup.cli import main


class TestExecuteCLI:
    def test_execute_dispatches(self, tmp_path: Path) -> None:
        config = MagicMock()
        sql_file = tmp_path / "revision.sql"

        with (
            patch("sqlbackup.cli.load_config", return_value=config) as mock_load,
            patch(
                "sqlbackup.cli.execute_from_path", return_value=(sql_file, None, 2)
            ) as mock_execute,
            patch(
                "sys.argv",
                ["sqlbackup", "--execute", "--config", "mydb", "--path", str(sql_file)],
            ),
        ):
            main()

        mock_load.assert_called_once_with("mydb")
        mock_execute.assert_called_once_with(
            config,
            sql_file,
            backup_path=None,
            incremental=None,
            zip=False,
            assume_yes=False,
            dry_run=False,
        )

    def test_execute_dry_run_dispatches(self, tmp_path: Path) -> None:
        config = MagicMock()
        sql_file = tmp_path / "revision.sql"

        with (
            patch("sqlbackup.cli.load_config", return_value=config),
            patch(
                "sqlbackup.cli.execute_from_path", return_value=(sql_file, None, 2)
            ) as mock_execute,
            patch(
                "sys.argv",
                [
                    "sqlbackup",
                    "--execute",
                    "--config",
                    "mydb",
                    "--path",
                    str(sql_file),
                    "--dry-run",
                ],
            ),
        ):
            main()

        mock_execute.assert_called_once_with(
            config,
            sql_file,
            backup_path=None,
            incremental=None,
            zip=False,
            assume_yes=False,
            dry_run=True,
        )

    def test_dry_run_without_execute_rejected(self) -> None:
        with (
            patch(
                "sys.argv",
                ["sqlbackup", "--backup", "--config", "mydb", "--path", "dump.sql", "--dry-run"],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 2

    def test_execute_error_exits(self, tmp_path: Path) -> None:
        from sqlbackup.exceptions import ExecuteError

        sql_file = tmp_path / "revision.sql"
        with (
            patch("sqlbackup.cli.load_config", return_value=MagicMock()),
            patch(
                "sqlbackup.cli.execute_from_path",
                side_effect=ExecuteError("bad file"),
            ),
            patch(
                "sys.argv",
                ["sqlbackup", "--execute", "--config", "mydb", "--path", str(sql_file)],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 1

    def test_backup_path_and_flags_passed_through(self, tmp_path: Path) -> None:
        config = MagicMock()
        sql_folder = tmp_path / "revisions"
        backup_dir = tmp_path / "backups"

        with (
            patch("sqlbackup.cli.load_config", return_value=config),
            patch(
                "sqlbackup.cli.execute_from_path",
                return_value=(tmp_path / "picked.sql", tmp_path / "dump.zip", 3),
            ) as mock_execute,
            patch(
                "sys.argv",
                [
                    "sqlbackup",
                    "--execute",
                    "--config",
                    "mydb",
                    "--path",
                    str(sql_folder),
                    "--backup-path",
                    str(backup_dir),
                    "--incremental",
                    "30",
                    "--zip",
                    "--yes",
                ],
            ),
        ):
            main()

        mock_execute.assert_called_once_with(
            config,
            sql_folder,
            backup_path=backup_dir,
            incremental=30,
            zip=True,
            assume_yes=True,
            dry_run=False,
        )

    def test_backup_path_without_execute_rejected(self, tmp_path: Path) -> None:
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
                    "--backup-path",
                    str(tmp_path),
                ],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 2

    def test_yes_without_execute_rejected(self) -> None:
        with (
            patch(
                "sys.argv",
                ["sqlbackup", "--backup", "--config", "mydb", "--path", "dump.sql", "--yes"],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 2

    def test_zip_with_execute_without_backup_path_rejected(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "revision.sql"
        with (
            patch(
                "sys.argv",
                [
                    "sqlbackup",
                    "--execute",
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

    def test_incremental_with_execute_without_backup_path_rejected(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "revision.sql"
        with (
            patch(
                "sys.argv",
                [
                    "sqlbackup",
                    "--execute",
                    "--config",
                    "mydb",
                    "--path",
                    str(sql_file),
                    "--incremental",
                    "5",
                ],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            main()

        assert exc_info.value.code == 2
