"""Tests for execute module."""

from __future__ import annotations

import os
import zipfile
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from sqlbackup.config import DbConfig
from sqlbackup.exceptions import ExecuteError
from sqlbackup.execute import (
    _list_sql_files,
    _select_sql_file,
    execute_from_path,
    execute_sql_file,
    resolve_sql_target,
    validate_sql_file,
)


@pytest.fixture()
def db_config() -> DbConfig:
    return DbConfig(host="localhost", port=3306, user="root", password="secret", database="testdb")


@pytest.fixture()
def mock_db_conn() -> MagicMock:
    mock = MagicMock()
    mock.__enter__ = MagicMock(return_value=mock)
    mock.__exit__ = MagicMock(return_value=False)
    return mock


class TestValidateSqlFile:
    def test_missing_file_raises(self, tmp_path: Path) -> None:
        missing = tmp_path / "missing.sql"
        with pytest.raises(ExecuteError, match="SQL file not found"):
            validate_sql_file(missing)

    def test_empty_file_raises(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "empty.sql"
        sql_file.write_text("-- just a comment\n\n", encoding="utf-8")
        with pytest.raises(ExecuteError, match="No executable SQL statements"):
            validate_sql_file(sql_file)

    def test_delimiter_block_raises(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "proc.sql"
        sql_file.write_text(
            "DELIMITER $$\nCREATE PROCEDURE foo() BEGIN SELECT 1; END $$\nDELIMITER ;\n",
            encoding="utf-8",
        )
        with pytest.raises(ExecuteError, match="DELIMITER"):
            validate_sql_file(sql_file)

    def test_unterminated_statement_raises(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "truncated.sql"
        sql_file.write_text(
            "UPDATE `app_contests` SET `foo` = 'bar';\n"
            "ALTER TABLE `app_contests` ADD COLUMN `baz` INT",
            encoding="utf-8",
        )
        with pytest.raises(ExecuteError, match="unterminated"):
            validate_sql_file(sql_file)

    def test_valid_file_returns_statements(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "revision.sql"
        sql_file.write_text(
            "-- header comment\n"
            "ALTER TABLE `app_contests` ADD COLUMN `content_vertical_align` VARCHAR(10);\n"
            "UPDATE `app_contests` SET `content_vertical_align` = 'top';\n",
            encoding="utf-8",
        )
        statements = validate_sql_file(sql_file)
        assert len(statements) == 2
        assert "ALTER TABLE `app_contests`" in statements[0]
        assert "UPDATE `app_contests`" in statements[1]

    def test_insert_on_duplicate_key_update_shape(self, tmp_path: Path) -> None:
        """Mirrors the real dbrevisions INSERT ... ON DUPLICATE KEY UPDATE files."""
        sql_file = tmp_path / "translations.sql"
        sql_file.write_text(
            "-- Idempotent via ON DUPLICATE KEY UPDATE.\n"
            "INSERT INTO `frk_translations` (`name`, `de`, `en`) VALUES\n"
            "\t('foo', 'Füße', 'feet — plural')\n"
            "ON DUPLICATE KEY UPDATE\n"
            "\t`de` = VALUES(`de`),\n"
            "\t`en` = VALUES(`en`);\n",
            encoding="utf-8",
        )
        statements = validate_sql_file(sql_file)
        assert len(statements) == 1
        assert "ON DUPLICATE KEY UPDATE" in statements[0]
        assert "Füße" in statements[0]

    def test_zip_input(self, tmp_path: Path) -> None:
        zip_path = tmp_path / "revision.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("revision.sql", "UPDATE `t` SET `x` = 1;\n")
        statements = validate_sql_file(zip_path)
        assert statements == ["UPDATE `t` SET `x` = 1"]


class TestExecuteSqlFile:
    def test_executes_each_statement(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        sql_file = tmp_path / "revision.sql"
        sql_file.write_text(
            "ALTER TABLE `t` ADD COLUMN `x` INT;\nUPDATE `t` SET `x` = 1;\n",
            encoding="utf-8",
        )

        with patch("sqlbackup.execute.DatabaseConnection", return_value=mock_db_conn):
            count = execute_sql_file(db_config, sql_file)

        assert count == 2
        calls = mock_db_conn.execute_sql.call_args_list
        assert call("SET SESSION net_read_timeout = 600") in calls
        assert call("SET SESSION net_write_timeout = 600") in calls
        assert call("ALTER TABLE `t` ADD COLUMN `x` INT") in calls
        assert call("UPDATE `t` SET `x` = 1") in calls

    def test_runs_against_non_empty_target_without_force(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        """execute never checks get_tables - running against a populated DB is the point."""
        sql_file = tmp_path / "revision.sql"
        sql_file.write_text("UPDATE `t` SET `x` = 1;\n", encoding="utf-8")

        with patch("sqlbackup.execute.DatabaseConnection", return_value=mock_db_conn):
            execute_sql_file(db_config, sql_file)

        mock_db_conn.get_tables.assert_not_called()

    def test_dry_run_never_connects(
        self, db_config: DbConfig, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        sql_file = tmp_path / "revision.sql"
        sql_file.write_text(
            "ALTER TABLE `t` ADD COLUMN `x` INT;\nUPDATE `t` SET `x` = 1;\n",
            encoding="utf-8",
        )

        with patch("sqlbackup.execute.DatabaseConnection") as mock_conn_cls:
            count = execute_sql_file(db_config, sql_file, dry_run=True)

        assert count == 2
        mock_conn_cls.assert_not_called()
        out = capsys.readouterr().out
        assert "ALTER TABLE" in out
        assert "UPDATE" in out

    def test_dry_run_invalid_file_still_raises(self, db_config: DbConfig, tmp_path: Path) -> None:
        missing = tmp_path / "missing.sql"
        with pytest.raises(ExecuteError, match="SQL file not found"):
            execute_sql_file(db_config, missing, dry_run=True)

    def test_stops_at_first_failing_statement(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        sql_file = tmp_path / "revision.sql"
        sql_file.write_text(
            "ALTER TABLE `t` ADD COLUMN `x` INT;\nUPDATE `t` SET `x` = 1;\n",
            encoding="utf-8",
        )
        mock_db_conn.execute_sql.side_effect = [None, None, RuntimeError("boom")]

        with (
            patch("sqlbackup.execute.DatabaseConnection", return_value=mock_db_conn),
            pytest.raises(RuntimeError, match="boom"),
        ):
            execute_sql_file(db_config, sql_file)

        # Only the two SET SESSION calls plus the failing ALTER should have run.
        assert mock_db_conn.execute_sql.call_count == 3


def _touch(path: Path, mtime: float) -> None:
    path.write_text("UPDATE `t` SET `x` = 1;\n", encoding="utf-8")
    os.utime(path, (mtime, mtime))


class TestListSqlFiles:
    def test_sorts_newest_first_by_mtime(self, tmp_path: Path) -> None:
        old = tmp_path / "20260101_old.sql"
        mid = tmp_path / "20260215_mid.sql"
        new = tmp_path / "20260301_new.sql"
        _touch(old, 1000)
        _touch(mid, 2000)
        _touch(new, 3000)

        assert _list_sql_files(tmp_path) == [new, mid, old]

    def test_ignores_non_sql_files(self, tmp_path: Path) -> None:
        _touch(tmp_path / "a.sql", 1000)
        (tmp_path / "readme.txt").write_text("nope", encoding="utf-8")

        assert [p.name for p in _list_sql_files(tmp_path)] == ["a.sql"]

    def test_empty_folder_returns_empty_list(self, tmp_path: Path) -> None:
        assert _list_sql_files(tmp_path) == []


class TestResolveSqlTarget:
    def test_missing_path_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ExecuteError, match="Path not found"):
            resolve_sql_target(tmp_path / "nope")

    def test_file_returns_itself(self, tmp_path: Path) -> None:
        sql_file = tmp_path / "revision.sql"
        sql_file.write_text("UPDATE `t` SET `x` = 1;\n", encoding="utf-8")
        assert resolve_sql_target(sql_file) == sql_file

    def test_empty_folder_raises(self, tmp_path: Path) -> None:
        with pytest.raises(ExecuteError, match="No .sql files found"):
            resolve_sql_target(tmp_path)

    def test_folder_with_assume_yes_picks_newest(self, tmp_path: Path) -> None:
        old = tmp_path / "old.sql"
        new = tmp_path / "new.sql"
        _touch(old, 1000)
        _touch(new, 2000)

        assert resolve_sql_target(tmp_path, assume_yes=True) == new


class TestSelectSqlFile:
    def test_empty_input_picks_newest(self, tmp_path: Path) -> None:
        old = tmp_path / "old.sql"
        new = tmp_path / "new.sql"
        _touch(old, 1000)
        _touch(new, 2000)

        with patch("builtins.input", return_value=""):
            assert _select_sql_file(tmp_path, assume_yes=False) == new

    def test_numeric_choice_picks_that_file(self, tmp_path: Path) -> None:
        old = tmp_path / "old.sql"
        new = tmp_path / "new.sql"
        _touch(old, 1000)
        _touch(new, 2000)

        with patch("builtins.input", return_value="2"):
            # newest ("new") is index 1, "old" is index 2
            assert _select_sql_file(tmp_path, assume_yes=False) == old

    def test_invalid_then_valid_reprompts(self, tmp_path: Path) -> None:
        new = tmp_path / "new.sql"
        _touch(new, 1000)

        with patch("builtins.input", side_effect=["nope", "1"]):
            assert _select_sql_file(tmp_path, assume_yes=False) == new

    def test_eof_picks_newest(self, tmp_path: Path) -> None:
        old = tmp_path / "old.sql"
        new = tmp_path / "new.sql"
        _touch(old, 1000)
        _touch(new, 2000)

        with patch("builtins.input", side_effect=EOFError):
            assert _select_sql_file(tmp_path, assume_yes=False) == new


class TestExecuteFromPath:
    def test_no_backup_path_skips_backup(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        sql_file = tmp_path / "revision.sql"
        sql_file.write_text("UPDATE `t` SET `x` = 1;\n", encoding="utf-8")

        with (
            patch("sqlbackup.execute.DatabaseConnection", return_value=mock_db_conn),
            patch("sqlbackup.execute.backup_database") as mock_backup,
        ):
            target, backup_written, count = execute_from_path(db_config, sql_file)

        mock_backup.assert_not_called()
        assert backup_written is None
        assert target == sql_file
        assert count == 1

    def test_backup_path_runs_backup_before_execute(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        sql_file = tmp_path / "revision.sql"
        sql_file.write_text("UPDATE `t` SET `x` = 1;\n", encoding="utf-8")
        backup_dir = tmp_path / "backups"
        order: list[str] = []

        def fake_backup(*args: object, **kwargs: object) -> Path:
            order.append("backup")
            return backup_dir / "dump.sql"

        def fake_execute_sql(sql: str) -> None:
            order.append("execute")

        mock_db_conn.execute_sql.side_effect = fake_execute_sql

        with (
            patch("sqlbackup.execute.DatabaseConnection", return_value=mock_db_conn),
            patch("sqlbackup.execute.backup_database", side_effect=fake_backup) as mock_backup,
        ):
            target, backup_written, count = execute_from_path(
                db_config, sql_file, backup_path=backup_dir
            )

        mock_backup.assert_called_once()
        assert order[0] == "backup"
        assert "execute" in order
        assert backup_written == backup_dir / "dump.sql"
        assert count == 1

    def test_backup_path_as_sql_file_used_as_base_with_incremental_and_zip(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        sql_file = tmp_path / "revision.sql"
        sql_file.write_text("UPDATE `t` SET `x` = 1;\n", encoding="utf-8")
        backup_file = tmp_path / "backups" / "backup.sql"

        with (
            patch("sqlbackup.execute.DatabaseConnection", return_value=mock_db_conn),
            patch("sqlbackup.execute.backup_database", return_value=backup_file) as mock_backup,
        ):
            execute_from_path(
                db_config,
                sql_file,
                backup_path=backup_file,
                incremental=30,
                zip=True,
            )

        mock_backup.assert_called_once_with(db_config, backup_file, incremental=30, zip=True)

    def test_backup_path_as_directory_auto_names_dump(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        sql_file = tmp_path / "revision.sql"
        sql_file.write_text("UPDATE `t` SET `x` = 1;\n", encoding="utf-8")
        backup_dir = tmp_path / "backups"

        with (
            patch("sqlbackup.execute.DatabaseConnection", return_value=mock_db_conn),
            patch(
                "sqlbackup.execute.backup_database", return_value=backup_dir / "dump.sql"
            ) as mock_backup,
        ):
            execute_from_path(db_config, sql_file, backup_path=backup_dir)

        called_base = mock_backup.call_args[0][1]
        assert called_base.parent == backup_dir
        # No --incremental given, so execute_from_path timestamps the auto-named base
        # via resolve_incremental_path to keep the exists-guard from ever triggering.
        assert called_base.name.endswith(f"_{db_config.database}.sql")

    def test_dry_run_skips_backup_and_connection(self, db_config: DbConfig, tmp_path: Path) -> None:
        sql_file = tmp_path / "revision.sql"
        sql_file.write_text("UPDATE `t` SET `x` = 1;\n", encoding="utf-8")
        backup_dir = tmp_path / "backups"

        with (
            patch("sqlbackup.execute.DatabaseConnection") as mock_conn_cls,
            patch("sqlbackup.execute.backup_database") as mock_backup,
        ):
            target, backup_written, count = execute_from_path(
                db_config, sql_file, backup_path=backup_dir, dry_run=True
            )

        mock_backup.assert_not_called()
        mock_conn_cls.assert_not_called()
        assert backup_written is None
        assert count == 1

    def test_folder_input_with_assume_yes_picks_newest_and_runs(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        old = tmp_path / "old.sql"
        new = tmp_path / "new.sql"
        _touch(old, 1000)
        _touch(new, 2000)

        with patch("sqlbackup.execute.DatabaseConnection", return_value=mock_db_conn):
            target, backup_written, count = execute_from_path(db_config, tmp_path, assume_yes=True)

        assert target == new
        assert backup_written is None
