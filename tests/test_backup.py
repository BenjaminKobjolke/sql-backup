"""Tests for backup module."""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sqlbackup.backup import backup_database, cleanup_old_backups, resolve_incremental_path
from sqlbackup.config import DbConfig
from sqlbackup.exceptions import BackupError


@pytest.fixture()
def db_config() -> DbConfig:
    return DbConfig(host="localhost", port=3306, user="root", password="secret", database="testdb")


@pytest.fixture()
def mock_db_conn() -> MagicMock:
    mock = MagicMock()
    mock.__enter__ = MagicMock(return_value=mock)
    mock.__exit__ = MagicMock(return_value=False)
    return mock


class TestBackupDatabase:
    def test_creates_output_file(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        mock_db_conn.get_tables.return_value = []
        output = tmp_path / "dump.sql"

        with patch("sqlbackup.backup.DatabaseConnection", return_value=mock_db_conn):
            backup_database(db_config, output)

        assert output.exists()

    def test_raises_if_file_exists(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        output = tmp_path / "dump.sql"
        output.write_text("existing")

        with (
            pytest.raises(BackupError, match="already exists"),
            patch("sqlbackup.backup.DatabaseConnection", return_value=mock_db_conn),
        ):
            backup_database(db_config, output)

    def test_header_contains_fk_disable(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        mock_db_conn.get_tables.return_value = []
        output = tmp_path / "dump.sql"

        with patch("sqlbackup.backup.DatabaseConnection", return_value=mock_db_conn):
            backup_database(db_config, output)

        content = output.read_text(encoding="utf-8")
        assert "SET FOREIGN_KEY_CHECKS = 0;" in content

    def test_footer_contains_fk_enable(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        mock_db_conn.get_tables.return_value = []
        output = tmp_path / "dump.sql"

        with patch("sqlbackup.backup.DatabaseConnection", return_value=mock_db_conn):
            backup_database(db_config, output)

        content = output.read_text(encoding="utf-8")
        assert "SET FOREIGN_KEY_CHECKS = 1;" in content

    def test_dumps_drop_and_create(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        mock_db_conn.get_tables.return_value = ["users"]
        mock_db_conn.get_create_table.return_value = "CREATE TABLE `users` (id INT)"
        mock_db_conn.get_column_names.return_value = ["id"]
        mock_db_conn.iter_rows.return_value = iter([])
        output = tmp_path / "dump.sql"

        with patch("sqlbackup.backup.DatabaseConnection", return_value=mock_db_conn):
            backup_database(db_config, output)

        content = output.read_text(encoding="utf-8")
        assert "DROP TABLE IF EXISTS `users`;" in content
        assert "CREATE TABLE `users` (id INT);" in content

    def test_dumps_insert_statements(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        mock_db_conn.get_tables.return_value = ["users"]
        create_ddl = "CREATE TABLE `users` (id INT, name VARCHAR(50))"
        mock_db_conn.get_create_table.return_value = create_ddl
        mock_db_conn.get_column_names.return_value = ["id", "name"]
        mock_db_conn.iter_rows.return_value = iter(
            [
                [(1, "alice"), (2, "bob")],
            ]
        )
        output = tmp_path / "dump.sql"

        with patch("sqlbackup.backup.DatabaseConnection", return_value=mock_db_conn):
            backup_database(db_config, output)

        content = output.read_text(encoding="utf-8")
        assert "INSERT INTO `users` (`id`, `name`) VALUES" in content
        assert "(1, 'alice')" in content
        assert "(2, 'bob')" in content

    def test_handles_null_values(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        mock_db_conn.get_tables.return_value = ["users"]
        create_ddl = "CREATE TABLE `users` (id INT, name VARCHAR(50))"
        mock_db_conn.get_create_table.return_value = create_ddl
        mock_db_conn.get_column_names.return_value = ["id", "name"]
        mock_db_conn.iter_rows.return_value = iter(
            [
                [(1, None)],
            ]
        )
        output = tmp_path / "dump.sql"

        with patch("sqlbackup.backup.DatabaseConnection", return_value=mock_db_conn):
            backup_database(db_config, output)

        content = output.read_text(encoding="utf-8")
        assert "(1, NULL)" in content

    def test_escapes_single_quotes(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        mock_db_conn.get_tables.return_value = ["users"]
        create_ddl = "CREATE TABLE `users` (id INT, name VARCHAR(50))"
        mock_db_conn.get_create_table.return_value = create_ddl
        mock_db_conn.get_column_names.return_value = ["id", "name"]
        mock_db_conn.iter_rows.return_value = iter(
            [
                [(1, "O'Brien")],
            ]
        )
        output = tmp_path / "dump.sql"

        with patch("sqlbackup.backup.DatabaseConnection", return_value=mock_db_conn):
            backup_database(db_config, output)

        content = output.read_text(encoding="utf-8")
        assert "O\\'Brien" in content

    def test_handles_bytes_values(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        mock_db_conn.get_tables.return_value = ["data"]
        mock_db_conn.get_create_table.return_value = "CREATE TABLE `data` (id INT, blob_col BLOB)"
        mock_db_conn.get_column_names.return_value = ["id", "blob_col"]
        mock_db_conn.iter_rows.return_value = iter(
            [
                [(1, b"\x00\x01\x02")],
            ]
        )
        output = tmp_path / "dump.sql"

        with patch("sqlbackup.backup.DatabaseConnection", return_value=mock_db_conn):
            backup_database(db_config, output)

        content = output.read_text(encoding="utf-8")
        assert "X'000102'" in content

    def test_multiple_tables(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        mock_db_conn.get_tables.return_value = ["users", "posts"]
        mock_db_conn.get_create_table.side_effect = [
            "CREATE TABLE `users` (id INT)",
            "CREATE TABLE `posts` (id INT)",
        ]
        mock_db_conn.get_column_names.side_effect = [["id"], ["id"]]
        mock_db_conn.iter_rows.side_effect = [iter([]), iter([])]
        output = tmp_path / "dump.sql"

        with patch("sqlbackup.backup.DatabaseConnection", return_value=mock_db_conn):
            backup_database(db_config, output)

        content = output.read_text(encoding="utf-8")
        assert "DROP TABLE IF EXISTS `users`;" in content
        assert "DROP TABLE IF EXISTS `posts`;" in content

    def test_creates_parent_directories(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        mock_db_conn.get_tables.return_value = []
        output = tmp_path / "sub" / "dir" / "dump.sql"

        with patch("sqlbackup.backup.DatabaseConnection", return_value=mock_db_conn):
            backup_database(db_config, output)

        assert output.exists()


class TestResolveIncrementalPath:
    def test_prepends_timestamp(self, tmp_path: Path) -> None:
        base = tmp_path / "db.sql"
        result = resolve_incremental_path(base)
        assert result.parent == tmp_path
        assert re.match(r"^\d{8}_\d{6}_db\.sql$", result.name)

    def test_preserves_directory(self, tmp_path: Path) -> None:
        base = tmp_path / "sub" / "db.sql"
        result = resolve_incremental_path(base)
        assert result.parent == tmp_path / "sub"


class TestCleanupOldBackups:
    def test_keeps_n_most_recent(self, tmp_path: Path) -> None:
        for i in range(5):
            (tmp_path / f"2026010{i}_120000_db.sql").write_text("")
        cleanup_old_backups(tmp_path / "db.sql", keep=3)
        remaining = sorted(tmp_path.glob("*_db.sql"))
        assert len(remaining) == 3
        assert remaining[0].name == "20260102_120000_db.sql"

    def test_no_delete_when_fewer_than_keep(self, tmp_path: Path) -> None:
        for i in range(2):
            (tmp_path / f"2026010{i}_120000_db.sql").write_text("")
        cleanup_old_backups(tmp_path / "db.sql", keep=5)
        remaining = list(tmp_path.glob("*_db.sql"))
        assert len(remaining) == 2

    def test_does_not_touch_unrelated_files(self, tmp_path: Path) -> None:
        (tmp_path / "20260101_120000_db.sql").write_text("")
        (tmp_path / "20260102_120000_other.sql").write_text("")
        cleanup_old_backups(tmp_path / "db.sql", keep=1)
        assert (tmp_path / "20260101_120000_db.sql").exists()
        assert (tmp_path / "20260102_120000_other.sql").exists()


class TestIncrementalBackup:
    @pytest.fixture()
    def db_config(self) -> DbConfig:
        return DbConfig(
            host="localhost", port=3306, user="root", password="secret", database="testdb"
        )

    @pytest.fixture()
    def mock_db_conn(self) -> MagicMock:
        mock = MagicMock()
        mock.__enter__ = MagicMock(return_value=mock)
        mock.__exit__ = MagicMock(return_value=False)
        mock.get_tables.return_value = []
        return mock

    def test_creates_timestamped_file(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        base = tmp_path / "db.sql"
        with patch("sqlbackup.backup.DatabaseConnection", return_value=mock_db_conn):
            actual = backup_database(db_config, base, incremental=5)
        assert re.match(r"^\d{8}_\d{6}_db\.sql$", actual.name)
        assert actual.exists()

    def test_does_not_error_when_base_exists(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        base = tmp_path / "db.sql"
        base.write_text("existing")
        with patch("sqlbackup.backup.DatabaseConnection", return_value=mock_db_conn):
            actual = backup_database(db_config, base, incremental=5)
        assert actual.exists()
        assert actual != base

    def test_cleanup_runs_after_backup(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        for i in range(5):
            (tmp_path / f"2026010{i}_120000_db.sql").write_text("")
        base = tmp_path / "db.sql"
        with patch("sqlbackup.backup.DatabaseConnection", return_value=mock_db_conn):
            backup_database(db_config, base, incremental=3)
        remaining = list(tmp_path.glob("*_db.sql"))
        assert len(remaining) == 3
