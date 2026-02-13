"""Tests for backup module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sqlbackup.backup import backup_database
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
