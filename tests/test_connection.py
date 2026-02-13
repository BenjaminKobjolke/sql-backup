"""Tests for connection module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from sqlbackup.config import DbConfig
from sqlbackup.connection import DatabaseConnection
from sqlbackup.exceptions import ConnectionError


@pytest.fixture()
def db_config() -> DbConfig:
    return DbConfig(host="localhost", port=3306, user="root", password="secret", database="testdb")


@pytest.fixture()
def mock_pymysql() -> MagicMock:
    with patch("sqlbackup.connection.pymysql") as mock:
        mock_conn = MagicMock()
        mock.connect.return_value = mock_conn
        yield mock


class TestDatabaseConnection:
    def test_connects_with_config(self, db_config: DbConfig, mock_pymysql: MagicMock) -> None:
        with DatabaseConnection(db_config):
            mock_pymysql.connect.assert_called_once_with(
                host="localhost",
                port=3306,
                user="root",
                password="secret",
                database="testdb",
                charset="utf8mb4",
            )

    def test_closes_on_exit(self, db_config: DbConfig, mock_pymysql: MagicMock) -> None:
        mock_conn = mock_pymysql.connect.return_value
        with DatabaseConnection(db_config):
            pass
        mock_conn.close.assert_called_once()

    def test_connection_failure_raises(self, db_config: DbConfig, mock_pymysql: MagicMock) -> None:
        mock_pymysql.connect.side_effect = Exception("Connection refused")
        with (
            pytest.raises(ConnectionError, match="Failed to connect"),
            DatabaseConnection(db_config),
        ):
            pass

    def test_get_tables(self, db_config: DbConfig, mock_pymysql: MagicMock) -> None:
        mock_conn = mock_pymysql.connect.return_value
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = [("users",), ("posts",)]

        with DatabaseConnection(db_config) as db:
            tables = db.get_tables()

        assert tables == ["users", "posts"]
        mock_cursor.execute.assert_called_once_with("SHOW TABLES")

    def test_get_create_table(self, db_config: DbConfig, mock_pymysql: MagicMock) -> None:
        mock_conn = mock_pymysql.connect.return_value
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchone.return_value = ("users", "CREATE TABLE `users` (id INT)")

        with DatabaseConnection(db_config) as db:
            ddl = db.get_create_table("users")

        assert ddl == "CREATE TABLE `users` (id INT)"
        mock_cursor.execute.assert_called_once_with("SHOW CREATE TABLE `users`")

    def test_get_column_names(self, db_config: DbConfig, mock_pymysql: MagicMock) -> None:
        mock_conn = mock_pymysql.connect.return_value
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_cursor.description = [("id",), ("name",), ("email",)]

        with DatabaseConnection(db_config) as db:
            cols = db.get_column_names("users")

        assert cols == ["id", "name", "email"]
        mock_cursor.execute.assert_called_once_with("SELECT * FROM `users` LIMIT 0")

    def test_iter_rows(self, db_config: DbConfig, mock_pymysql: MagicMock) -> None:
        mock_conn = mock_pymysql.connect.return_value
        mock_ss_cursor = MagicMock()
        mock_pymysql.cursors.SSCursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_ss_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_ss_cursor.fetchmany.side_effect = [
            [(1, "alice"), (2, "bob")],
            [(3, "carol")],
            [],
        ]

        with DatabaseConnection(db_config) as db:
            rows = list(db.iter_rows("users", batch_size=2))

        assert rows == [[(1, "alice"), (2, "bob")], [(3, "carol")]]

    def test_execute_sql(self, db_config: DbConfig, mock_pymysql: MagicMock) -> None:
        mock_conn = mock_pymysql.connect.return_value
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with DatabaseConnection(db_config) as db:
            db.execute_sql("DROP TABLE IF EXISTS `users`")

        mock_cursor.execute.assert_called_once_with("DROP TABLE IF EXISTS `users`")
        mock_conn.commit.assert_called()
