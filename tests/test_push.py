"""Tests for push module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from sqlbackup.config import DbConfig
from sqlbackup.exceptions import PushError
from sqlbackup.push import push_database


@pytest.fixture()
def db_config() -> DbConfig:
    return DbConfig(host="localhost", port=3306, user="root", password="secret", database="testdb")


@pytest.fixture()
def mock_db_conn() -> MagicMock:
    mock = MagicMock()
    mock.__enter__ = MagicMock(return_value=mock)
    mock.__exit__ = MagicMock(return_value=False)
    return mock


class TestPushDatabase:
    def test_file_not_found_raises(self, db_config: DbConfig, tmp_path: Path) -> None:
        missing = tmp_path / "missing.sql"
        with pytest.raises(PushError, match="SQL file not found"):
            push_database(db_config, missing)

    def test_executes_statements(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        sql_file = tmp_path / "dump.sql"
        sql_file.write_text(
            "SET FOREIGN_KEY_CHECKS = 0;\n"
            "DROP TABLE IF EXISTS `users`;\n"
            "CREATE TABLE `users` (id INT);\n"
            "SET FOREIGN_KEY_CHECKS = 1;\n",
            encoding="utf-8",
        )

        with patch("sqlbackup.push.DatabaseConnection", return_value=mock_db_conn):
            push_database(db_config, sql_file)

        calls = mock_db_conn.execute_sql.call_args_list
        assert call("SET GLOBAL max_allowed_packet = 67108864") in calls
        assert call("SET SESSION net_read_timeout = 600") in calls
        assert call("SET SESSION net_write_timeout = 600") in calls
        assert call("SET FOREIGN_KEY_CHECKS = 0") in calls
        assert call("DROP TABLE IF EXISTS `users`") in calls
        assert call("CREATE TABLE `users` (id INT)") in calls
        assert call("SET FOREIGN_KEY_CHECKS = 1") in calls

    def test_skips_empty_lines_and_comments(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        sql_file = tmp_path / "dump.sql"
        sql_file.write_text(
            "-- This is a comment\n\nDROP TABLE IF EXISTS `users`;\n\n-- Another comment\n",
            encoding="utf-8",
        )

        with patch("sqlbackup.push.DatabaseConnection", return_value=mock_db_conn):
            push_database(db_config, sql_file)

        calls = mock_db_conn.execute_sql.call_args_list
        assert len(calls) == 4
        assert calls[3] == call("DROP TABLE IF EXISTS `users`")

    def test_handles_multiline_statements(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        sql_file = tmp_path / "dump.sql"
        sql_file.write_text(
            "INSERT INTO `users` (`id`, `name`) VALUES\n(1, 'alice'),\n(2, 'bob');\n",
            encoding="utf-8",
        )

        with patch("sqlbackup.push.DatabaseConnection", return_value=mock_db_conn):
            push_database(db_config, sql_file)

        calls = mock_db_conn.execute_sql.call_args_list
        assert len(calls) == 4
        assert "INSERT INTO `users`" in calls[3][0][0]
        assert "(1, 'alice')" in calls[3][0][0]
        assert "(2, 'bob')" in calls[3][0][0]

    def test_handles_create_table_multiline(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        sql_file = tmp_path / "dump.sql"
        sql_file.write_text(
            "CREATE TABLE `users` (\n"
            "  `id` int NOT NULL,\n"
            "  `name` varchar(50) DEFAULT NULL,\n"
            "  PRIMARY KEY (`id`)\n"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n",
            encoding="utf-8",
        )

        with patch("sqlbackup.push.DatabaseConnection", return_value=mock_db_conn):
            push_database(db_config, sql_file)

        calls = mock_db_conn.execute_sql.call_args_list
        assert len(calls) == 4
        assert "CREATE TABLE `users`" in calls[3][0][0]
        assert "ENGINE=InnoDB" in calls[3][0][0]

    def test_handles_semicolons_in_string_values(
        self, db_config: DbConfig, mock_db_conn: MagicMock, tmp_path: Path
    ) -> None:
        """Semicolons inside string literals must not split the statement."""
        sql_file = tmp_path / "dump.sql"
        sql_file.write_text(
            "INSERT INTO `wp_options` (`option_name`, `option_value`) VALUES\n"
            "('wptouch_settings', 's:10:\"site_title\";\n"
            "s:29:\"XIDA Design & Tech\";\n"
            "s:5:\"color\";\n"
            "s:7:\"#ffffff\";');\n",
            encoding="utf-8",
        )

        with patch("sqlbackup.push.DatabaseConnection", return_value=mock_db_conn):
            push_database(db_config, sql_file)

        calls = mock_db_conn.execute_sql.call_args_list
        # 1 SET GLOBAL + 2 session SETs + 1 INSERT = 4
        assert len(calls) == 4
        stmt = calls[3][0][0]
        assert "INSERT INTO `wp_options`" in stmt
        assert "s:10:\"site_title\";" in stmt
        assert "s:7:\"#ffffff\";" in stmt
