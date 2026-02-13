"""Database connection wrapper around pymysql."""

from __future__ import annotations

from typing import Any

import pymysql
import pymysql.cursors

from sqlbackup.config import DbConfig
from sqlbackup.constants import ERR_CONNECTION_FAILED
from sqlbackup.exceptions import ConnectionError


class DatabaseConnection:
    """Context-managed database connection."""

    def __init__(self, config: DbConfig) -> None:
        self._config = config
        self._conn: pymysql.connections.Connection[Any] | None = None

    def __enter__(self) -> DatabaseConnection:
        try:
            self._conn = pymysql.connect(
                host=self._config.host,
                port=self._config.port,
                user=self._config.user,
                password=self._config.password,
                database=self._config.database,
                charset="utf8mb4",
            )
        except Exception as exc:
            raise ConnectionError(ERR_CONNECTION_FAILED.format(error=exc)) from exc
        return self

    def __exit__(self, *args: object) -> None:
        if self._conn is not None:
            self._conn.close()

    @property
    def conn(self) -> pymysql.connections.Connection[Any]:
        assert self._conn is not None
        return self._conn

    def get_tables(self) -> list[str]:
        """Return list of table names in the database."""
        with self.conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            return [row[0] for row in cursor.fetchall()]

    def get_create_table(self, table: str) -> str:
        """Return the CREATE TABLE statement for a table."""
        with self.conn.cursor() as cursor:
            cursor.execute(f"SHOW CREATE TABLE `{table}`")
            row = cursor.fetchone()
            return str(row[1])

    def get_column_names(self, table: str) -> list[str]:
        """Return column names for a table."""
        with self.conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM `{table}` LIMIT 0")
            return [desc[0] for desc in cursor.description]

    def iter_rows(self, table: str, batch_size: int = 1000) -> Any:
        """Yield batches of rows from a table using server-side cursor."""
        with self.conn.cursor(pymysql.cursors.SSCursor) as cursor:
            cursor.execute(f"SELECT * FROM `{table}`")
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                yield rows

    def execute_sql(self, sql: str) -> None:
        """Execute a single SQL statement and commit."""
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
        self.conn.commit()
