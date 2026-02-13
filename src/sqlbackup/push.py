"""Database restore (push) logic."""

from __future__ import annotations

from pathlib import Path

from sqlbackup.config import DbConfig
from sqlbackup.connection import DatabaseConnection
from sqlbackup.constants import ERR_PUSH_FILE_NOT_FOUND
from sqlbackup.exceptions import PushError


def _parse_statements(sql_path: Path) -> list[str]:
    """Parse a SQL file into individual statements.

    Handles multi-line statements terminated by semicolons.
    Skips comments and blank lines.
    """
    statements: list[str] = []
    current: list[str] = []

    with open(sql_path, encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()

            if not stripped or stripped.startswith("--"):
                continue

            current.append(stripped)

            if stripped.endswith(";"):
                stmt = "\n".join(current)
                stmt = stmt.rstrip(";")
                statements.append(stmt)
                current = []

    return statements


def push_database(config: DbConfig, sql_path: Path) -> None:
    """Restore a .sql dump file to a database.

    Args:
        config: Database connection configuration.
        sql_path: Path to the SQL dump file.
    """
    if not sql_path.exists():
        raise PushError(ERR_PUSH_FILE_NOT_FOUND.format(path=sql_path))

    statements = _parse_statements(sql_path)

    with DatabaseConnection(config) as db:
        for stmt in statements:
            db.execute_sql(stmt)
