"""Database restore (push) logic."""

from __future__ import annotations

import contextlib
from pathlib import Path

from sqlbackup.config import DbConfig
from sqlbackup.connection import DatabaseConnection
from sqlbackup.constants import ERR_PUSH_FILE_NOT_FOUND
from sqlbackup.exceptions import PushError


def _parse_statements(sql_path: Path) -> list[str]:
    """Parse a SQL file into individual statements.

    Handles multi-line statements terminated by semicolons.
    Skips comments and blank lines.
    Tracks single-quote state so semicolons inside string literals
    (e.g. serialized PHP data) are not treated as statement boundaries.
    """
    statements: list[str] = []
    current: list[str] = []
    in_string = False

    with open(sql_path, encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()

            if not in_string and (not stripped or stripped.startswith("--")):
                continue

            current.append(stripped)

            # Track single-quote state to find real statement boundaries
            i = 0
            while i < len(stripped):
                c = stripped[i]
                if in_string:
                    if c == "\\":
                        i += 2  # skip escaped character
                        continue
                    if c == "'":
                        in_string = False
                else:
                    if c == "'":
                        in_string = True
                i += 1

            if not in_string and stripped.endswith(";"):
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

    # Increase server max_allowed_packet (requires SUPER/SYSTEM_VARIABLES_ADMIN).
    # New connections pick up the global value, so we do this before the main connection.
    with DatabaseConnection(config) as db, contextlib.suppress(Exception):
        db.execute_sql("SET GLOBAL max_allowed_packet = 67108864")

    with DatabaseConnection(config) as db:
        db.execute_sql("SET SESSION net_read_timeout = 600")
        db.execute_sql("SET SESSION net_write_timeout = 600")
        for stmt in statements:
            db.execute_sql(stmt)
