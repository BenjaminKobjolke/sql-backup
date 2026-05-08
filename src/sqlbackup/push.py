"""Database restore (push) logic."""

from __future__ import annotations

import contextlib
import tempfile
import zipfile
from pathlib import Path

from sqlbackup.config import DbConfig
from sqlbackup.connection import DatabaseConnection
from sqlbackup.constants import (
    ERR_PUSH_FILE_NOT_FOUND,
    ERR_PUSH_ZIP_MULTIPLE_SQL,
    ERR_PUSH_ZIP_NO_SQL,
    SQL_EXT,
    ZIP_EXT,
)
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


def _extract_sql_from_zip(zip_path: Path, dest_dir: Path) -> Path:
    """Extract a single .sql member from *zip_path* into *dest_dir*.

    Raises PushError if the archive contains zero or multiple .sql files.
    """
    with zipfile.ZipFile(zip_path) as zf:
        sql_members = [n for n in zf.namelist() if n.lower().endswith(SQL_EXT)]
        if not sql_members:
            raise PushError(ERR_PUSH_ZIP_NO_SQL.format(path=zip_path))
        if len(sql_members) > 1:
            raise PushError(ERR_PUSH_ZIP_MULTIPLE_SQL.format(path=zip_path))
        return Path(zf.extract(sql_members[0], dest_dir))


def push_database(config: DbConfig, sql_path: Path) -> None:
    """Restore a .sql dump file to a database.

    If *sql_path* ends in ``.zip``, the archive is extracted to a temp dir and
    the contained ``.sql`` file is restored.

    Args:
        config: Database connection configuration.
        sql_path: Path to the SQL dump file (or a .zip containing one).
    """
    if not sql_path.exists():
        raise PushError(ERR_PUSH_FILE_NOT_FOUND.format(path=sql_path))

    if sql_path.suffix.lower() == ZIP_EXT:
        with tempfile.TemporaryDirectory() as td:
            extracted = _extract_sql_from_zip(sql_path, Path(td))
            statements = _parse_statements(extracted)
    else:
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
