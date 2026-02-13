"""Database backup (dump) logic."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any

from sqlbackup.config import DbConfig
from sqlbackup.connection import DatabaseConnection
from sqlbackup.constants import (
    DEFAULT_BATCH_SIZE,
    ERR_BACKUP_PATH_EXISTS,
    SQL_DROP_TABLE,
    SQL_FOOTER,
    SQL_HEADER,
    TIMESTAMP_FORMAT,
)
from sqlbackup.exceptions import BackupError


def _format_value(value: Any) -> str:
    """Format a Python value as a SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, bytes):
        return "X'" + value.hex() + "'"
    s = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{s}'"


def _write_table_data(
    f: IO[str],
    db: DatabaseConnection,
    table: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> None:
    """Write INSERT statements for a table's data."""
    columns = db.get_column_names(table)
    if not columns:
        return

    col_list = ", ".join(f"`{c}`" for c in columns)

    for batch in db.iter_rows(table, batch_size=batch_size):
        f.write(f"INSERT INTO `{table}` ({col_list}) VALUES\n")
        row_strings: list[str] = []
        for row in batch:
            values = ", ".join(_format_value(v) for v in row)
            row_strings.append(f"({values})")
        f.write(",\n".join(row_strings))
        f.write(";\n\n")


def resolve_incremental_path(base_path: Path) -> Path:
    """Return a timestamped version of the given path.

    Example: ``backups/db.sql`` becomes ``backups/20260213_143022_db.sql``.
    """
    timestamp = datetime.now(UTC).strftime(TIMESTAMP_FORMAT)
    return base_path.parent / f"{timestamp}_{base_path.name}"


def cleanup_old_backups(base_path: Path, keep: int) -> None:
    """Delete old incremental backups, keeping only the *keep* most recent.

    Matches files named ``*_{base_path.name}`` in the parent directory.
    """
    pattern = f"*_{base_path.name}"
    matches = sorted(base_path.parent.glob(pattern))
    for old in matches[:-keep]:
        old.unlink()


def backup_database(
    config: DbConfig,
    output_path: Path,
    batch_size: int = DEFAULT_BATCH_SIZE,
    incremental: int | None = None,
) -> Path:
    """Dump a database to a .sql file.

    Args:
        config: Database connection configuration.
        output_path: Path for the output SQL file.
        batch_size: Number of rows per INSERT batch.
        incremental: If set, prepend a timestamp and keep only N backups.

    Returns:
        The actual path the backup was written to.
    """
    if incremental is not None:
        actual_path = resolve_incremental_path(output_path)
    else:
        actual_path = output_path
        if actual_path.exists():
            raise BackupError(ERR_BACKUP_PATH_EXISTS.format(path=actual_path))

    actual_path.parent.mkdir(parents=True, exist_ok=True)

    with DatabaseConnection(config) as db, open(actual_path, "w", encoding="utf-8") as f:
        now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
        f.write(SQL_HEADER.format(database=config.database, date=now))

        tables = db.get_tables()
        for table in tables:
            f.write(SQL_DROP_TABLE.format(table=table))

            ddl = db.get_create_table(table)
            f.write(ddl)
            f.write(";\n\n")

            _write_table_data(f, db, table, batch_size=batch_size)

        f.write(SQL_FOOTER)

    if incremental is not None:
        cleanup_old_backups(output_path, keep=incremental)

    return actual_path
