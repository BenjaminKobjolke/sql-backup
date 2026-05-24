"""Database backup (dump) logic."""

from __future__ import annotations

import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any

from sqlbackup.config import DbConfig
from sqlbackup.connection import DatabaseConnection
from sqlbackup.constants import (
    DEFAULT_BATCH_SIZE,
    ERR_BACKUP_PATH_EXISTS,
    ERR_INCLUDE_EXCLUDE_MUTUAL,
    ERR_INCLUDE_MISSING_TABLES,
    SQL_DROP_TABLE,
    SQL_FOOTER,
    SQL_HEADER,
    TIMESTAMP_FORMAT,
    ZIP_EXT,
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


def cleanup_old_backups(base_path: Path, keep: int, zipped: bool = False) -> None:
    """Delete old incremental backups, keeping only the *keep* most recent.

    Matches files named ``*_{base_path.name}`` (or ``*_{stem}.zip`` when ``zipped``)
    in the parent directory.
    """
    pattern = f"*_{base_path.stem}{ZIP_EXT}" if zipped else f"*_{base_path.name}"
    matches = sorted(base_path.parent.glob(pattern))
    for old in matches[:-keep]:
        old.unlink()


def _zip_sql_file(sql_path: Path) -> Path:
    """Compress *sql_path* to a sibling .zip and delete the original.

    Returns the path to the resulting .zip file.
    """
    zip_path = sql_path.with_suffix(ZIP_EXT)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(sql_path, arcname=sql_path.name)
    sql_path.unlink()
    return zip_path


def _filter_tables(
    tables: list[str],
    includes: list[str] | None,
    excludes: list[str] | None,
) -> list[str]:
    """Return *tables* filtered by include/exclude lists.

    Caller must ensure *includes* and *excludes* are not both set.
    Raises BackupError if an include name is missing from *tables*.
    """
    if includes:
        missing = [t for t in includes if t not in tables]
        if missing:
            raise BackupError(ERR_INCLUDE_MISSING_TABLES.format(tables=", ".join(missing)))
        return [t for t in tables if t in includes]
    if excludes:
        return [t for t in tables if t not in excludes]
    return tables


def backup_database(
    config: DbConfig,
    output_path: Path,
    batch_size: int = DEFAULT_BATCH_SIZE,
    incremental: int | None = None,
    zip: bool = False,
    includes: list[str] | None = None,
    excludes: list[str] | None = None,
) -> Path:
    """Dump a database to a .sql file (optionally compressed as .zip).

    Args:
        config: Database connection configuration.
        output_path: Path for the output SQL file.
        batch_size: Number of rows per INSERT batch.
        incremental: If set, prepend a timestamp and keep only N backups.
        zip: If True, compress the output as a .zip archive (the .sql is removed).
        includes: If set, dump only these tables. Mutually exclusive with *excludes*.
        excludes: If set, dump all tables except these.

    Returns:
        The actual path the backup was written to (.sql or .zip).
    """
    if includes and excludes:
        raise BackupError(ERR_INCLUDE_EXCLUDE_MUTUAL)

    if incremental is not None:
        actual_path = resolve_incremental_path(output_path)
    else:
        actual_path = output_path
        if actual_path.exists():
            raise BackupError(ERR_BACKUP_PATH_EXISTS.format(path=actual_path))
        if zip:
            final_zip = actual_path.with_suffix(ZIP_EXT)
            if final_zip.exists():
                raise BackupError(ERR_BACKUP_PATH_EXISTS.format(path=final_zip))

    actual_path.parent.mkdir(parents=True, exist_ok=True)

    with DatabaseConnection(config) as db:
        tables = _filter_tables(db.get_tables(), includes, excludes)

        with open(actual_path, "w", encoding="utf-8") as f:
            now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
            f.write(SQL_HEADER.format(database=config.database, date=now))

            for table in tables:
                f.write(SQL_DROP_TABLE.format(table=table))

                ddl = db.get_create_table(table)
                f.write(ddl)
                f.write(";\n\n")

                _write_table_data(f, db, table, batch_size=batch_size)

            f.write(SQL_FOOTER)

    if zip:
        actual_path = _zip_sql_file(actual_path)

    if incremental is not None:
        cleanup_old_backups(output_path, keep=incremental, zipped=zip)

    return actual_path
