"""Execute an arbitrary SQL file against an existing (non-empty) database."""

from __future__ import annotations

import re
import tempfile
from pathlib import Path

from sqlbackup.backup import backup_database, resolve_incremental_path
from sqlbackup.config import DbConfig
from sqlbackup.connection import DatabaseConnection
from sqlbackup.constants import (
    ERR_EXECUTE_DELIMITER_UNSUPPORTED,
    ERR_EXECUTE_EMPTY,
    ERR_EXECUTE_FILE_NOT_FOUND,
    ERR_EXECUTE_NO_SQL_IN_FOLDER,
    ERR_EXECUTE_PATH_NOT_FOUND,
    ERR_EXECUTE_UNTERMINATED,
    SQL_EXT,
    ZIP_EXT,
)
from sqlbackup.exceptions import ExecuteError
from sqlbackup.push import _extract_sql_from_zip, _parse_statements

_DELIMITER_RE = re.compile(r"^\s*DELIMITER\b", re.IGNORECASE)
_PREVIEW_LEN = 80


def _check_no_custom_delimiter(sql_path: Path, display_path: Path) -> None:
    """Reject files using DELIMITER blocks (stored procs/triggers) - unsupported."""
    with open(sql_path, encoding="utf-8") as f:
        for line in f:
            if _DELIMITER_RE.match(line):
                raise ExecuteError(ERR_EXECUTE_DELIMITER_UNSUPPORTED.format(path=display_path))


def _validate_parsed(actual_path: Path, display_path: Path) -> list[str]:
    _check_no_custom_delimiter(actual_path, display_path)
    statements, leftover = _parse_statements(actual_path)
    if leftover.strip():
        raise ExecuteError(ERR_EXECUTE_UNTERMINATED.format(path=display_path))
    if not statements:
        raise ExecuteError(ERR_EXECUTE_EMPTY.format(path=display_path))
    return statements


def validate_sql_file(sql_path: Path) -> list[str]:
    """Parse and validate a SQL file without connecting to the database.

    Raises ExecuteError if the file is missing, empty, uses an unsupported
    DELIMITER block, or ends mid-statement/mid string literal.
    """
    if not sql_path.exists():
        raise ExecuteError(ERR_EXECUTE_FILE_NOT_FOUND.format(path=sql_path))

    if sql_path.suffix.lower() == ZIP_EXT:
        with tempfile.TemporaryDirectory() as td:
            extracted = _extract_sql_from_zip(sql_path, Path(td))
            return _validate_parsed(extracted, sql_path)
    return _validate_parsed(sql_path, sql_path)


def execute_sql_file(config: DbConfig, sql_path: Path, *, dry_run: bool = False) -> int:
    """Validate and run every statement in *sql_path* against *config*'s database.

    Unlike push_database, this does not require an empty target and never
    asks for --force - running SQL against an already-populated database is
    the point. Stops at the first failing statement (no transaction wrapper:
    MySQL DDL auto-commits, so a rollback couldn't undo an ALTER anyway).

    If dry_run is True, statements are parsed/validated and previewed but no
    database connection is made.
    """
    statements = validate_sql_file(sql_path)

    if dry_run:
        for stmt in statements:
            preview = " ".join(stmt.split())[:_PREVIEW_LEN]
            print(f"  {preview}")
        return len(statements)

    with DatabaseConnection(config) as db:
        db.execute_sql("SET SESSION net_read_timeout = 600")
        db.execute_sql("SET SESSION net_write_timeout = 600")
        for stmt in statements:
            db.execute_sql(stmt)

    return len(statements)


def _list_sql_files(folder: Path) -> list[Path]:
    """Return .sql files in *folder*, newest (by modified time) first."""
    files = [p for p in folder.iterdir() if p.is_file() and p.suffix.lower() == SQL_EXT]
    return sorted(files, key=lambda p: p.stat().st_mtime, reverse=True)


def _select_sql_file(folder: Path, *, assume_yes: bool) -> Path:
    """Prompt the user to pick a .sql file from *folder*, newest preselected."""
    files = _list_sql_files(folder)
    if not files:
        raise ExecuteError(ERR_EXECUTE_NO_SQL_IN_FOLDER.format(path=folder))

    if assume_yes:
        return files[0]

    print(f"SQL files in {folder} (newest first):")
    for i, f in enumerate(files, start=1):
        marker = "   [default]" if i == 1 else ""
        print(f"  {i}) {f.name}{marker}")

    for _attempt in range(3):
        try:
            choice = input("Select a file to execute [1]: ").strip()
        except EOFError:
            return files[0]
        if not choice:
            return files[0]
        if choice.isdigit() and 1 <= int(choice) <= len(files):
            return files[int(choice) - 1]
        print(f"Enter a number between 1 and {len(files)}.")

    return files[0]


def resolve_sql_target(path: Path, *, assume_yes: bool = False) -> Path:
    """Resolve *path* to a SQL file - itself if a file, or an interactive/auto pick if a folder."""
    if not path.exists():
        raise ExecuteError(ERR_EXECUTE_PATH_NOT_FOUND.format(path=path))
    if path.is_dir():
        return _select_sql_file(path, assume_yes=assume_yes)
    return path


def execute_from_path(
    config: DbConfig,
    path: Path,
    *,
    backup_path: Path | None = None,
    incremental: int | None = None,
    zip: bool = False,
    assume_yes: bool = False,
    dry_run: bool = False,
) -> tuple[Path, Path | None, int]:
    """Resolve *path* (file or folder), optionally back up, then execute it.

    Returns (target_file, backup_written_or_None, statement_count).

    If *path* is a folder, the user is prompted to pick a .sql file from it
    (newest preselected; --yes/assume_yes auto-picks the newest). The file is
    validated before any backup is taken, so a bad revision file fails before
    touching the database. If dry_run is True, no backup is taken and no
    database connection is made at all.
    """
    target = resolve_sql_target(path, assume_yes=assume_yes)
    validate_sql_file(target)

    if dry_run:
        count = execute_sql_file(config, target, dry_run=True)
        return target, None, count

    backup_written = None
    if backup_path is not None:
        base = (
            backup_path
            if backup_path.suffix.lower() == SQL_EXT
            else backup_path / f"{config.database}{SQL_EXT}"
        )
        if incremental is not None:
            backup_written = backup_database(config, base, incremental=incremental, zip=zip)
        else:
            backup_written = backup_database(config, resolve_incremental_path(base), zip=zip)

    count = execute_sql_file(config, target)
    return target, backup_written, count
