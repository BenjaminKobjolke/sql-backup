"""Direct database-to-database copy via intermediate temp .sql file."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

from sqlbackup.backup import backup_database
from sqlbackup.config import DbConfig
from sqlbackup.push import push_database


def copy_database(
    source: DbConfig,
    target: DbConfig,
    *,
    includes: list[str] | None = None,
    excludes: list[str] | None = None,
    force: bool = False,
) -> None:
    """Copy *source* DB to *target* DB by dumping to a temp .sql and restoring.

    The intermediate file lives in the system temp dir and is deleted after
    the push completes (or fails).

    Args:
        source: Source DB connection config (read).
        target: Target DB connection config (overwritten).
        includes: If set, copy only these tables. Mutually exclusive with *excludes*.
        excludes: If set, copy all tables except these.
        force: If False and target DB has tables, refuse the copy.
    """
    fd, tmp_name = tempfile.mkstemp(suffix=".sql", prefix="sqlbackup_copy_")
    os.close(fd)
    tmp_path = Path(tmp_name)
    # backup_database refuses to overwrite an existing file; remove the
    # empty placeholder mkstemp created.
    tmp_path.unlink()

    try:
        print(f"Dumping source DB '{source.database}'...")
        backup_database(source, tmp_path, includes=includes, excludes=excludes)
        print(f"Restoring to target DB '{target.database}'...")
        push_database(target, tmp_path, force=force)
    finally:
        tmp_path.unlink(missing_ok=True)
