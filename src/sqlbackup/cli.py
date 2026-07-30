"""Command-line interface for sqlbackup."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlbackup.backup import backup_database
from sqlbackup.config import load_config
from sqlbackup.constants import (
    ERR_BACKUP_PATH_REQUIRES_EXECUTE,
    ERR_COPY_REJECTS_CONFIG_PATH,
    ERR_COPY_REQUIRES_SOURCE_TARGET,
    ERR_DRY_RUN_REQUIRES_EXECUTE,
    ERR_FILTERS_REQUIRE_BACKUP_OR_COPY,
    ERR_FORCE_REQUIRES_PUSH_OR_COPY,
    ERR_INCLUDE_EXCLUDE_MUTUAL,
    ERR_INCREMENTAL_REQUIRES_BACKUP_PATH,
    ERR_YES_REQUIRES_EXECUTE,
    ERR_ZIP_REQUIRES_BACKUP,
)
from sqlbackup.copy import copy_database
from sqlbackup.exceptions import SqlBackupError
from sqlbackup.execute import execute_from_path
from sqlbackup.push import push_database


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sqlbackup",
        description="Backup, restore, and copy MySQL/MariaDB databases.",
    )

    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--backup", action="store_true", help="Dump database to SQL file")
    action.add_argument("--push", action="store_true", help="Restore SQL file to database")
    action.add_argument(
        "--copy", action="store_true", help="Copy a live DB to another DB (source -> target)"
    )
    action.add_argument(
        "--execute",
        action="store_true",
        help="Run a SQL file against an existing database (no empty-target check)",
    )

    parser.add_argument(
        "--config", help="Config name (without .json) for --backup/--push/--execute"
    )
    parser.add_argument(
        "--path",
        help="Path to .sql file for --backup/--push, or a .sql file/folder for --execute",
    )
    parser.add_argument("--source", help="Source config name for --copy")
    parser.add_argument("--target", help="Target config name for --copy")
    parser.add_argument(
        "--incremental",
        type=int,
        default=None,
        help="Keep N most recent timestamped backups (--backup, or --execute --backup-path)",
    )
    parser.add_argument(
        "--zip",
        action="store_true",
        help=(
            "Compress backup as .zip (--backup, or --execute --backup-path; "
            "--push auto-detects .zip)"
        ),
    )
    parser.add_argument(
        "--include-table",
        action="append",
        dest="include_tables",
        default=None,
        help="Only include this table (repeatable). Valid with --backup/--copy.",
    )
    parser.add_argument(
        "--exclude-table",
        action="append",
        dest="exclude_tables",
        default=None,
        help="Skip this table (repeatable). Valid with --backup/--copy.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite a non-empty target DB. Valid with --push/--copy.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and preview statements without connecting to the DB. Valid with --execute.",
    )
    parser.add_argument(
        "--backup-path",
        help=(
            "Safety-backup file/dir written before --execute runs the SQL. "
            "Reuses --incremental/--zip."
        ),
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help=(
            "Skip the interactive file prompt; auto-select the newest .sql in --path. "
            "Valid with --execute."
        ),
    )

    return parser


def _validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    execute_with_backup = args.execute and args.backup_path
    if args.zip and not (args.backup or execute_with_backup):
        parser.error(ERR_ZIP_REQUIRES_BACKUP)
    if args.include_tables and args.exclude_tables:
        parser.error(ERR_INCLUDE_EXCLUDE_MUTUAL)
    if (args.include_tables or args.exclude_tables) and not (args.backup or args.copy):
        parser.error(ERR_FILTERS_REQUIRE_BACKUP_OR_COPY)
    if args.force and not (args.push or args.copy):
        parser.error(ERR_FORCE_REQUIRES_PUSH_OR_COPY)
    if args.dry_run and not args.execute:
        parser.error(ERR_DRY_RUN_REQUIRES_EXECUTE)
    if args.backup_path and not args.execute:
        parser.error(ERR_BACKUP_PATH_REQUIRES_EXECUTE)
    if args.yes and not args.execute:
        parser.error(ERR_YES_REQUIRES_EXECUTE)
    if args.incremental is not None and args.execute and not args.backup_path:
        parser.error(ERR_INCREMENTAL_REQUIRES_BACKUP_PATH)

    if args.copy:
        if not args.source or not args.target:
            parser.error(ERR_COPY_REQUIRES_SOURCE_TARGET)
        if args.config or args.path:
            parser.error(ERR_COPY_REJECTS_CONFIG_PATH)
    else:
        if not args.config:
            parser.error("--config is required for --backup/--push/--execute.")
        if not args.path:
            parser.error("--path is required for --backup/--push/--execute.")


def main() -> None:
    """CLI entry point."""
    parser = _build_parser()
    args = parser.parse_args()
    _validate_args(parser, args)

    try:
        if args.backup:
            config = load_config(args.config)
            actual_path = backup_database(
                config,
                Path(args.path),
                incremental=args.incremental,
                zip=args.zip,
                includes=args.include_tables,
                excludes=args.exclude_tables,
            )
            print(f"Backup complete: {actual_path}")
        elif args.push:
            config = load_config(args.config)
            sql_path = Path(args.path)
            push_database(config, sql_path, force=args.force)
            print(f"Push complete: {sql_path}")
        elif args.execute:
            config = load_config(args.config)
            backup_path = Path(args.backup_path) if args.backup_path else None
            sql_target, backup_written, count = execute_from_path(
                config,
                Path(args.path),
                backup_path=backup_path,
                incremental=args.incremental,
                zip=args.zip,
                assume_yes=args.yes,
                dry_run=args.dry_run,
            )
            if backup_written is not None:
                print(f"Backup complete: {backup_written}")
            verb = "Would execute" if args.dry_run else "Executed"
            print(f"{verb} {count} statement(s): {sql_target}")
        elif args.copy:
            source = load_config(args.source)
            target = load_config(args.target)
            copy_database(
                source,
                target,
                includes=args.include_tables,
                excludes=args.exclude_tables,
                force=args.force,
            )
            print(f"Copy complete: '{source.database}' -> '{target.database}'")
    except SqlBackupError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
