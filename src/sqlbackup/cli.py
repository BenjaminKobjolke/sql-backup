"""Command-line interface for sqlbackup."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlbackup.backup import backup_database
from sqlbackup.config import load_config
from sqlbackup.constants import (
    ERR_COPY_REJECTS_CONFIG_PATH,
    ERR_COPY_REQUIRES_SOURCE_TARGET,
    ERR_FILTERS_REQUIRE_BACKUP_OR_COPY,
    ERR_FORCE_REQUIRES_PUSH_OR_COPY,
    ERR_INCLUDE_EXCLUDE_MUTUAL,
    ERR_ZIP_REQUIRES_BACKUP,
)
from sqlbackup.copy import copy_database
from sqlbackup.exceptions import SqlBackupError
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

    parser.add_argument("--config", help="Config name (without .json) for --backup/--push")
    parser.add_argument("--path", help="Path to .sql file for --backup/--push")
    parser.add_argument("--source", help="Source config name for --copy")
    parser.add_argument("--target", help="Target config name for --copy")
    parser.add_argument(
        "--incremental",
        type=int,
        default=None,
        help="Keep N most recent timestamped backups (only with --backup)",
    )
    parser.add_argument(
        "--zip",
        action="store_true",
        help="Compress backup as .zip (only with --backup; --push auto-detects .zip)",
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

    return parser


def _validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.zip and not args.backup:
        parser.error(ERR_ZIP_REQUIRES_BACKUP)
    if args.include_tables and args.exclude_tables:
        parser.error(ERR_INCLUDE_EXCLUDE_MUTUAL)
    if (args.include_tables or args.exclude_tables) and not (args.backup or args.copy):
        parser.error(ERR_FILTERS_REQUIRE_BACKUP_OR_COPY)
    if args.force and not (args.push or args.copy):
        parser.error(ERR_FORCE_REQUIRES_PUSH_OR_COPY)

    if args.copy:
        if not args.source or not args.target:
            parser.error(ERR_COPY_REQUIRES_SOURCE_TARGET)
        if args.config or args.path:
            parser.error(ERR_COPY_REJECTS_CONFIG_PATH)
    else:
        if not args.config:
            parser.error("--config is required for --backup/--push.")
        if not args.path:
            parser.error("--path is required for --backup/--push.")


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
