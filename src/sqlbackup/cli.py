"""Command-line interface for sqlbackup."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from sqlbackup.backup import backup_database
from sqlbackup.config import load_config
from sqlbackup.exceptions import SqlBackupError
from sqlbackup.push import push_database


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sqlbackup",
        description="Backup and restore MySQL/MariaDB databases.",
    )

    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--backup", action="store_true", help="Dump database to SQL file")
    action.add_argument("--push", action="store_true", help="Restore SQL file to database")

    parser.add_argument("--config", required=True, help="Config name (without .json)")
    parser.add_argument("--path", required=True, help="Path to .sql file")

    return parser


def main() -> None:
    """CLI entry point."""
    parser = _build_parser()
    args = parser.parse_args()

    try:
        config = load_config(args.config)
        sql_path = Path(args.path)

        if args.backup:
            backup_database(config, sql_path)
            print(f"Backup complete: {sql_path}")
        elif args.push:
            push_database(config, sql_path)
            print(f"Push complete: {sql_path}")
    except SqlBackupError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
