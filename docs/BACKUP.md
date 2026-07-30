# `--backup`

Dump a database to a `.sql` file.

```bash
sqlbackup --backup --config my_database --path backups/20260213_my_database.sql
```

Fails if the output file already exists (use a different `--path`, or use `--incremental`
to timestamp it automatically).

## Incremental backup

Use `--incremental N` to automatically prepend a timestamp to the filename and keep only the
N most recent backups:

```bash
sqlbackup --backup --config my_database --path backups/my_database.sql --incremental 10
```

This produces files like `backups/20260213_143022_my_database.sql`. Once there are more than
10 matching backups, the oldest are deleted.

## Compress as `.zip`

Add `--zip` to compress the dump and remove the uncompressed `.sql`:

```bash
sqlbackup --backup --config my_database --path backups/my_database.sql --incremental 30 --zip
```

## Limit tables

`--include-table` (whitelist) and `--exclude-table` (blacklist) are repeatable and mutually
exclusive:

```bash
# Only dump a whitelist
sqlbackup --backup --config my_database --path backups/my_database.sql --include-table users --include-table orders

# Dump everything except a blacklist
sqlbackup --backup --config my_database --path backups/my_database.sql --exclude-table audit_log
```

An unknown `--include-table` name raises an error.

See also: [`--execute --backup-path`](EXECUTE.md#safety-backup-before-executing), which reuses
`--incremental`/`--zip` to take a safety backup before running a SQL file.
