# `--execute`

Run a migration/revision file (e.g. one `ALTER`/`INSERT` file per change) against a database
that already has tables. Unlike [`--push`](PUSH.md), there's no empty-target check and no
`--force` needed:

```bash
sqlbackup --execute --config my_database --path dbrevisions/20260730_translations_emojipicker.sql
```

The file is validated before anything is sent to the database: missing file, empty file, a
`DELIMITER $$` block (stored procedures/triggers aren't supported — the statement splitter only
understands `;`), and a file that ends mid-statement all fail with a clear error and no DB
connection is opened.

If any statement fails, execution stops immediately — statements before it have already been
committed (no transaction wrapper, since MySQL DDL auto-commits anyway).

## `--dry-run`

Validate and preview the parsed statements without connecting to the database at all:

```bash
sqlbackup --execute --config my_database --path dbrevisions/20260730_translations_emojipicker.sql --dry-run
```

## Pick a revision file from a folder

Point `--path` at a folder instead of a file and `--execute` lists the `.sql` files in it
(newest first, by modified time) and asks which to run:

```bash
sqlbackup --execute --config my_database --path dbrevisions
```

```
SQL files in dbrevisions (newest first):
  1) 20260730_translations_emojipicker.sql   [default]
  2) 20260729_translations_flow_embed_height.sql
  3) 20260724_translations_slotmachine.sql
Select a file to execute [1]:
```

Press enter to accept the default (newest), or type a number. Add `--yes` (`-y`) to skip the
prompt entirely and auto-pick the newest — useful in scripts/CI.

## Safety backup before executing

Add `--backup-path` to back up the target database immediately before the SQL runs. It reuses
the same `--incremental`/`--zip` flags as [`--backup`](BACKUP.md):

```bash
sqlbackup --execute --config my_database --path dbrevisions \
  --backup-path backups/my_database.sql --incremental 30 --zip
```

`--backup-path` can be a `.sql` file (used as the backup base name, same as `--backup --path`)
or a directory (the dump is auto-named `<database>.sql` inside it). The chosen revision file is
validated *before* the backup is taken, so a bad file fails fast without an unnecessary backup.
`--dry-run` skips the backup step too (no DB connection at all).

`--incremental`/`--zip` with `--execute` require `--backup-path` — there's nothing to timestamp
or compress otherwise.
