# `--push`

Restore a `.sql` dump (as produced by [`--backup`](BACKUP.md)) into a database.

```bash
sqlbackup --push --config my_database_local --path backups/20260213_my_database.sql
```

By default, `--push` refuses to write to a target database that already contains tables. Pass
`--force` to overwrite an existing schema:

```bash
sqlbackup --push --config my_database_local --path backups/20260213_my_database.sql --force
```

## `.zip` input

If `--path` points at a `.zip` file, it's extracted to a temp dir and the single `.sql` member
inside it is restored. The archive must contain exactly one `.sql` file.

## When to use `--push` vs `--execute`

`--push` is for restoring a **full database dump** — it expects an empty target unless you pass
`--force`, since overwriting an existing schema is the whole point. For running a **small
migration/revision file** against a database that already has data, use
[`--execute`](EXECUTE.md) instead — it never requires `--force` and adds pre-flight validation.
