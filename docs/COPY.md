# `--copy`

Clone one live database to another in a single command. Internally this dumps the source to a
temp `.sql` and restores it to the target; the temp file is removed afterwards.

```bash
sqlbackup --copy --source production --target test
```

Both `--source` and `--target` are config names (each pointing to a JSON file under
`configs/`). `--copy` does not accept `--config` or `--path`.

The target must be empty unless `--force` is supplied:

```bash
sqlbackup --copy --source production --target test --force
```

## Progress output

The dump phase prints the same [progress lines](BACKUP.md#progress-output) as `--backup`
(`Backup... 10%`, ...). The restore phase prints no progress:

```
Dumping source DB 'production'...
Backup... 10%
...
Backup... 100%
Restoring to target DB 'test'...
Copy complete: 'production' -> 'test'
```

## Limit tables

`--include-table` (whitelist) and `--exclude-table` (blacklist) are repeatable and mutually
exclusive:

```bash
# Only copy a whitelist
sqlbackup --copy --source production --target test --include-table users --include-table orders --force

# Copy everything except a blacklist
sqlbackup --copy --source production --target test --exclude-table audit_log --force
```
