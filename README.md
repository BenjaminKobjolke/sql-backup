# SQL Backup

CLI tool to backup and restore MySQL/MariaDB databases. Pure Python implementation using pymysql — no `mysqldump` binary required.

## Setup

```bash
install.bat
```

Requires [uv](https://docs.astral.sh/uv/getting-started/installation/) to be installed.

## Configuration

1. Copy the example folder to create your configs directory:

```bash
cp -r configs_example configs
```

2. Copy `example.json` and rename it for your database:

```bash
cp configs/example.json configs/my_database.json
```

3. Edit the new file with your database credentials:

```json
{
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "secret",
    "database": "my_db"
}
```

The `configs/` folder is gitignored. You can create as many config files as you need.

## Usage

Four commands, each documented in its own file:

| Command | Purpose |
|---|---|
| [`--backup`](docs/BACKUP.md) | Dump a database to a `.sql` file (incremental, `.zip`, table filters) |
| [`--push`](docs/PUSH.md) | Restore a `.sql`/`.zip` dump into a database |
| [`--execute`](docs/EXECUTE.md) | Run a migration/revision file (or pick one from a folder) against an existing database, with an optional safety backup first |
| [`--copy`](docs/COPY.md) | Clone a live database to another database in one command |

Quick example:

```bash
sqlbackup --backup --config my_database --path backups/20260213_my_database.sql
```

The `--config`, `--source`, and `--target` values are config filenames under `configs/`. The `.json` suffix is optional (`my_database` and `my_database.json` both resolve to the same file). Absolute paths are also accepted. The `--path` value is the path to a `.sql` file — for `--execute` it can also be a folder (see [docs/EXECUTE.md](docs/EXECUTE.md#pick-a-revision-file-from-a-folder)).

## Development

```bash
# Run tests
tools\tests.bat

# Lint
uv run ruff check src/ tests/

# Format
uv run ruff format src/ tests/

# Type check
uv run mypy src/

# Update dependencies
update.bat
```
