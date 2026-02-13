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

### Backup a database

```bash
sqlbackup --backup --config my_database --path backups/20260213_my_database.sql
```

### Restore a backup

```bash
sqlbackup --push --config my_database_local --path backups/20260213_my_database.sql
```

### Incremental backup

Use `--incremental N` to automatically prepend a timestamp to the filename and keep only the N most recent backups:

```bash
sqlbackup --backup --config my_database --path backups/my_database.sql --incremental 10
```

This produces files like `backups/20260213_143022_my_database.sql`. Once there are more than 10 matching backups, the oldest are deleted.

The `--config` value is the filename without `.json`. The `--path` value is the path to the `.sql` file.

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
