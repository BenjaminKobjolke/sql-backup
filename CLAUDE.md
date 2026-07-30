# SQL Backup Tool

## Build / Run / Test Commands
- Install: `install.bat` or `uv sync --all-extras`
- Run: `uv run sqlbackup --backup --config <name> --path <file.sql>`
- Run: `uv run sqlbackup --push --config <name> --path <file.sql>`
- Run: `uv run sqlbackup --execute --config <name> --path <file.sql>` (run a SQL file against an existing DB; add `--dry-run` to preview without connecting)
- Run: `uv run sqlbackup --execute --config <name> --path <folder> --backup-path <file.sql> --incremental 30 --zip` (pick a revision from a folder, newest preselected / `--yes` for newest without prompting; back up first)
- Test all: `uv run pytest tests/ -v`
- Test single: `uv run pytest tests/test_config.py -v`
- Lint: `uv run ruff check src/ tests/`
- Format: `uv run ruff format src/ tests/`
- Type check: `uv run mypy src/`

## Code Style & Conventions
- Python 3.11+, strict mypy, ruff for linting
- Frozen dataclasses for config objects
- Custom exception hierarchy in `exceptions.py`
- String constants centralized in `constants.py`
- pymysql with raw SQL (not SQLAlchemy) — DDL faithfulness requires it
- TDD: tests first, implementation second
- Batched INSERTs (1000 rows default) for memory efficiency
- Server-side cursors for large table iteration

## Code Analysis

After implementing new features or making significant changes, run the code analysis:

```bash
powershell -Command "cd 'D:\GIT\BenjaminKobjolke\sql-backup'; cmd /c '.\tools\analyze_code.bat'"
```

Fix any reported issues before committing.

## Project Structure
- `src/sqlbackup/` — main package
- `tests/` — pytest test suite
- `configs/` — JSON credential files (gitignored, copy from `configs_example/`)
- `configs_example/` — example config template (committed)
- `docs/` — one markdown file per CLI command (`BACKUP.md`, `PUSH.md`, `EXECUTE.md`, `COPY.md`); README.md links to these instead of duplicating usage docs
