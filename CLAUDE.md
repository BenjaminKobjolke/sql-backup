# SQL Backup Tool

## Build / Run / Test Commands
- Install: `install.bat` or `uv sync --all-extras`
- Run: `uv run sqlbackup --backup --config <name> --path <file.sql>`
- Run: `uv run sqlbackup --push --config <name> --path <file.sql>`
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

## Project Structure
- `src/sqlbackup/` — main package
- `tests/` — pytest test suite
- `configs/` — JSON credential files (gitignored, copy from `configs_example/`)
- `configs_example/` — example config template (committed)
