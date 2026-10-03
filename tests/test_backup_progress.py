"""Tests for backup progress output."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from sqlbackup.backup import backup_database
from sqlbackup.config import DbConfig


def _dump(tmp_path: Path, estimate: int, zip: bool = False) -> None:
    """Dump one 20-row table (10 batches of 2) whose row estimate is *estimate*."""
    mock = MagicMock()
    mock.__enter__ = MagicMock(return_value=mock)
    mock.__exit__ = MagicMock(return_value=False)
    mock.get_tables.return_value = ["users"]
    mock.get_create_table.return_value = "CREATE TABLE `users` (id INT)"
    mock.get_column_names.return_value = ["id"]
    mock.get_row_estimates.return_value = {"users": estimate}
    mock.iter_rows.return_value = iter([[(i,), (i + 1,)] for i in range(0, 20, 2)])
    config = DbConfig(host="localhost", port=3306, user="root", password="secret", database="db")

    with patch("sqlbackup.backup.DatabaseConnection", return_value=mock):
        backup_database(config, tmp_path / "dump.sql", zip=zip)


class TestBackupProgress:
    def test_prints_each_step_once(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        _dump(tmp_path, estimate=20)

        lines = capsys.readouterr().out.splitlines()
        assert lines == [f"Backup... {pct}%" for pct in range(10, 101, 10)]

    def test_low_estimate_reports_100_only_at_end(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        _dump(tmp_path, estimate=4)

        lines = capsys.readouterr().out.splitlines()
        assert lines.count("Backup... 100%") == 1
        assert lines[-1] == "Backup... 100%"

    def test_no_estimate_prints_nothing(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        _dump(tmp_path, estimate=0)

        assert capsys.readouterr().out == ""

    def test_zip_announced(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        _dump(tmp_path, estimate=20, zip=True)

        assert capsys.readouterr().out.splitlines()[-1] == "Zipping backup..."
