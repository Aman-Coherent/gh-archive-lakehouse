"""Tests for pipeline.retention: deletes old bronze, never touches gold."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest

from pipeline.config import Settings
from pipeline.retention import enforce_retention
from pipeline.storage import Storage


@pytest.fixture(autouse=True)
def _local_storage_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("BRONZE_RETENTION_DAYS", "7")


def _write_bronze_partition(event_date: str, hour: str) -> None:
    storage = Storage(Settings.load())
    table = pa.table({"event_id": [1]})
    storage.write_parquet_atomic(
        table, f"bronze/events/event_date={event_date}/hour={hour}/part-0.parquet"
    )


def _write_gold_file(tmp_path: Path) -> Path:
    gold_dir = tmp_path / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)
    path = gold_dir / "fact_events.parquet"
    path.write_bytes(b"not a real parquet file, just needs to exist and be measurable")
    return path


def test_deletes_only_partitions_older_than_retention_window(tmp_path: Path) -> None:
    today = datetime.now(UTC).date()
    old_date = (today - timedelta(days=10)).isoformat()
    recent_date = (today - timedelta(days=1)).isoformat()
    _write_bronze_partition(old_date, "05")
    _write_bronze_partition(recent_date, "05")

    result = enforce_retention()

    assert result.dates_deleted == 1
    assert result.files_deleted == 1
    assert result.bytes_reclaimed > 0

    storage = Storage(Settings.load())
    assert not storage.exists(f"bronze/events/event_date={old_date}/hour=05/part-0.parquet")
    assert storage.exists(f"bronze/events/event_date={recent_date}/hour=05/part-0.parquet")


def test_gold_is_never_touched(tmp_path: Path) -> None:
    old_date = (datetime.now(UTC).date() - timedelta(days=30)).isoformat()
    _write_bronze_partition(old_date, "00")
    gold_path = _write_gold_file(tmp_path)
    original_bytes = gold_path.read_bytes()

    enforce_retention()

    assert gold_path.exists()
    assert gold_path.read_bytes() == original_bytes


def test_noop_when_nothing_is_old_enough(tmp_path: Path) -> None:
    recent_date = (datetime.now(UTC).date() - timedelta(days=1)).isoformat()
    _write_bronze_partition(recent_date, "00")

    result = enforce_retention()

    assert result.dates_deleted == 0
    assert result.files_deleted == 0
    assert result.bytes_reclaimed == 0


def test_works_with_a_relative_local_data_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # WHY this test exists: LOCAL_DATA_ROOT defaults to the relative "./data"
    # in real usage, but every other test here uses tmp_path, which pytest
    # always hands out as an absolute path — that masked a real bug where
    # Storage.list_partitions() mis-stripped paths for a relative root
    # (fsspec's find() returns absolute paths regardless of the root given),
    # producing a garbled path and a FileNotFoundError. Caught by actually
    # running `pipeline retention` for real against this project's real
    # bronze data, not by the other tests in this file.
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("LOCAL_DATA_ROOT", "./data")

    old_date = (datetime.now(UTC).date() - timedelta(days=30)).isoformat()
    _write_bronze_partition(old_date, "00")

    result = enforce_retention()

    assert result.dates_deleted == 1
    assert result.files_deleted == 1
