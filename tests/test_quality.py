"""Tests for pipeline.quality: freshness is a claim about recency, not correctness."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from pipeline.quality import check_freshness


@pytest.fixture(autouse=True)
def _local_storage_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("FRESHNESS_SLA_HOURS", "3")


def _write_fact_events(tmp_path: Path, latest_created_at: datetime) -> None:
    gold_dir = tmp_path / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)
    table = pa.table({"event_id": [1], "created_at": [latest_created_at]})
    con = duckdb.connect()
    con.register("fact_events_tbl", table)
    path = (gold_dir / "fact_events.parquet").as_posix()
    con.execute(f"COPY fact_events_tbl TO '{path}' (FORMAT PARQUET)")


def test_fresh_when_latest_event_within_sla(tmp_path: Path) -> None:
    _write_fact_events(tmp_path, datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=1))

    result = check_freshness()

    assert result.is_fresh is True
    assert result.age_hours is not None
    assert result.age_hours < 3


def test_stale_when_latest_event_beyond_sla(tmp_path: Path) -> None:
    _write_fact_events(tmp_path, datetime.now(UTC).replace(tzinfo=None) - timedelta(hours=10))

    result = check_freshness()

    assert result.is_fresh is False
    assert result.age_hours is not None
    assert result.age_hours > 3


def test_stale_when_gold_does_not_exist_yet(tmp_path: Path) -> None:
    # WHY no fixture write here: this asserts the graceful-degradation path
    # for a pipeline that hasn't produced gold data at all yet.
    result = check_freshness()

    assert result.is_fresh is False
    assert result.latest_event_at is None
