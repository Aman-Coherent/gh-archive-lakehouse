"""Tests for pipeline.dbt_results: parses dbt's own run_results.json faithfully."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.dbt_results import record_dbt_results


@pytest.fixture(autouse=True)
def _local_storage_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))


def _write_run_results(tmp_path: Path, statuses: list[str]) -> Path:
    path = tmp_path / "run_results.json"
    results = [
        {"unique_id": f"test.gh_archive_lakehouse.some_test_{i}", "status": status}
        for i, status in enumerate(statuses)
    ]
    # WHY a non-test node is included: run_results.json also lists every
    # model/seed/snapshot dbt ran — this proves the filter only counts
    # unique_ids that actually start with "test.".
    results.append({"unique_id": "model.gh_archive_lakehouse.stg_events", "status": "success"})
    path.write_text(json.dumps({"results": results}), encoding="utf-8")
    return path


def test_counts_test_statuses_correctly(tmp_path: Path) -> None:
    path = _write_run_results(tmp_path, ["pass", "pass", "warn", "fail", "error"])

    counts = record_dbt_results(str(path))

    assert counts == {
        "total_tests": 5,
        "passed_tests": 2,
        "warned_tests": 1,
        "failed_tests": 2,
    }


def test_returns_none_when_file_missing(tmp_path: Path) -> None:
    counts = record_dbt_results(str(tmp_path / "does_not_exist.json"))

    assert counts is None
