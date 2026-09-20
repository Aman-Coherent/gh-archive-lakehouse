"""Tests for pipeline.ingest: idempotency, malformed-line handling, event-type filtering."""

from __future__ import annotations

import gzip
import io
import json
from datetime import datetime
from pathlib import Path

import pytest

from pipeline import ingest as ingest_module
from pipeline.ingest import ingest_hour, partition_path


def _make_event(event_id: int, event_type: str, **extra: object) -> dict:
    base = {
        "id": str(event_id),
        "type": event_type,
        "created_at": "2024-01-15T12:00:00Z",
        "actor": {"id": 1, "login": "octocat"},
        "repo": {"id": 10, "name": "octocat/hello-world"},
    }
    base.update(extra)
    return base


def _gzip_lines(lines: list[str]) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(("\n".join(lines) + "\n").encode("utf-8"))
    return buf.getvalue()


class _FakeResponse:
    def __init__(self, body: bytes, status_code: int = 200) -> None:
        self.raw = io.BytesIO(body)
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


@pytest.fixture(autouse=True)
def _local_storage_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # WHY autouse: every test in this file ingests into an isolated temp dir —
    # nothing here should ever touch a developer's real ./data.
    monkeypatch.setenv("STORAGE_BACKEND", "local")
    monkeypatch.setenv("LOCAL_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("EVENT_TYPES", "PushEvent,PullRequestEvent")


def _patch_download(monkeypatch: pytest.MonkeyPatch, lines: list[str]) -> None:
    body = _gzip_lines(lines)
    monkeypatch.setattr(ingest_module.requests, "get", lambda *a, **k: _FakeResponse(body))


def test_event_type_filter(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    lines = [
        json.dumps(_make_event(1, "PushEvent")),
        json.dumps(_make_event(2, "WatchEvent")),  # not in EVENT_TYPES, dropped
        json.dumps(
            _make_event(
                3, "PullRequestEvent", payload={"action": "opened", "pull_request": {"number": 7}}
            )
        ),
    ]
    _patch_download(monkeypatch, lines)

    result = ingest_hour(datetime(2024, 1, 15, 12))

    assert result.status == "success"
    assert result.rows_read == 3
    assert result.rows_kept == 2  # WatchEvent filtered out
    assert result.malformed_rows == 0


def test_malformed_lines_counted_not_fatal(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    lines = [
        json.dumps(_make_event(1, "PushEvent")),
        "{not valid json",
        json.dumps({"type": "PushEvent"}),  # missing required id/actor -> malformed
    ]
    _patch_download(monkeypatch, lines)

    result = ingest_hour(datetime(2024, 1, 15, 13))

    assert result.status == "success"
    assert result.rows_read == 3
    assert result.rows_kept == 1
    assert result.malformed_rows == 2


def test_idempotent_rerun_skips_and_leaves_file_untouched(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _patch_download(monkeypatch, [json.dumps(_make_event(1, "PushEvent"))])

    dt = datetime(2024, 1, 15, 14)
    first = ingest_hour(dt)
    assert first.status == "success"

    written_path = tmp_path / partition_path(dt)
    first_mtime = written_path.stat().st_mtime_ns

    def _fail_if_called(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("should not re-download on a skipped run")

    monkeypatch.setattr(ingest_module.requests, "get", _fail_if_called)
    second = ingest_hour(dt)

    assert second.status == "skipped"
    assert written_path.stat().st_mtime_ns == first_mtime


def test_force_reingest_overwrites_rather_than_duplicates(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    _patch_download(monkeypatch, [json.dumps(_make_event(1, "PushEvent"))])
    dt = datetime(2024, 1, 15, 16)
    ingest_hour(dt)

    _patch_download(
        monkeypatch,
        [json.dumps(_make_event(1, "PushEvent")), json.dumps(_make_event(2, "PushEvent"))],
    )
    second = ingest_hour(dt, force=True)

    assert second.status == "success"
    assert second.rows_kept == 2  # replaced, not appended to the first run's 1 row
