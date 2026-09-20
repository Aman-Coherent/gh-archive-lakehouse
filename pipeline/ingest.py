"""Bronze ingestion: download one GH Archive hour, project + flatten, write one partition.

WHY the bronze schema here differs from the build spec's original table: ADR-001
(docs/decisions/ADR-001-retention-and-projection.md) measured real, current GH
Archive data and found that `payload.pull_request.merged/created_at/merged_at`
and `payload.commits` no longer exist in GitHub's live feed (a dated upstream
change, confirmed against older data). `pr_merged`, `pr_created_at`,
`pr_merged_at`, and `commit_count` are dropped rather than shipped as columns
that could never hold data. `action` and `pr_number` are kept — `action` now
carries PR-merge status directly (a PullRequestEvent with action="merged" is a
merge), and later phases can derive merge duration from paired opened/merged
event timestamps instead of an embedded field.
"""

from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

import pyarrow as pa
import requests

from pipeline.config import Settings
from pipeline.storage import Storage

# WHY explicit schema (not inferred): inference over ~50k dicts with sparse
# fields is slower and, worse, silently guesses a type from whatever values
# happen to be present in this particular hour — a column that's all-null in
# one hour could infer as the wrong type entirely. An explicit schema is also
# the single source of truth for what "bronze" contractually contains.
BRONZE_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.int64(), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("created_at", pa.timestamp("us"), nullable=False),
        pa.field("actor_id", pa.int64(), nullable=False),
        pa.field("actor_login", pa.string(), nullable=False),
        # WHY nullable: Phase 1 measured 2 events out of 61,147 in a real hour
        # missing repo.id/repo.name entirely — rare, but real, so the schema
        # has to allow it rather than crash ingest on those rows.
        pa.field("repo_id", pa.int64(), nullable=True),
        pa.field("repo_name", pa.string(), nullable=True),
        pa.field("org_id", pa.int64(), nullable=True),
        pa.field("action", pa.string(), nullable=True),
        pa.field("pr_number", pa.int32(), nullable=True),
        pa.field("issue_number", pa.int32(), nullable=True),
        pa.field("ingested_at", pa.timestamp("us"), nullable=False),
        pa.field("source_file", pa.string(), nullable=False),
    ]
)

_GHARCHIVE_BASE_URL = "https://data.gharchive.org"


@dataclass(frozen=True)
class IngestResult:
    """Outcome of one ingest_hour() call — always returned, never raised, for a
    graceful-failure caller (backfill, cron) to inspect without a try/except."""

    hour: datetime
    status: Literal["success", "skipped", "failed"]
    rows_read: int = 0
    rows_kept: int = 0
    malformed_rows: int = 0
    bytes_written: int = 0
    error_message: str | None = None


def partition_path(dt: datetime) -> str:
    """Bronze partition path for one hour, relative to the storage root.

    WHY zero-padded hour here even though GH Archive's own URLs aren't
    (2024-01-15-5.json.gz, not -05): this is our own partition layout, chosen
    for lexicographic sort order (hour=05 sorts before hour=12 as strings;
    hour=5 would not), independent of the source's naming convention.
    """
    return f"bronze/events/event_date={dt:%Y-%m-%d}/hour={dt.hour:02d}/part-0.parquet"


def _source_url(dt: datetime) -> str:
    # WHY no zero-padding here: verified directly against data.gharchive.org —
    # hour 5 is served as "...-5.json.gz", not "...-05.json.gz". A zero-padded
    # guess 404s for every single-digit hour, silently losing 10 of 24 hours/day.
    return f"{_GHARCHIVE_BASE_URL}/{dt:%Y-%m-%d}-{dt.hour}.json.gz"


def _parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str):
        raise TypeError(f"expected str timestamp, got {type(value).__name__}")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(UTC).replace(tzinfo=None)
    return parsed


def _flatten_event(raw: dict, *, source_file: str, ingested_at: datetime) -> dict:
    """Project + flatten one raw GH Archive event into a bronze row.

    Raises KeyError/TypeError/ValueError on any required field that's missing
    or malformed — the caller treats that as one malformed row, not a fatal error.
    """
    actor = raw["actor"]
    repo = raw.get("repo") or {}
    org = raw.get("org") or {}
    payload = raw.get("payload") or {}
    pull_request = payload.get("pull_request") or {}
    issue = payload.get("issue") or {}

    return {
        "event_id": int(raw["id"]),
        "event_type": raw["type"],
        "created_at": _parse_timestamp(raw["created_at"]),
        "actor_id": int(actor["id"]),
        "actor_login": actor["login"],
        "repo_id": repo.get("id"),
        "repo_name": repo.get("name"),
        "org_id": org.get("id"),
        "action": payload.get("action"),
        "pr_number": pull_request.get("number"),
        "issue_number": issue.get("number"),
        "ingested_at": ingested_at,
        "source_file": source_file,
    }


def ingest_hour(dt: datetime, *, force: bool = False) -> IngestResult:
    """Download one GH Archive hour, project + flatten, write one Parquet partition.

    Never raises for expected failure modes (missing upstream file, corrupt
    gzip, network error) — those come back as status="failed" with
    error_message set, so a scheduler can log it and move on to the next hour
    instead of crashing.
    """
    if (dt.minute, dt.second, dt.microsecond) != (0, 0, 0):
        raise ValueError(f"ingest_hour requires an exact hour boundary, got {dt!r}")

    settings = Settings.load()
    storage = Storage(settings)
    path = partition_path(dt)

    if storage.exists(path) and not force:
        return IngestResult(hour=dt, status="skipped")

    source_url = _source_url(dt)
    try:
        response = requests.get(source_url, stream=True, timeout=60)
        if response.status_code == 404:
            return IngestResult(
                hour=dt,
                status="failed",
                error_message=f"upstream file not published (404): {source_url}",
            )
        response.raise_for_status()

        event_types = set(settings.event_types)
        ingested_at = datetime.now(UTC).replace(tzinfo=None)
        rows_read = 0
        malformed_rows = 0
        records: list[dict] = []

        # WHY gzip.GzipFile over response.raw, not response.iter_lines(): this
        # decompresses on the fly as bytes arrive, so the full ~100MB
        # uncompressed hour is never buffered in memory at once — only the
        # ~12-column rows we actually keep are.
        with gzip.GzipFile(fileobj=response.raw) as gz_stream:
            for raw_line in gz_stream:
                line = raw_line.strip()
                if not line:
                    continue
                rows_read += 1
                try:
                    raw_event = json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    malformed_rows += 1
                    continue

                if raw_event.get("type") not in event_types:
                    continue

                try:
                    records.append(
                        _flatten_event(raw_event, source_file=source_url, ingested_at=ingested_at)
                    )
                except (KeyError, TypeError, ValueError):
                    malformed_rows += 1
                    continue
    except (requests.RequestException, gzip.BadGzipFile, OSError) as exc:
        return IngestResult(hour=dt, status="failed", error_message=str(exc))

    table = pa.Table.from_pylist(records, schema=BRONZE_SCHEMA)
    bytes_written = storage.write_parquet_atomic(table, path)

    return IngestResult(
        hour=dt,
        status="success",
        rows_read=rows_read,
        rows_kept=len(records),
        malformed_rows=malformed_rows,
        bytes_written=bytes_written,
    )
