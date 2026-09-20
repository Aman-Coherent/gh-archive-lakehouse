"""Retention enforcement: deletes bronze partitions older than BRONZE_RETENTION_DAYS.

WHY silver needs no special handling here: silver (transform/models/staging/)
is fully rebuilt from whatever bronze currently contains, on every dbt run
(Phase 4) — once this deletes an old bronze partition, the next `dbt run`
naturally produces silver without that data. No separate deletion step
needed, and none exists.

WHY gold is safe from this module structurally, not just by convention: this
file only ever lists/deletes paths under "bronze/events/" — there is no code
path here capable of touching "gold/" at all. Gold's own durability (surviving
this deletion) comes from fact_events being incremental, not from this module
being careful.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

from pipeline.config import Settings
from pipeline.storage import Storage

_PARTITION_DATE_RE = re.compile(r"event_date=(\d{4}-\d{2}-\d{2})")


@dataclass(frozen=True)
class RetentionResult:
    """Outcome of one enforce_retention() call."""

    cutoff_date: date
    dates_deleted: int
    files_deleted: int
    bytes_reclaimed: int


def enforce_retention() -> RetentionResult:
    """Delete bronze partitions whose event_date is older than BRONZE_RETENTION_DAYS."""
    settings = Settings.load()
    storage = Storage(settings)

    cutoff = (
        datetime.now(UTC).replace(tzinfo=None) - timedelta(days=settings.bronze_retention_days)
    ).date()

    dates_to_delete: set[str] = set()
    for path in storage.list_partitions("bronze/events"):
        match = _PARTITION_DATE_RE.search(path)
        if match and date.fromisoformat(match.group(1)) < cutoff:
            dates_to_delete.add(match.group(1))

    bytes_reclaimed = 0
    files_deleted = 0
    for event_date in sorted(dates_to_delete):
        prefix = f"bronze/events/event_date={event_date}"
        # WHY size() before delete_prefix(): once a file is deleted there's
        # nothing left to measure — bytes have to be tallied first.
        bytes_reclaimed += sum(storage.size(p) for p in storage.list_partitions(prefix))
        files_deleted += storage.delete_prefix(prefix)

    return RetentionResult(
        cutoff_date=cutoff,
        dates_deleted=len(dates_to_delete),
        files_deleted=files_deleted,
        bytes_reclaimed=bytes_reclaimed,
    )
