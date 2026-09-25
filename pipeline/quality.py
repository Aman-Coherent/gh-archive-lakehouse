"""Data quality and freshness checks.

Answers a different question than "did the job succeed" (pipeline/runlog.py,
Phase 8): "is the data good enough to trust right now." dbt tests (in
transform/) answer *correctness* — is this data internally consistent. This
module answers *freshness* — is this data recent enough to be useful at all,
independent of whether it's correct.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import duckdb

from pipeline.config import Settings
from pipeline.gold import connect, gold_path


@dataclass(frozen=True)
class FreshnessResult:
    """Outcome of one check_freshness() call."""

    is_fresh: bool
    latest_event_at: datetime | None
    age_hours: float | None
    sla_hours: int


def check_freshness() -> FreshnessResult:
    """Compare the newest event timestamp in gold against now.

    "Fresh" means the newest row in fact_events is no older than
    FRESHNESS_SLA_HOURS. A missing or empty gold table is treated as stale,
    not as an error — there's nothing to be fresh about yet.
    """
    settings = Settings.load()
    con = connect(settings)
    path = gold_path("fact_events.parquet", settings)

    try:
        row = con.execute(f"select max(created_at) from read_parquet('{path}')").fetchone()
        latest = row[0] if row else None
    except duckdb.Error:
        latest = None

    if latest is None:
        return FreshnessResult(
            is_fresh=False,
            latest_event_at=None,
            age_hours=None,
            sla_hours=settings.freshness_sla_hours,
        )

    now = datetime.now(UTC).replace(tzinfo=None)
    age_hours = (now - latest).total_seconds() / 3600
    return FreshnessResult(
        is_fresh=age_hours <= settings.freshness_sla_hours,
        latest_event_at=latest,
        age_hours=age_hours,
        sla_hours=settings.freshness_sla_hours,
    )
