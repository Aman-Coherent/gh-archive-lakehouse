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


@dataclass(frozen=True)
class FreshnessResult:
    """Outcome of one check_freshness() call."""

    is_fresh: bool
    latest_event_at: datetime | None
    age_hours: float | None
    sla_hours: int


def _gold_fact_events_path(settings: Settings) -> str:
    # WHY this must match dbt's gold_root default exactly: this reads the
    # same physical fact_events.parquet dbt just wrote — a path that drifts
    # from dbt_project.yml's `gold_root` var would silently check the wrong
    # (or no) data instead of failing loudly.
    if settings.storage_backend == "local":
        return f"{settings.local_data_root}/gold/fact_events.parquet"
    return f"s3://{settings.r2_bucket}/gold/fact_events.parquet"


def _connect(settings: Settings) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    if settings.storage_backend == "r2":
        # WHY duplicated from transform/profiles.yml's prod target rather
        # than shared: this is a plain DuckDB connection outside of dbt, so
        # dbt's own profile config isn't reachable from here.
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        con.execute("SET s3_region='auto'")
        con.execute(f"SET s3_endpoint='{settings.r2_account_id}.r2.cloudflarestorage.com'")
        con.execute(f"SET s3_access_key_id='{settings.r2_access_key_id}'")
        con.execute(f"SET s3_secret_access_key='{settings.r2_secret_access_key}'")
        con.execute("SET s3_url_style='path'")
    return con


def check_freshness() -> FreshnessResult:
    """Compare the newest event timestamp in gold against now.

    "Fresh" means the newest row in fact_events is no older than
    FRESHNESS_SLA_HOURS. A missing or empty gold table is treated as stale,
    not as an error — there's nothing to be fresh about yet.
    """
    settings = Settings.load()
    con = _connect(settings)
    path = _gold_fact_events_path(settings)

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
