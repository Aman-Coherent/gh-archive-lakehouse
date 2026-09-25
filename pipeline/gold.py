"""Shared helpers for reading gold data directly, bypassing dbt.

WHY this exists as its own module: pipeline.quality's freshness check and the
Phase 9 dashboard both need the exact same two things — the physical path to
a gold file, and a DuckDB connection configured for R2 when that's the active
backend. Duplicating that logic a second time risks the two copies quietly
drifting apart, which is exactly the class of bug Phase 7 found the hard way
(fact_events silently never exporting its physical file).
"""

from __future__ import annotations

import duckdb

from pipeline.config import Settings


def gold_path(filename: str, settings: Settings | None = None) -> str:
    """Physical path to a gold Parquet file (or glob), matching dbt's
    gold_root default exactly — a path that drifts from dbt_project.yml's
    `gold_root` var would silently read stale or missing data instead of
    failing loudly."""
    settings = settings or Settings.load()
    if settings.storage_backend == "local":
        return f"{settings.local_data_root}/gold/{filename}"
    return f"s3://{settings.r2_bucket}/gold/{filename}"


def connect(settings: Settings | None = None) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection ready to read gold Parquet, local or R2."""
    settings = settings or Settings.load()
    con = duckdb.connect()
    if settings.storage_backend == "r2":
        # WHY duplicated from transform/profiles.yml's prod target rather
        # than shared with dbt: this is a plain DuckDB connection outside of
        # dbt, so dbt's own profile config isn't reachable from here.
        con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        con.execute("SET s3_region='auto'")
        con.execute(f"SET s3_endpoint='{settings.r2_account_id}.r2.cloudflarestorage.com'")
        con.execute(f"SET s3_access_key_id='{settings.r2_access_key_id}'")
        con.execute(f"SET s3_secret_access_key='{settings.r2_secret_access_key}'")
        con.execute("SET s3_url_style='path'")
    return con
