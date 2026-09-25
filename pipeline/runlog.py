"""Pipeline run log: one row per ingest attempt, appended to gold/ops/pipeline_runs/.

WHY this is its own dataset in gold, not just GitHub Actions' run history:
"is the pipeline healthy" should be answerable with a SQL query against
structured history (see mart_pipeline_health), not by reading through CI run
history by hand, one run at a time.

WHY this takes plain fields instead of an IngestResult object: pipeline.ingest
already needs to call this module to log a run — importing IngestResult back
here would create a circular import for no real benefit, since the caller
already has the individual fields on hand.

WHY one small Parquet file per run instead of one shared incremental table:
Phase 7 found how easy it is to get dbt-duckdb's incremental+external
materialization subtly wrong (fact_events silently never exporting its
physical file on non-full-refresh runs). One file per run mirrors bronze's
already-proven atomic partition-per-unit design instead of repeating that risk.
"""

from __future__ import annotations

import uuid
from datetime import datetime

import pyarrow as pa

from pipeline.config import Settings
from pipeline.storage import Storage

RUNLOG_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("started_at", pa.timestamp("us"), nullable=False),
        pa.field("finished_at", pa.timestamp("us"), nullable=False),
        pa.field("duration_s", pa.float64(), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("hour_processed", pa.timestamp("us"), nullable=False),
        pa.field("rows_read", pa.int64(), nullable=False),
        pa.field("rows_written", pa.int64(), nullable=False),
        pa.field("malformed_rows", pa.int64(), nullable=False),
        pa.field("bytes_written", pa.int64(), nullable=False),
        pa.field("error_message", pa.string(), nullable=True),
    ]
)


def new_run_id() -> str:
    return uuid.uuid4().hex


def record_run(
    *,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    hour_processed: datetime,
    rows_read: int,
    rows_written: int,
    malformed_rows: int,
    bytes_written: int,
    error_message: str | None,
) -> None:
    """Append one row describing an ingest_hour() outcome to the run log."""
    settings = Settings.load()
    storage = Storage(settings)

    row = {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_s": (finished_at - started_at).total_seconds(),
        "status": status,
        "hour_processed": hour_processed,
        "rows_read": rows_read,
        "rows_written": rows_written,
        "malformed_rows": malformed_rows,
        "bytes_written": bytes_written,
        "error_message": error_message,
    }
    table = pa.Table.from_pylist([row], schema=RUNLOG_SCHEMA)
    storage.write_parquet_atomic(table, f"gold/ops/pipeline_runs/{run_id}.parquet")
