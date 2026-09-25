"""Parses dbt's own run_results.json after a `dbt build`/`dbt test` and
records a summary to gold/ops/dbt_test_results/, so the Phase 9 dashboard
can show a real dbt test pass rate without re-running dbt itself.

WHY read dbt's own artifact instead of re-deriving pass/fail some other way:
dbt already computes exactly this information as part of every run and
writes it to target/run_results.json — parsing the file dbt already
produced is more honest and less error-prone than recomputing test outcomes
independently and hoping the two never disagree.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, datetime

import pyarrow as pa

from pipeline.config import Settings
from pipeline.storage import Storage

DBT_RESULTS_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("recorded_at", pa.timestamp("us"), nullable=False),
        pa.field("total_tests", pa.int64(), nullable=False),
        pa.field("passed_tests", pa.int64(), nullable=False),
        pa.field("warned_tests", pa.int64(), nullable=False),
        pa.field("failed_tests", pa.int64(), nullable=False),
    ]
)


def record_dbt_results(run_results_path: str) -> dict[str, int] | None:
    """Parse a dbt run_results.json file and append one summary row to gold.

    Returns the parsed counts, or None if no file exists yet — WHY that's
    not an error: a dbt run that fails to even parse never produces
    run_results.json at all, and this shouldn't crash the workflow over a
    missing observability artifact when the real failure is already being
    reported elsewhere.
    """
    if not os.path.exists(run_results_path):
        return None

    with open(run_results_path, encoding="utf-8") as f:
        run_results = json.load(f)

    test_statuses = [
        r["status"] for r in run_results["results"] if r["unique_id"].startswith("test.")
    ]
    counts = {
        "total_tests": len(test_statuses),
        "passed_tests": test_statuses.count("pass"),
        "warned_tests": test_statuses.count("warn"),
        "failed_tests": test_statuses.count("fail") + test_statuses.count("error"),
    }

    settings = Settings.load()
    storage = Storage(settings)
    row = {
        "run_id": uuid.uuid4().hex,
        "recorded_at": datetime.now(UTC).replace(tzinfo=None),
        **counts,
    }
    table = pa.Table.from_pylist([row], schema=DBT_RESULTS_SCHEMA)
    storage.write_parquet_atomic(table, f"gold/ops/dbt_test_results/{row['run_id']}.parquet")
    return counts
