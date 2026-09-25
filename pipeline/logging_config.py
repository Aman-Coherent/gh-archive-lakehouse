"""Structured JSON logging, used throughout the pipeline.

WHY JSON, not plain text: log lines land in GitHub Actions' log viewer today
and potentially a log aggregator later — JSON means every line is reliably
machine-parseable (e.g. filter for "level": "ERROR") instead of depending on
fragile text-pattern matching that breaks the moment a message's wording changes.

WHY run_id on every line: a single hourly run touches several log lines
across download, parse, and write. Without a shared run_id, correlating
"which lines belong to the run that failed" means guessing from timestamps.
"""

from __future__ import annotations

import json
import logging
import sys


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "run_id": getattr(record, "run_id", None),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def configure_logging() -> None:
    """Configure root logging to emit one JSON object per line to stdout."""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(logging.INFO)


def get_logger(name: str, *, run_id: str) -> logging.LoggerAdapter:
    """Return a logger that automatically attaches run_id to every line."""
    return logging.LoggerAdapter(logging.getLogger(name), {"run_id": run_id})
