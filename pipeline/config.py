"""Environment-driven configuration for the pipeline.

Every setting is read from an environment variable (optionally loaded from a
local .env file) rather than hardcoded, so the exact same code runs
unmodified on a laptop, in CI, and in production — only the environment
differs. See .env.example for the full variable contract.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

_VALID_STORAGE_BACKENDS = ("local", "r2")

# WHY: R2 credentials are only required once you actually choose the "r2"
# backend. Requiring them unconditionally would force every local contributor
# to have a Cloudflare account before they could run `Settings.load()`.
_R2_REQUIRED_VARS = (
    "R2_ACCOUNT_ID",
    "R2_ACCESS_KEY_ID",
    "R2_SECRET_ACCESS_KEY",
    "R2_BUCKET",
)


@dataclass(frozen=True)
class Settings:
    """Resolved pipeline configuration for one run.

    Frozen (immutable) so a run's config can't drift mid-execution — WHY:
    a config object that can be mutated after load() invites a subtle class
    of bug where one part of the pipeline reads a different value than
    another part expected, and it becomes read-only for free with dataclasses.
    """

    storage_backend: str
    local_data_root: str
    r2_account_id: str | None
    r2_access_key_id: str | None
    r2_secret_access_key: str | None = field(repr=False)
    r2_bucket: str | None
    bronze_retention_days: int
    event_types: tuple[str, ...]
    freshness_sla_hours: int
    alert_email: str | None

    def __repr__(self) -> str:
        # WHY: the acceptance check for this phase prints Settings directly,
        # and in CI that output lands in a public Actions log. A default
        # dataclass repr would print r2_access_key_id and r2_bucket in the
        # clear; redact anything credential-shaped so this is safe to log
        # even before secret-scanning kicks in.
        access_key = "***set***" if self.r2_access_key_id else None
        secret_key = "***redacted***" if self.r2_secret_access_key else None
        return (
            f"Settings(storage_backend={self.storage_backend!r}, "
            f"local_data_root={self.local_data_root!r}, "
            f"r2_account_id={self.r2_account_id!r}, "
            f"r2_access_key_id={access_key!r}, "
            f"r2_secret_access_key={secret_key!r}, "
            f"r2_bucket={self.r2_bucket!r}, "
            f"bronze_retention_days={self.bronze_retention_days}, "
            f"event_types={self.event_types}, "
            f"freshness_sla_hours={self.freshness_sla_hours}, "
            f"alert_email={self.alert_email!r})"
        )

    @classmethod
    def load(cls) -> Settings:
        """Read and validate settings from the environment.

        Loads a local .env file if present (no-op in CI, where real
        environment variables are injected via GitHub Actions secrets).
        """
        load_dotenv()

        storage_backend = os.getenv("STORAGE_BACKEND", "local")
        if storage_backend not in _VALID_STORAGE_BACKENDS:
            raise ValueError(
                f"STORAGE_BACKEND must be one of {_VALID_STORAGE_BACKENDS}, "
                f"got {storage_backend!r}"
            )

        r2_account_id = os.getenv("R2_ACCOUNT_ID") or None
        r2_access_key_id = os.getenv("R2_ACCESS_KEY_ID") or None
        r2_secret_access_key = os.getenv("R2_SECRET_ACCESS_KEY") or None
        r2_bucket = os.getenv("R2_BUCKET") or None

        if storage_backend == "r2":
            # WHY: fail fast at config-load time with a clear message,
            # instead of failing deep inside a storage call with a cryptic
            # "NoneType has no attribute" once ingest is already running.
            missing = [
                name
                for name, value in zip(
                    _R2_REQUIRED_VARS,
                    (r2_account_id, r2_access_key_id, r2_secret_access_key, r2_bucket),
                    strict=True,
                )
                if not value
            ]
            if missing:
                raise ValueError(
                    "STORAGE_BACKEND=r2 requires the following environment "
                    f"variables to be set: {', '.join(missing)}"
                )

        bronze_retention_days = _positive_int("BRONZE_RETENTION_DAYS", default=7)
        freshness_sla_hours = _positive_int("FRESHNESS_SLA_HOURS", default=3)

        raw_event_types = os.getenv(
            "EVENT_TYPES",
            "PushEvent,PullRequestEvent,IssuesEvent,IssueCommentEvent,WatchEvent,ForkEvent",
        )
        event_types = tuple(t.strip() for t in raw_event_types.split(",") if t.strip())

        return cls(
            storage_backend=storage_backend,
            local_data_root=os.getenv("LOCAL_DATA_ROOT", "./data"),
            r2_account_id=r2_account_id,
            r2_access_key_id=r2_access_key_id,
            r2_secret_access_key=r2_secret_access_key,
            r2_bucket=r2_bucket,
            bronze_retention_days=bronze_retention_days,
            event_types=event_types,
            freshness_sla_hours=freshness_sla_hours,
            alert_email=os.getenv("ALERT_EMAIL") or None,
        )


def _positive_int(env_var: str, *, default: int) -> int:
    """Parse an environment variable as a positive int, or raise a clear error."""
    raw = os.getenv(env_var)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{env_var} must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{env_var} must be positive, got {value}")
    return value
