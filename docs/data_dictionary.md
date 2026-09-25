# Data dictionary — gold layer

Every column below was extracted directly from dbt's compiled catalog
(`dbt docs generate`) against real data, not hand-typed from memory — column
names and types are exactly what's actually in each Parquet file today.

## Dimensions

### `dim_repo` — SCD Type 2, one row per repository per distinct name version

Grain: one row per `(repo_id, valid_from)`. See ADR-004 for why this
dimension (and not `dim_actor`) is SCD2.

| Column | Type | Description |
|---|---|---|
| `repo_key` | VARCHAR | Surrogate key, per *version* of a repo — not per repo. The join target from `fact_events`. |
| `repo_id` | BIGINT | GitHub's own repository ID. Not unique in this table (one row per version). |
| `repo_name` | VARCHAR | Lowercased, trimmed `owner/name`, as of this version. |
| `repo_owner` | VARCHAR | The `owner` segment of `repo_name`. |
| `repo_short_name` | VARCHAR | The `name` segment of `repo_name`. |
| `valid_from` | TIMESTAMP | When this version of the repo's name became active. |
| `valid_to` | TIMESTAMP | When this version stopped being active (`NULL` = still current). |
| `is_current` | BOOLEAN | `valid_to IS NULL` — convenience flag for "give me the latest name." |

### `dim_actor` — SCD Type 1, one row per GitHub user

| Column | Type | Description |
|---|---|---|
| `actor_key` | VARCHAR | Surrogate key, one per `actor_id`, forever (no history — see ADR-004). |
| `actor_id` | BIGINT | GitHub's own user ID. Unique in this table. |
| `actor_login` | VARCHAR | Most recently observed username. Overwritten on change, not versioned. |

### `dim_date` — generated calendar, 2015-01-01 through 2030-12-31

| Column | Type | Description |
|---|---|---|
| `date_key` | INTEGER | `YYYYMMDD` integer surrogate key. |
| `date_day` | TIMESTAMP | The actual calendar date. |
| `day_of_week` | BIGINT | 0 = Sunday .. 6 = Saturday (DuckDB's convention). |
| `is_weekend` | BOOLEAN | `day_of_week IN (0, 6)`. |
| `iso_week` | BIGINT | ISO week number. |
| `month` / `quarter` / `year` | BIGINT | Calendar parts. |

### `dim_event_type` — static lookup, from `seeds/event_types.csv`

| Column | Type | Description |
|---|---|---|
| `event_type_key` | VARCHAR | Surrogate key, deterministic hash of `event_type`. |
| `event_type` | VARCHAR | GitHub's own event type name (e.g. `PushEvent`). |
| `description` | VARCHAR | Human-readable description. |
| `category` | VARCHAR | One of `code`, `collaboration`, `social`. |

## Fact

### `fact_events` — grain: one row per GitHub event

Incremental (`unique_key='event_id'`, watermarked on `ingested_at`) — see
`fact_events.sql` and ADR-001/Phase 7 for why.

| Column | Type | Description |
|---|---|---|
| `event_id` | BIGINT | Unique GitHub event ID. Primary key of this table. |
| `event_type` | VARCHAR | e.g. `PushEvent`, `PullRequestEvent`. |
| `created_at` | TIMESTAMP | When the event happened, per GitHub. |
| `ingested_at` | TIMESTAMP | When this pipeline processed the row. **Incremental watermark** — do not repurpose for business logic. |
| `repo_key` | VARCHAR | FK to `dim_repo.repo_key`, via a **point-in-time join** (repo_id + timestamp falling inside `valid_from`/`valid_to`). Nullable — an event whose `repo_id` has no matching `dim_repo` row. |
| `actor_key` | VARCHAR | FK to `dim_actor.actor_key`. |
| `date_key` | INTEGER | FK to `dim_date.date_key`, derived from `created_at`. |
| `event_type_key` | VARCHAR | FK to `dim_event_type.event_type_key`. |
| `repo_id`, `actor_id` | BIGINT | Degenerate dimensions — the natural keys, kept alongside the surrogate FKs for convenience. |
| `action` | VARCHAR | `payload.action` from the source event, nullable. For `PullRequestEvent`, `action='merged'` is how this pipeline knows a PR was merged (ADR-001 — the old `payload.pull_request.merged` boolean no longer exists upstream). |
| `pr_number` | INTEGER | Nullable, set only for PR-related events. |
| `issue_number` | INTEGER | Nullable, set only for issue-related events. |
| `is_pr_merged` | BOOLEAN | `true` exactly on the specific event row where a PR merge happened — not "has this PR ever been merged." |
| `hours_to_merge` | DOUBLE | Nullable; populated only on merge-event rows, via `int_pr_lifecycle`'s self-join on paired opened/merged events. |

**Not present**, deliberately: `commit_count`. See ADR-001 — `payload.commits`
no longer exists in GitHub's live feed, with no substitute field.

## Marts

### `mart_daily_repo_activity` — grain: `(repo_id, date_key, category)`

| Column | Type | Description |
|---|---|---|
| `repo_id` | BIGINT | |
| `date_key` | INTEGER | |
| `date_day` | TIMESTAMP | Denormalized from `dim_date` for convenience. |
| `category` | VARCHAR | `code` / `collaboration` / `social`. |
| `event_count` | BIGINT | Total events in this repo/day/category. |
| `distinct_contributors` | BIGINT | Distinct `actor_id` count. |
| `push_event_count` | BIGINT | Count of `PushEvent` specifically — the closest available proxy for "commit activity" now that raw commit counts don't exist upstream (ADR-001). |

### `mart_pr_lifecycle` — grain: one row per merged PR

| Column | Type | Description |
|---|---|---|
| `repo_id`, `pr_number` | BIGINT, INTEGER | Identify the PR. |
| `opened_at`, `merged_at` | TIMESTAMP | Derived from paired event timestamps, not embedded fields (ADR-001). |
| `hours_to_merge` | DOUBLE | `merged_at - opened_at`, in hours. |
| `repo_p50_hours_to_merge` / `repo_p95_hours_to_merge` | DOUBLE | This PR's repo's median/95th-percentile merge time, as of this row — a window function, not a separate aggregate table. |

Only includes PRs whose "opened" event is still within the current
bronze/silver retention window — see `int_pr_lifecycle.sql`.

### `mart_hourly_volume` — grain: `(date_key, event_hour)`

| Column | Type | Description |
|---|---|---|
| `date_key` | INTEGER | |
| `event_hour` | BIGINT | 0-23, UTC. |
| `event_count` | BIGINT | Total events in that hour. |

### `mart_pipeline_health` — grain: exactly one row (current snapshot)

| Column | Type | Description |
|---|---|---|
| `computed_at` | TIMESTAMP | When this snapshot was computed (real UTC — see the timezone note below). |
| `runs_24h` / `successes_24h` / `success_rate_24h` | BIGINT, BIGINT, DOUBLE | Ingest run outcomes in the trailing 24 hours. |
| `runs_7d` / `successes_7d` / `success_rate_7d` | BIGINT, BIGINT, DOUBLE | Same, trailing 7 days. |
| `p50_duration_s` / `p95_duration_s` | DOUBLE | Run duration percentiles. |
| `consecutive_failures` | BIGINT | Unbroken failure streak ending at the *most recent* run — not a total count over a window. |
| `latest_event_at` | TIMESTAMP | Newest `created_at` in `fact_events`. |
| `data_age_hours` | DOUBLE | How stale gold currently is. |

**Note:** every timestamp comparison in this model explicitly uses
`current_timestamp AT TIME ZONE 'UTC'`, not bare `current_timestamp` —
DuckDB's `current_timestamp` reflects the local system timezone, which
silently corrupted this model's numbers during development on a non-UTC
machine before being caught and fixed (Phase 8).

## Operational data (`gold/ops/`)

Not dbt models — written directly by `pipeline/runlog.py` and
`pipeline/dbt_results.py`, one small Parquet file per run, read by
`int_pipeline_runs` and the dashboard.

**`gold/ops/pipeline_runs/{run_id}.parquet`** — one row per non-skipped
`ingest_hour()` call: `run_id`, `started_at`, `finished_at`, `duration_s`,
`status`, `hour_processed`, `rows_read`, `rows_written`, `malformed_rows`,
`bytes_written`, `error_message`.

**`gold/ops/dbt_test_results/{run_id}.parquet`** — one row per `dbt build`
invocation: `run_id`, `recorded_at`, `total_tests`, `passed_tests`,
`warned_tests`, `failed_tests`.
