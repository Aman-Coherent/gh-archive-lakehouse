# Architecture

This is the deep version. For the pitch, see the [README](../README.md); for
column-level detail, see the [data dictionary](data_dictionary.md); for why
specific technology choices were made, see [docs/decisions/](decisions/).

## End-to-end data flow

```
GH Archive (hourly .json.gz)
    │  pipeline/ingest.py: stream-download, filter to EVENT_TYPES,
    │  flatten, atomic write (temp-then-move)
    ▼
BRONZE   data/bronze/events/event_date=YYYY-MM-DD/hour=HH/part-0.parquet
    │  one partition per hour, idempotent (existing partition = skip),
    │  force=true fully replaces rather than appends
    │
    │  dbt (DuckDB), full rebuild every run
    ▼
SILVER   data/silver/stg_events.parquet, stg_repos.parquet, stg_actors.parquet
    │  dedupes on event_id (row_number() over ingested_at desc),
    │  casts types, splits repo_name, derives event_date/event_hour
    │
    │  dbt: dim_repo (SCD2 snapshot), dim_actor, dim_date, dim_event_type
    │       fact_events (incremental), aggregate marts
    ▼
GOLD     data/gold/*.parquet + data/gold/ops/*/  (retained forever)
    │
    ├──► transform/tests/ + schema tests   → data quality (Phase 6)
    ├──► dashboard/app.py                   → Streamlit, reads only gold (Phase 9)
    └──► pipeline/quality.py, retention.py  → freshness + retention (Phase 6/7)

ORCHESTRATION: .github/workflows/ingest_hourly.yml
  cron 15 * * * * → ingest(T-2h) → dbt build → check-freshness → retention
  concurrency-guarded (group: ingest), alerts on any step's failure
```

## Why bronze is partitioned the way it is

`event_date=YYYY-MM-DD/hour=HH/` (Hive-style, hour zero-padded even though
GH Archive's own URLs aren't — a deliberate choice for lexicographic sort
order; see `pipeline/ingest.py`). One partition = one atomic unit: written
via a temp-path-then-move, so a reader never observes a half-written
partition, and a retry of the same hour either finishes cleanly or is a
true no-op (idempotency, Phase 2).

## Why bronze/silver are ephemeral and gold isn't

Bronze and silver are deleted after `BRONZE_RETENTION_DAYS` (default 7).
Silver needs no separate deletion logic — it's a full rebuild from whatever
bronze currently contains, on every dbt run, so it naturally shrinks the
moment bronze does. `pipeline/retention.py` only ever touches paths under
`bronze/events/` — there's no code path in that module capable of reaching
`gold/` at all, which is a *structural* guarantee, not a promise enforced by
convention.

Gold has to survive that deletion, which is why `fact_events` is
**incremental** (`unique_key='event_id'`, watermarked on `ingested_at`, with
a `post_hook` that re-exports the full table to Parquet after every run —
see below for why that hook exists) rather than a full rebuild like
staging. Everything downstream that reads `fact_events` (both aggregate
marts, `mart_pr_lifecycle`) inherits that durability for free.

## The point-in-time join, precisely

`fact_events` joins to `dim_repo` (SCD2) on:

```sql
on events.repo_id = dim_repo.repo_id
and events.created_at >= dim_repo.valid_from
and events.created_at < coalesce(dim_repo.valid_to, timestamp '9999-12-31')
```

The `coalesce()` instead of the more readable `created_at < valid_to OR
valid_to IS NULL` is load-bearing, not stylistic: the `OR` form made
DuckDB's planner fall back to a nested-loop join instead of a hash join on
`repo_id`, measured directly at 34 minutes against this project's real data
(4.6M events × 725K repo-history rows) versus 8.6 seconds after the
rewrite — confirmed with `EXPLAIN` before and after, not assumed from the
SQL alone.

## Known rough edges, documented rather than hidden

- **`fact_events`'s incremental export required a `post_hook`.** dbt-duckdb's
  `incremental` + `external` materialization writes the physical Parquet
  file on a `--full-refresh`, but silently does *not* re-export it on a
  normal incremental run (confirmed directly: every other external model in
  this project produced a real file, only the incremental one didn't). The
  fix is a `post_hook` that re-exports the full table after every run —
  see `fact_events.sql` for the exact mechanism.
- **A plain (non-aggregated) external model whose query returns zero rows
  writes one spurious all-null row to Parquet, not zero rows** — found
  running Phase 9's fresh-clone acceptance test against a single hour of
  real data with no merged PRs in it. `int_pr_lifecycle`'s underlying
  DuckDB relation genuinely had 0 rows (dbt's own `not_null` test against
  it correctly passed), but `mart_pr_lifecycle.parquet` — built directly
  from that same empty result — ended up with exactly one row, every
  column null. The two disagreed, which means a dbt test passing doesn't
  guarantee the exported file it's "testing" is what actually shipped, for
  this specific case. Worked around at the consumer, not the source:
  `dashboard/app.py`'s `load_pr_lifecycle()` drops rows with a null
  `merged_at` before anything downstream sees them.
- **`current_timestamp` in DuckDB reflects the local system timezone**, not
  UTC. Every model comparing against this project's stored (naive, but
  semantically UTC) timestamps uses `current_timestamp AT TIME ZONE 'UTC'`
  explicitly — found by actually running `mart_pipeline_health` on a non-UTC
  machine and getting numbers that were off by exactly the local offset.
- **GH Archive's live feed no longer matches the shape this project's build
  spec originally assumed** — `payload.pull_request.merged/created_at/
  merged_at` and `payload.commits` don't exist anymore (a dated upstream
  change, confirmed by comparing against 2025-06-10 data, which still had
  them). See ADR-001 for the full finding and how `fact_events`/
  `mart_pr_lifecycle` were redesigned around it.

## Observability

`pipeline/runlog.py` appends one small Parquet file per non-skipped ingest
run to `gold/ops/pipeline_runs/` — one file per run, deliberately mirroring
bronze's proven atomic partition-per-unit pattern rather than another
incremental table (given the `fact_events` export issue above, minimizing
new incremental-materialization surface area was a deliberate choice).
`pipeline/dbt_results.py` does the same for dbt's own `run_results.json`
after every build. `mart_pipeline_health` aggregates both into the single
snapshot row the dashboard's status tab reads.

## Orchestration

See ADR-003 for why GitHub Actions' `schedule` trigger, not a dedicated
orchestrator. Three workflows: `ingest_hourly.yml` (the real hourly job),
`backfill.yml` (`workflow_dispatch`, reuses the exact same `backfill()`
function the CLI and hourly job use — no separate "bulk" code path to drift
out of sync), `ci.yml` (`ruff` + `pytest` + `dbt parse`/`compile` on every
PR, catching broken SQL before it reaches the schedule).

`STORAGE_BACKEND` is currently hardcoded to `local` in the workflows — no
Cloudflare R2 account existed at time of writing, so each GitHub Actions
run starts from an empty disk and discards its data at job end. This proves
the orchestration mechanics (schedule, the `T-2h` lag, `dbt build`,
freshness checks, retention) genuinely work — confirmed by 23+ consecutive
unattended successful runs — but real cross-run persistence needs R2 wired
in via the same `pipeline.config.Settings` / `pipeline.storage.Storage`
abstraction already built for it.
