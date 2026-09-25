{{
    config(
        materialized='external',
        location=var('gold_root') ~ '/mart_pipeline_health.parquet'
    )
}}

-- Grain: exactly one row — the pipeline's current health snapshot as of
-- this dbt run. Feeds the Phase 9 status page directly, so "is the
-- pipeline healthy" is answerable from one row instead of assembling
-- several separate queries by hand.

-- WHY at time zone 'UTC', not bare current_timestamp: DuckDB's
-- current_timestamp reflects the session's local system timezone, not UTC
-- (confirmed directly — on this machine it's IST, +5:30). Every stored
-- timestamp in this project (started_at, created_at, ...) is naive but
-- semantically UTC; comparing them against a timezone-aware current_timestamp
-- silently mixes the two and skews every duration in this model by the
-- machine's UTC offset. `AT TIME ZONE 'UTC'` converts to a plain, comparable
-- UTC timestamp — verified directly against `date -u` before relying on it.
with now_utc as (
    select current_timestamp at time zone 'UTC' as now_utc
),

runs as (
    select * from {{ ref('int_pipeline_runs') }}
),

ordered as (
    select
        status,
        duration_s,
        started_at,
        row_number() over (order by started_at desc) as rn
    from runs
),

-- WHY this specific approach for "consecutive failures", not a simple count
-- of recent failures: what matters for "should someone be paged right now"
-- is an unbroken failure streak ending at the *most recent* run, not a
-- total failure count over some window — three failures scattered across
-- a week with successes in between is a very different signal from three
-- failures in a row just now.
first_non_failure_rn as (
    select min(rn) as rn from ordered where status != 'failed'
),

consecutive as (
    select
        coalesce(
            (select rn - 1 from first_non_failure_rn),
            (select count(*) from ordered)
        ) as consecutive_failures
),

health as (
    select
        count(*) filter (where ordered.started_at >= now_utc.now_utc - interval 24 hour)
            as runs_24h,
        count(*) filter (
            where ordered.started_at >= now_utc.now_utc - interval 24 hour
            and ordered.status = 'success'
        ) as successes_24h,
        count(*) filter (where ordered.started_at >= now_utc.now_utc - interval 7 day)
            as runs_7d,
        count(*) filter (
            where ordered.started_at >= now_utc.now_utc - interval 7 day
            and ordered.status = 'success'
        ) as successes_7d,
        quantile_cont(ordered.duration_s, 0.5) as p50_duration_s,
        quantile_cont(ordered.duration_s, 0.95) as p95_duration_s
    from ordered, now_utc
),

-- WHY current data age lives here too, not just in pipeline.quality's
-- check_freshness(): that Python check exists for the CLI's exit-code
-- contract (Phase 6); this gives the same number to anything reading gold
-- directly, like the Phase 9 dashboard, without re-deriving it.
latest_gold_event as (
    select max(created_at) as latest_event_at from {{ ref('fact_events') }}
)

select
    now_utc.now_utc as computed_at,
    health.runs_24h,
    health.successes_24h,
    case when health.runs_24h = 0 then null
         else health.successes_24h::double / health.runs_24h end as success_rate_24h,
    health.runs_7d,
    health.successes_7d,
    case when health.runs_7d = 0 then null
         else health.successes_7d::double / health.runs_7d end as success_rate_7d,
    health.p50_duration_s,
    health.p95_duration_s,
    consecutive.consecutive_failures,
    latest_gold_event.latest_event_at,
    date_diff('second', latest_gold_event.latest_event_at, now_utc.now_utc) / 3600.0
        as data_age_hours
from health, consecutive, latest_gold_event, now_utc
