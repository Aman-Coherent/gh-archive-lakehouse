{{
    config(
        materialized='external',
        location=var('silver_root') ~ '/stg_repos.parquet'
    )
}}

-- One row per repo per day, with the name observed most recently that day.
-- WHY per-day, not one row per repo overall: this feeds dim_repo's SCD2
-- snapshot in Phase 5, which needs to detect a rename between one day and
-- the next — collapsing straight to one row per repo would throw away the
-- exact information SCD2 exists to preserve.

with ranked as (
    select
        repo_id,
        repo_name,
        repo_owner,
        repo_short_name,
        event_date,
        row_number() over (
            partition by repo_id, event_date
            order by created_at desc
        ) as rn
    from {{ ref('stg_events') }}
    where repo_id is not null
)

select
    repo_id,
    repo_name,
    repo_owner,
    repo_short_name,
    event_date
from ranked
where rn = 1
