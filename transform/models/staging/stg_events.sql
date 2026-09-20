{{
    config(
        materialized='external',
        location=var('silver_root') ~ '/stg_events.parquet'
    )
}}

-- One row per deduplicated GitHub event. Grain: one row per event_id.
--
-- WHY dedupe here, not in bronze: bronze is an append-only, atomic-overwrite
-- record of exactly what was downloaded — it should never be "fixed" after
-- the fact, or we lose the ability to answer "what did the raw feed actually
-- say" during an investigation. Silver is where "one row per event" becomes
-- a real guarantee. Duplicates can come from a force=true re-ingest, or (a
-- real case found in Phase 3) GH Archive's own hourly file occasionally
-- containing the same event twice.
--
-- WHY a full rebuild every run, not incremental: bronze itself only keeps
-- BRONZE_RETENTION_DAYS of history before retention deletes it (Phase 7) —
-- an incremental model would need extra logic just to purge silver rows
-- whose bronze source is already gone. A full rebuild is naturally always
-- consistent with whatever bronze currently contains, and bronze is small
-- enough (~0.5 GiB at 7-day retention, measured in ADR-001) that rebuilding
-- is cheap.

with bronze as (
    select *
    from read_parquet('{{ var("bronze_root") }}/events/**/*.parquet')
),

deduped as (
    select
        *,
        row_number() over (
            partition by event_id
            order by ingested_at desc
        ) as rn
    from bronze
)

select
    event_id,
    event_type,
    created_at,
    actor_id,
    trim(actor_login) as actor_login,
    repo_id,
    trim(lower(repo_name)) as repo_name,
    split_part(trim(lower(repo_name)), '/', 1) as repo_owner,
    split_part(trim(lower(repo_name)), '/', 2) as repo_short_name,
    org_id,
    action,
    pr_number,
    issue_number,
    cast(created_at as date) as event_date,
    extract(hour from created_at) as event_hour,
    ingested_at,
    source_file
from deduped
where rn = 1
