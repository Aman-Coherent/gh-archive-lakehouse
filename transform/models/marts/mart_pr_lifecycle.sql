{{
    config(
        materialized='external',
        location=var('gold_root') ~ '/mart_pr_lifecycle.parquet'
    )
}}

-- Grain: one row per merged PR. Repo-level p50/p95 are attached as window
-- functions on the same row rather than a separate aggregate table, so
-- "time to merge for this PR" and "how does that compare to its repo's
-- usual pace" are answerable from one table without a second query.
--
-- WHY this reads fact_events, not int_pr_lifecycle directly (changed in
-- Phase 7): fact_events is incremental and durably accumulates forever;
-- int_pr_lifecycle is a full rebuild from stg_events' current 7-day
-- bronze/silver window, so a PR merged today but opened 10 days ago would
-- already be invisible to it once that "opened" event ages out of bronze.
-- Once fact_events captures a merge event's hours_to_merge, it survives
-- retention deleting the bronze/silver rows it was originally computed
-- from — this table inherits that durability for free by reading from it
-- instead. int_pr_lifecycle still exists and is still needed: it's what
-- fact_events itself joins against each incremental run to compute
-- hours_to_merge for newly-arriving merge events in the first place.
--
-- WHY this stays a full rebuild rather than also going incremental: it's
-- a small aggregate over fact_events' merge rows (a minority of all
-- events), not a scan of everything — cheap enough that correctness
-- (always reflects fact_events' current, durable state) matters more than
-- optimizing a rebuild that's already fast.

select
    repo_id,
    pr_number,
    created_at - (hours_to_merge * interval '1 hour') as opened_at,
    created_at as merged_at,
    hours_to_merge,
    -- WHY quantile_cont, not standard SQL's percentile_cont() WITHIN GROUP:
    -- DuckDB doesn't implement the WITHIN GROUP syntax — quantile_cont is its
    -- own equivalent, confirmed directly against a running DuckDB instance.
    quantile_cont(hours_to_merge, 0.5) over (partition by repo_id) as repo_p50_hours_to_merge,
    quantile_cont(hours_to_merge, 0.95) over (partition by repo_id) as repo_p95_hours_to_merge
from {{ ref('fact_events') }}
where is_pr_merged and hours_to_merge is not null
