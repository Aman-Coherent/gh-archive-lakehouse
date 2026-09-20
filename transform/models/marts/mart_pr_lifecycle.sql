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

select
    repo_id,
    pr_number,
    opened_at,
    merged_at,
    hours_to_merge,
    -- WHY quantile_cont, not standard SQL's percentile_cont() WITHIN GROUP:
    -- DuckDB doesn't implement the WITHIN GROUP syntax — quantile_cont is its
    -- own equivalent, confirmed directly against a running DuckDB instance.
    quantile_cont(hours_to_merge, 0.5) over (partition by repo_id) as repo_p50_hours_to_merge,
    quantile_cont(hours_to_merge, 0.95) over (partition by repo_id) as repo_p95_hours_to_merge
from {{ ref('int_pr_lifecycle') }}
