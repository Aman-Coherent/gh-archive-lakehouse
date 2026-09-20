{{
    config(
        materialized='external',
        location=var('gold_root') ~ '/mart_hourly_volume.parquet'
    )
}}

-- Grain: one row per (date_key, event_hour). Feeds the ops/status page
-- (Phase 9) — deliberately not broken down by repo or event type, since its
-- job is a single "is the pipeline's volume normal" timeseries, not analysis.

select
    date_key,
    extract(hour from created_at) as event_hour,
    count(*) as event_count
from {{ ref('fact_events') }}
group by 1, 2
