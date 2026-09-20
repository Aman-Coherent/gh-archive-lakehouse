{{
    config(
        materialized='external',
        location=var('gold_root') ~ '/mart_daily_repo_activity.parquet'
    )
}}

-- Grain: one row per (repo_id, date_key, category). WHY per-category rows
-- instead of pivoted category columns: adding a 4th event category later
-- (there are only 3 today: code, collaboration, social) means adding rows,
-- not a schema migration — and it's what dbt tests naturally expect.
--
-- WHY "push_event_count" instead of "commits": the spec's original design
-- asked for a commit count here too — same ADR-001 limitation as
-- fact_events. PushEvent count is the closest available proxy for "how much
-- code-pushing activity happened," not a like-for-like replacement.

select
    fact_events.repo_id,
    fact_events.date_key,
    dim_date.date_day,
    dim_event_type.category,
    count(*) as event_count,
    count(distinct fact_events.actor_id) as distinct_contributors,
    count(*) filter (where fact_events.event_type = 'PushEvent') as push_event_count
from {{ ref('fact_events') }} as fact_events
inner join {{ ref('dim_date') }} as dim_date
    on fact_events.date_key = dim_date.date_key
inner join {{ ref('dim_event_type') }} as dim_event_type
    on fact_events.event_type_key = dim_event_type.event_type_key
where fact_events.repo_id is not null
group by 1, 2, 3, 4
