{% snapshot dim_repo_snapshot %}

{{
    config(
        target_schema='main',
        unique_key='repo_id',
        strategy='check',
        check_cols=['repo_name', 'repo_owner'],
    )
}}

-- WHY select the latest row per repo_id, not all of stg_repos' daily history:
-- a dbt snapshot's job is to compare "current state" against what it saw
-- last run and record a change when one occurred — feeding it stg_repos'
-- full daily grain would make every single day look like a "change" even
-- when the name never moved. This gives dbt exactly one current-state row
-- per repo to compare each time `dbt snapshot` runs (Phase 7's hourly cron).
select
    repo_id,
    repo_name,
    repo_owner,
    repo_short_name
from {{ ref('stg_repos') }}
qualify row_number() over (partition by repo_id order by event_date desc) = 1

{% endsnapshot %}
