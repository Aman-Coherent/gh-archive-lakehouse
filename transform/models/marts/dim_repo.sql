{{
    config(
        materialized='external',
        location=var('gold_root') ~ '/dim_repo.parquet'
    )
}}

-- SCD Type 2: one row per repo per distinct (repo_name, repo_owner) version
-- it has ever had. repo_key is per-version (not per-repo), which is exactly
-- why it — not repo_id — is the primary key here and the join target from
-- fact_events: repo_id alone can't distinguish which version of a renamed
-- repo an old event belongs to.
select
    {{ dbt_utils.generate_surrogate_key(['repo_id', 'dbt_valid_from']) }} as repo_key,
    repo_id,
    repo_name,
    repo_owner,
    repo_short_name,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to is null as is_current
from {{ ref('dim_repo_snapshot') }}
