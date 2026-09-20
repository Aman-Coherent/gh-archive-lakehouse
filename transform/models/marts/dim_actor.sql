{{
    config(
        materialized='external',
        location=var('gold_root') ~ '/dim_actor.parquet'
    )
}}

-- SCD Type 1 (overwrite, no history). WHY this dimension doesn't need Type
-- 2 the way dim_repo does: a repo rename changes what a historical report
-- should call something, so getting it wrong retroactively rewrites the
-- past. A username change isn't analytically load-bearing the same way —
-- nobody's asking "what did this contributor's account used to be called
-- last quarter" the way they ask "what was this repo called." Latest login
-- wins, and that's a deliberate simplification, not an oversight.
select
    {{ dbt_utils.generate_surrogate_key(['actor_id']) }} as actor_key,
    actor_id,
    actor_login
from {{ ref('stg_actors') }}
