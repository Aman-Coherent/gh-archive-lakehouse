{{
    config(
        materialized='external',
        location=var('silver_root') ~ '/stg_actors.parquet'
    )
}}

-- One row per GitHub actor (user), with the most recently observed login.
-- WHY Type 1 (overwrite), not Type 2 like dim_repo: a username change isn't
-- analytically interesting the way a repo rename is — nobody is asking "what
-- was this user's login last month," so there's no history worth keeping
-- here. See ADR for dim_actor in Phase 5 for the full reasoning.

with ranked as (
    select
        actor_id,
        actor_login,
        row_number() over (
            partition by actor_id
            order by created_at desc
        ) as rn
    from {{ ref('stg_events') }}
)

select
    actor_id,
    actor_login
from ranked
where rn = 1
