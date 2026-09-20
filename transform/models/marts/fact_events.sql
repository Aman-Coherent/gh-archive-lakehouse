{{
    config(
        materialized='external',
        location=var('gold_root') ~ '/fact_events.parquet'
    )
}}

-- Grain: one row per GitHub event.
--
-- WHY commit_count isn't here even though the spec's original design
-- included it: ADR-001 (Phase 1) measured that payload.commits no longer
-- exists in GitHub's live feed at all, with no substitute field — it was
-- already dropped at the bronze schema level (Phase 2), so there's nothing
-- to carry through here.
--
-- WHY actor_key/event_type_key are computed by hashing the natural key
-- directly, instead of joining to dim_actor/dim_event_type to fetch them:
-- both are deterministic functions of their natural key (dim_actor is
-- Type 1 — one key per actor_id, ever; dim_event_type is a static lookup),
-- so the hash computed here always equals the hash computed in those
-- models for the same input — no join needed. dim_repo is different (SCD2,
-- so repo_key depends on *when* the event happened, not just repo_id) and
-- genuinely requires the point-in-time join below.
--
-- KNOWN SCOPE LIMIT, to be resolved in Phase 7: this model is a full
-- rebuild from stg_events every run, and stg_events only ever contains
-- whatever's currently in bronze/silver. That's fine today because nothing
-- deletes old bronze partitions yet — but once Phase 7's retention job
-- starts enforcing BRONZE_RETENTION_DAYS, a full rebuild would silently
-- lose gold history older than that window, contradicting "gold is
-- retained forever." Phase 7 needs to make this (and the time-based
-- aggregate marts) incremental — append-only for new events, not a full
-- replace — as part of building retention itself, not as an afterthought.

with events as (
    select * from {{ ref('stg_events') }}
),

pr_lifecycle as (
    select * from {{ ref('int_pr_lifecycle') }}
)

select
    events.event_id,
    events.event_type,
    events.created_at,

    -- foreign keys
    dim_repo.repo_key,
    {{ dbt_utils.generate_surrogate_key(['events.actor_id']) }} as actor_key,
    cast(strftime(events.created_at, '%Y%m%d') as int) as date_key,
    {{ dbt_utils.generate_surrogate_key(['events.event_type']) }} as event_type_key,

    -- degenerate dimensions / context
    events.repo_id,
    events.actor_id,
    events.action,
    events.pr_number,
    events.issue_number,

    -- measures
    (events.event_type = 'PullRequestEvent' and events.action = 'merged') as is_pr_merged,
    pr_lifecycle.hours_to_merge

from events
left join {{ ref('dim_repo') }} as dim_repo
    -- WHY this exact predicate, and why it's called a point-in-time join:
    -- match on repo_id AND on "was this dim_repo row the currently valid
    -- one at the moment the event happened" — not just the latest one.
    -- See the fixture proof in this phase's checkpoint for a worked example.
    --
    -- WHY coalesce(valid_to, far-future sentinel) instead of the more
    -- readable "created_at < valid_to OR valid_to IS NULL": measured
    -- directly — the OR form makes DuckDB's planner fall back to a
    -- BLOCKWISE_NL_JOIN (nested loop) instead of a HASH_JOIN on repo_id,
    -- because it can't extract a clean equi-join key from a non-conjunctive
    -- condition. On this project's real data (4.6M events x 725K repo
    -- versions) that's the difference between 0.03s and 34 minutes — not a
    -- micro-optimization, a hard correctness-of-runtime requirement for an
    -- hourly job.
    on events.repo_id = dim_repo.repo_id
    and events.created_at >= dim_repo.valid_from
    and events.created_at < coalesce(dim_repo.valid_to, timestamp '9999-12-31')
left join pr_lifecycle
    on events.event_id = pr_lifecycle.merge_event_id
