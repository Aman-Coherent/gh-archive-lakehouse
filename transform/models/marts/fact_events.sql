{{
    config(
        materialized='incremental',
        unique_key='event_id',
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
-- RESOLVED IN PHASE 7 (was flagged as a known gap in Phase 5): this model
-- is now incremental, not a full rebuild — each run only processes events
-- whose ingested_at is newer than what's already in this table, and
-- unique_key='event_id' means a force=true re-ingest correction *replaces*
-- the existing row for that event rather than duplicating it (verified
-- directly: a synthetic incremental+external+unique_key test correctly
-- replaced a changed key and appended a new one, not both).
--
-- WHY ingested_at as the watermark, not created_at: a backfill of an old
-- hour processed today has an old created_at but a today's ingested_at.
-- Filtering on created_at would silently skip backfilled history that
-- arrives after fact_events has already moved past that date — exactly
-- the "late-arriving data" problem this whole phase is about. ingested_at
-- always reflects "when THIS pipeline touched the row," so it's
-- monotonically safe to use as a high-watermark regardless of backfill order.
--
-- WHY mart_daily_repo_activity and mart_hourly_volume needed no changes to
-- fix the same problem: they already ref() this table, not stg_events
-- directly, so they inherit durability for free once this model has it.
-- mart_pr_lifecycle *did* need a change — see that model for why.

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
    events.ingested_at,

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

{% if is_incremental() %}
where events.ingested_at > (select coalesce(max(ingested_at), timestamp '1900-01-01') from {{ this }})
{% endif %}
