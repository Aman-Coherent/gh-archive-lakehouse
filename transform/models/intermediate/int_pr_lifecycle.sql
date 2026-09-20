-- WHY this lives in intermediate/, not inlined twice: both fact_events
-- (hours_to_merge, on the merge-event row) and mart_pr_lifecycle (percentiles
-- by repo) need "how long between a PR opening and merging" — computing this
-- self-join once here and having both ref() it means there's exactly one
-- place that defines what "hours to merge" means.
--
-- WHY this can undercount: bronze/silver only retain BRONZE_RETENTION_DAYS
-- of history (7 days by default). A PR opened before that window and merged
-- within it has no matching "opened" row anymore, so it's silently absent
-- here rather than wrong — a real, documented limitation of computing PR
-- lifecycle from a rolling event window instead of GitHub's full PR history.

with pr_events as (
    select
        event_id,
        repo_id,
        pr_number,
        action,
        created_at
    from {{ ref('stg_events') }}
    where event_type = 'PullRequestEvent'
      and pr_number is not null
      and action in ('opened', 'merged')
),

opened as (
    select repo_id, pr_number, created_at as opened_at
    from pr_events
    where action = 'opened'
    -- WHY take the earliest: a reopened PR could have more than one
    -- "opened" action; the PR's true lifecycle starts at the first one.
    qualify row_number() over (partition by repo_id, pr_number order by created_at asc) = 1
),

merged as (
    select
        event_id as merge_event_id,
        repo_id,
        pr_number,
        created_at as merged_at
    from pr_events
    where action = 'merged'
)

select
    merged.merge_event_id,
    merged.repo_id,
    merged.pr_number,
    opened.opened_at,
    merged.merged_at,
    date_diff('second', opened.opened_at, merged.merged_at) / 3600.0 as hours_to_merge
from merged
inner join opened
    on merged.repo_id = opened.repo_id
    and merged.pr_number = opened.pr_number
