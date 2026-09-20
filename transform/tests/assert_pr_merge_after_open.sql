-- Singular test: fails if it returns any rows.
--
-- WHY this checks int_pr_lifecycle.merged_at/opened_at instead of the
-- spec's original payload.pull_request.merged_at/created_at: ADR-001 (Phase
-- 1) established those embedded fields no longer exist in GitHub's feed —
-- int_pr_lifecycle derives the same open->merge relationship from paired
-- event timestamps instead. This test asserts the same real-world
-- invariant the spec intended (a PR can't be merged before it was opened),
-- just against the column names this pipeline actually has.
{{ config(severity='error') }}

select *
from {{ ref('int_pr_lifecycle') }}
where merged_at < opened_at
