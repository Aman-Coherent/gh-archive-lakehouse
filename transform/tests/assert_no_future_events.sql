-- Singular test: fails if it returns any rows.
--
-- WHY on stg_events (silver), not fact_events (gold): catch a corrupt
-- timestamp as early in the pipeline as possible, before it propagates into
-- every downstream mart. A future created_at means either a corrupt
-- upstream event or a clock-skew bug in this pipeline — both are "page
-- someone now," so this stays at dbt's default severity: error.
--
-- WHY current_timestamp at time zone 'UTC', not bare current_timestamp:
-- DuckDB's current_timestamp reflects the session's local system timezone,
-- not UTC (confirmed directly — a machine set to IST, ahead of UTC, would
-- make this test require an event to be over 5 hours in the future before
-- ever flagging it; a machine behind UTC could flag perfectly valid current
-- data instead). created_at is naive but semantically UTC throughout this
-- project, so the comparison needs to be too.
{{ config(severity='error') }}

select *
from {{ ref('stg_events') }}
where created_at > (current_timestamp at time zone 'UTC')
