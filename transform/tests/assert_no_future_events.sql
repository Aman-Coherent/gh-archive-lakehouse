-- Singular test: fails if it returns any rows.
--
-- WHY on stg_events (silver), not fact_events (gold): catch a corrupt
-- timestamp as early in the pipeline as possible, before it propagates into
-- every downstream mart. A future created_at means either a corrupt
-- upstream event or a clock-skew bug in this pipeline — both are "page
-- someone now," so this stays at dbt's default severity: error.
{{ config(severity='error') }}

select *
from {{ ref('stg_events') }}
where created_at > current_timestamp
