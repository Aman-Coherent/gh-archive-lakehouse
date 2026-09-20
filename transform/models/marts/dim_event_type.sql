{{
    config(
        materialized='external',
        location=var('gold_root') ~ '/dim_event_type.parquet'
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['event_type']) }} as event_type_key,
    event_type,
    description,
    category
from {{ ref('event_types') }}
