{{
    config(
        materialized='external',
        location=var('gold_root') ~ '/dim_date.parquet'
    )
}}

-- Generated calendar, 2015-01-01 through 2030-12-31 — comfortably covers
-- this project's lifetime plus GH Archive's own history on the early side.
with days as (
    select cast('2015-01-01' as date) + interval (i) day as date_day
    from range(0, date_diff('day', cast('2015-01-01' as date), cast('2031-01-01' as date))) as t(i)
)

select
    cast(strftime(date_day, '%Y%m%d') as int) as date_key,
    date_day,
    dayofweek(date_day) as day_of_week,  -- 0=Sunday .. 6=Saturday (DuckDB convention)
    dayofweek(date_day) in (0, 6) as is_weekend,
    weekofyear(date_day) as iso_week,
    month(date_day) as month,
    quarter(date_day) as quarter,
    year(date_day) as year
from days
