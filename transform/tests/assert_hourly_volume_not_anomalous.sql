-- Singular test: fails if the most recent day's hourly volumes look
-- anomalous vs. their own trailing 7-day history for that same hour-of-day.
--
-- WHY per hour-of-day, not a flat threshold: GitHub traffic is heavily
-- diurnal — comparing a quiet 3am hour against a flat threshold tuned for
-- daytime volume would false-alarm every single night. Comparing 3am
-- against other 3ams (and 2pm against other 2pms) is the only comparison
-- that's actually meaningful.
--
-- WHY this can legitimately return zero rows even on a healthy pipeline:
-- with less than 7 days of continuous history (true today — this project's
-- real ingested data is a single day, 2024-01-15), there's no trailing
-- baseline to compare against, so the join below naturally produces no
-- rows and the test passes. That's an honest "not enough history to judge
-- yet," not a false positive — it'll have real teeth once the hourly cron
-- (Phase 7) has been running long enough to build up a baseline.
--
-- WHY severity: warn: a single anomalous hour is worth investigating in
-- daylight, not paging someone at 3am — it could just as easily be a real
-- traffic spike as a real problem.
{{ config(severity='warn') }}

with hourly as (
    select
        mart_hourly_volume.date_key,
        mart_hourly_volume.event_hour,
        mart_hourly_volume.event_count,
        dim_date.date_day
    from {{ ref('mart_hourly_volume') }} as mart_hourly_volume
    inner join {{ ref('dim_date') }} as dim_date
        on mart_hourly_volume.date_key = dim_date.date_key
),

latest_day as (
    select max(date_day) as date_day from hourly
),

baseline_stats as (
    select
        hourly.event_hour,
        avg(hourly.event_count) as mean_count,
        stddev_samp(hourly.event_count) as stddev_count
    from hourly, latest_day
    where hourly.date_day >= latest_day.date_day - interval 7 day
      and hourly.date_day < latest_day.date_day
    group by hourly.event_hour
),

latest as (
    select hourly.*
    from hourly, latest_day
    where hourly.date_day = latest_day.date_day
)

select
    latest.date_day,
    latest.event_hour,
    latest.event_count,
    baseline_stats.mean_count,
    baseline_stats.stddev_count
from latest
inner join baseline_stats
    on latest.event_hour = baseline_stats.event_hour
where baseline_stats.stddev_count > 0
  and abs(latest.event_count - baseline_stats.mean_count) > 3 * baseline_stats.stddev_count
