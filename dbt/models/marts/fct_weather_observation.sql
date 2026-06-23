/*
  Fact table: one row per weather observation (grain = observation_id).

  Incremental so each run only processes newly-landed partitions instead of
  rebuilding history. dbt-athena uses the `dt`-style insert strategy; we filter
  on ingest_date so only fresh partitions are scanned.
*/
{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partitioned_by = ['ingest_date'],
    unique_key = 'observation_id',
    on_schema_change = 'append_new_columns'
  )
}}

select
    w.observation_id,
    w.city_id,
    w.observed_at,
    w.observed_date,
    w.temperature_c,
    w.apparent_temperature_c,
    w.relative_humidity_pct,
    w.precipitation_mm,
    w.wind_speed_ms,
    w.wind_direction_deg,
    w.pressure_msl_hpa,
    w.cloud_cover_pct,
    w.weather_code,
    w.is_day,
    w.is_daytime,
    w.ingested_at,
    -- Derived flags useful for the daily aggregate and dashboards.
    (w.precipitation_mm > 0)                         as is_raining,
    (w.temperature_c >= 30)                          as is_hot,
    (w.temperature_c <= 0)                           as is_freezing,
    w.ingest_date
from {{ ref('stg_weather') }} w

{% if is_incremental() %}
  -- Only scan partitions newer than what we've already loaded.
  where w.ingest_date >= (select coalesce(max(ingest_date), '1900-01-01') from {{ this }})
{% endif %}
