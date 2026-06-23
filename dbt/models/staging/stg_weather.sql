/*
  Staging: one clean, typed, de-duplicated row per observation.

  The clean zone can in theory contain the same observation_id twice if an hour
  was re-ingested across a partition boundary, so we keep only the most recently
  ingested copy of each observation_id.
*/
with source as (
    select * from {{ source('weather_lake', 'weather') }}
),

ranked as (
    select
        observation_id,
        city_id,
        city_name,
        country,
        cast(latitude as double)              as latitude,
        cast(longitude as double)             as longitude,
        timezone,
        cast(observed_at as timestamp)        as observed_at,
        cast(temperature_c as double)         as temperature_c,
        cast(apparent_temperature_c as double) as apparent_temperature_c,
        cast(relative_humidity_pct as double) as relative_humidity_pct,
        cast(precipitation_mm as double)      as precipitation_mm,
        cast(wind_speed_ms as double)         as wind_speed_ms,
        cast(wind_direction_deg as double)    as wind_direction_deg,
        cast(pressure_msl_hpa as double)      as pressure_msl_hpa,
        cast(cloud_cover_pct as double)       as cloud_cover_pct,
        cast(weather_code as integer)         as weather_code,
        cast(is_day as integer)               as is_day,
        cast(ingested_at as timestamp)        as ingested_at,
        dt                                    as ingest_date,
        row_number() over (
            partition by observation_id
            order by cast(ingested_at as timestamp) desc
        ) as _row_num
    from source
)

select
    observation_id,
    city_id,
    city_name,
    country,
    latitude,
    longitude,
    timezone,
    observed_at,
    temperature_c,
    apparent_temperature_c,
    relative_humidity_pct,
    precipitation_mm,
    wind_speed_ms,
    wind_direction_deg,
    pressure_msl_hpa,
    cloud_cover_pct,
    weather_code,
    is_day,
    ingested_at,
    ingest_date,
    -- Convenience grain helpers for the marts.
    date(observed_at)                         as observed_date,
    (is_day = 1)                              as is_daytime
from ranked
where _row_num = 1
