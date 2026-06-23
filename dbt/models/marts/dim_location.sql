/*
  Location dimension: one row per city we ingest. Picks the most recent
  metadata seen for each city_id.
*/
with ranked as (
    select
        city_id,
        city_name,
        country,
        latitude,
        longitude,
        timezone,
        row_number() over (partition by city_id order by ingested_at desc) as _rn
    from {{ ref('stg_weather') }}
)

select
    city_id,
    city_name,
    country,
    latitude,
    longitude,
    timezone
from ranked
where _rn = 1
