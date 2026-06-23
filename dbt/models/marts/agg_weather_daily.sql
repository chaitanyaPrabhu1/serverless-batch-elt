/*
  Daily analytics mart: one row per city per observed_date, summarizing the
  hourly observations collected that day. This is the "gold" table a dashboard
  or analyst would query.
*/
select
    f.city_id,
    l.city_name,
    l.country,
    f.observed_date,
    count(*)                                   as observation_count,
    round(avg(f.temperature_c), 2)             as avg_temperature_c,
    min(f.temperature_c)                       as min_temperature_c,
    max(f.temperature_c)                       as max_temperature_c,
    round(avg(f.relative_humidity_pct), 2)     as avg_humidity_pct,
    round(avg(f.wind_speed_ms), 2)             as avg_wind_speed_ms,
    round(sum(f.precipitation_mm), 2)          as total_precipitation_mm,
    sum(case when f.is_raining then 1 else 0 end)  as rainy_hours,
    sum(case when f.is_hot then 1 else 0 end)      as hot_hours,
    sum(case when f.is_freezing then 1 else 0 end) as freezing_hours
from {{ ref('fct_weather_observation') }} f
inner join {{ ref('dim_location') }} l
    on f.city_id = l.city_id
group by 1, 2, 3, 4
