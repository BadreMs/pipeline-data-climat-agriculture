-- 1 ligne = 1 region x 1 annee (annee presente dans la meteo).
-- Agregation annuelle de la meteo + LEFT JOIN agriculture : les annees sans donnees
-- agricoles (apres 2023 avec le mock) sont conservees, agriculture_* = null.
-- Les sommes du bilan hydrique ne portent que sur les jours ou precip ET ET0 sont connues
-- (ratio P/ET0 coherent). Seuils (jour sec < 1 mm, jour ensoleille >= 5 kWh/m2) : ici seulement.
with weather_by_year as (
    select
        region_code,
        extract(year from observation_date)::int as year,
        count(*) as days_observed,
        count(*) filter (where has_water_balance_data) as water_balance_days,
        sum(precipitation_mm) filter (where has_water_balance_data) as precipitation_total_mm,
        sum(et0_mm) filter (where has_water_balance_data) as et0_total_mm,
        count(*) filter (where has_water_balance_data and precipitation_mm < 1.0) as dry_days,
        count(*) filter (where has_solar_data) as solar_days,
        avg(solar_radiation_kwh_m2) as solar_radiation_avg_kwh_m2_day,
        sum(solar_radiation_kwh_m2) as solar_radiation_total_kwh_m2,
        count(*) filter (where solar_radiation_kwh_m2 >= 5.0) as sunny_days,
        avg(temp_mean_c) as temp_mean_avg_c,
        avg(humidity_mean_pct) as humidity_mean_avg_pct
    from {{ ref('stg_weather_daily') }}
    group by 1, 2
),

with_coverage as (
    select
        *,
        (make_date(year, 12, 31) - make_date(year, 1, 1) + 1) as days_in_year
    from weather_by_year
)

select
    w.*,
    round(w.water_balance_days::numeric / w.days_in_year, 3) as water_balance_coverage,
    round(w.solar_days::numeric / w.days_in_year, 3) as solar_coverage,
    (a.region_code is not null) as has_agriculture_data,
    a.production_tonnes,
    a.irrigated_area_ha,
    a.water_resource_m3,
    a.data_source as agriculture_source,
    a.is_mock as agriculture_is_mock
from with_coverage as w
left join {{ ref('stg_agriculture_regional') }} as a
    on a.region_code = w.region_code
    and a.year = w.year
