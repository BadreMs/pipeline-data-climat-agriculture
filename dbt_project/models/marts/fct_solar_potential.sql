-- 1 ligne = 1 region x 1 annee. Potentiel solaire.
-- kWh/m2/jour = MJ/m2/jour / 3.6 (applique dans stg_weather_daily).
-- solar_potential_score (0-100) = 0.7 x radiation_score + 0.3 x regularity_score, annees
-- completes seulement (couverture >= 95 %).
--   radiation_score  : rayonnement moyen, borne lineaire fixe 3.0 kWh/m2/j -> 0 ; 7.0 -> 100
--                      (bornes fixes, pas min-max entre regions : score stable dans le temps)
--   regularity_score : % de jours ensoleilles (>= 5 kWh/m2/j) = fiabilite de la production
with base as (
    select
        i.region_code,
        d.region_name,
        d.latitude,
        d.longitude,
        d.altitude_m,
        i.year,
        i.solar_days,
        i.solar_coverage,
        (i.solar_coverage >= 0.95) as is_complete_year,
        i.solar_radiation_avg_kwh_m2_day,
        i.solar_radiation_total_kwh_m2,
        i.sunny_days,
        round(100.0 * i.sunny_days / nullif(i.solar_days, 0), 1) as sunny_days_pct
    from {{ ref('int_weather_agriculture_join') }} as i
    inner join {{ ref('dim_region') }} as d on d.region_code = i.region_code
),

scored as (
    select
        *,
        round(
            100 * least(greatest((solar_radiation_avg_kwh_m2_day - 3.0) / (7.0 - 3.0), 0), 1), 1
        ) as radiation_score,
        sunny_days_pct as regularity_score
    from base
)

select
    region_code,
    region_name,
    latitude,
    longitude,
    altitude_m,
    year,
    is_complete_year,
    solar_days,
    solar_coverage,
    round(solar_radiation_avg_kwh_m2_day, 3) as solar_radiation_avg_kwh_m2_day,
    round(solar_radiation_total_kwh_m2, 1) as solar_radiation_total_kwh_m2,
    sunny_days,
    sunny_days_pct,
    case when is_complete_year then radiation_score end as radiation_score,
    case when is_complete_year then regularity_score end as regularity_score,
    case
        when is_complete_year then round(0.7 * radiation_score + 0.3 * regularity_score, 1)
    end as solar_potential_score
from scored
