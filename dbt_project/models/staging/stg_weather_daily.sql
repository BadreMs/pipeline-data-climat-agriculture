-- 1 ligne = 1 region x 1 jour. Typage/renommage (unites explicites), flags de completude.
-- Les nulls Open-Meteo (derniers jours non consolides) sont conserves tels quels.
-- Unites par defaut Open-Meteo : degC, mm, MJ/m2/jour, km/h, %.
select
    region_code,
    date as observation_date,
    temperature_2m_max as temp_max_c,
    temperature_2m_min as temp_min_c,
    temperature_2m_mean as temp_mean_c,
    precipitation_sum as precipitation_mm,
    et0_fao_evapotranspiration as et0_mm,
    shortwave_radiation_sum as solar_radiation_mj_m2,
    -- Formule solaire validee : kWh/m2 = MJ/m2 / 3.6 (1 kWh = 3.6 MJ)
    round(shortwave_radiation_sum / 3.6, 3) as solar_radiation_kwh_m2,
    relative_humidity_2m_mean as humidity_mean_pct,
    wind_speed_10m_max as wind_max_kmh,
    -- Un jour n'entre dans le bilan hydrique que si precipitation ET ET0 sont connues
    (precipitation_sum is not null and et0_fao_evapotranspiration is not null)
        as has_water_balance_data,
    (shortwave_radiation_sum is not null) as has_solar_data,
    _ingested_at as ingested_at
from {{ source('raw', 'weather_daily') }}
