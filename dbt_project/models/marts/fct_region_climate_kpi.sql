-- 1 ligne = 1 region x 1 annee. KPI secheresse.
-- precip_et0_ratio = precipitations / ET0 annuelles (indice d'aridite, seuils UNEP).
-- drought_class et spi_simplified : annees COMPLETES seulement (couverture >= 95 %), car une
-- annee partielle est biaisee par la saisonnalite des pluies.
-- spi_simplified = z-score des precipitations annuelles vs moyenne/ecart-type de la region sur
-- les annees completes (>= 5 annees). Ce n'est PAS le SPI normalise (pas d'ajustement gamma).
with base as (
    select
        i.*,
        d.region_name,
        (i.water_balance_coverage >= 0.95) as is_complete_year,
        case
            when i.et0_total_mm > 0 then i.precipitation_total_mm / i.et0_total_mm
        end as precip_et0_ratio
    from {{ ref('int_weather_agriculture_join') }} as i
    inner join {{ ref('dim_region') }} as d on d.region_code = i.region_code
),

with_reference as (
    select
        *,
        avg(precipitation_total_mm) filter (where is_complete_year)
            over (partition by region_code) as precip_ref_mean_mm,
        stddev_samp(precipitation_total_mm) filter (where is_complete_year)
            over (partition by region_code) as precip_ref_std_mm,
        count(*) filter (where is_complete_year)
            over (partition by region_code) as precip_ref_years
    from base
)

select
    region_code,
    region_name,
    year,
    is_complete_year,
    water_balance_days,
    water_balance_coverage,
    round(precipitation_total_mm, 1) as precipitation_total_mm,
    round(et0_total_mm, 1) as et0_total_mm,
    round(precip_et0_ratio, 3) as precip_et0_ratio,
    -- Seuils d'aridite (UNEP) : <0.20 tres sec | 0.20-0.50 sec | 0.50-0.65 normal | >=0.65 humide
    case
        when not is_complete_year or precip_et0_ratio is null then null
        when precip_et0_ratio < 0.20 then 'tres_sec'
        when precip_et0_ratio < 0.50 then 'sec'
        when precip_et0_ratio < 0.65 then 'normal'
        else 'humide'
    end as drought_class,
    dry_days,
    round(dry_days::numeric / nullif(water_balance_days, 0), 3) as dry_days_ratio,
    case
        when is_complete_year and precip_ref_years >= 5 and precip_ref_std_mm > 0
            then round((precipitation_total_mm - precip_ref_mean_mm) / precip_ref_std_mm, 2)
    end as spi_simplified,
    has_agriculture_data,
    production_tonnes,
    irrigated_area_ha,
    water_resource_m3,
    agriculture_source,
    agriculture_is_mock
from with_reference
