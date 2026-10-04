-- 1 ligne = 1 region x 1 annee.
select
    region_code,
    annee as year,
    production_tonnes,
    surface_irriguee_ha as irrigated_area_ha,
    ressource_hydrique_m3 as water_resource_m3,
    _source as data_source,
    -- DONNEES SYNTHETIQUES tant qu'aucune URL data.gov.ma exploitable n'est configuree
    (_source like 'mock%') as is_mock,
    _ingested_at as ingested_at
from {{ source('raw', 'agriculture_regional') }}
