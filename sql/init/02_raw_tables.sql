-- Tables raw (bronze) : donnees brutes ingerees, idempotentes via upsert.
-- Pas de FK vers une table de reference region : dim_region arrive en Phase 3 (dbt).
-- L'integrite du code region est assuree par un CHECK sur le format ISO 3166-2:MA.

CREATE TABLE raw.weather_daily (
    region_code                 TEXT          NOT NULL,
    date                        DATE          NOT NULL,
    temperature_2m_max          NUMERIC(5,2),
    temperature_2m_min          NUMERIC(5,2),
    temperature_2m_mean         NUMERIC(5,2),
    precipitation_sum           NUMERIC(6,2),
    et0_fao_evapotranspiration  NUMERIC(6,2),
    shortwave_radiation_sum     NUMERIC(7,2),
    relative_humidity_2m_mean   NUMERIC(5,2),
    wind_speed_10m_max          NUMERIC(5,2),
    _source                     TEXT          NOT NULL DEFAULT 'open-meteo-archive',
    _source_url                 TEXT,
    _ingested_at                TIMESTAMPTZ   NOT NULL DEFAULT now(),
    _batch_id                   UUID          NOT NULL,
    CONSTRAINT pk_weather_daily PRIMARY KEY (region_code, date),
    CONSTRAINT ck_weather_daily_region_code CHECK (region_code ~ '^MA-[0-9]{2}$'),
    CONSTRAINT ck_weather_daily_date_range
        CHECK (date BETWEEN DATE '1900-01-01' AND DATE '2100-01-01')
);

COMMENT ON TABLE raw.weather_daily IS
    'Observations climatiques journalieres par region (Open-Meteo archive API), brutes.';
COMMENT ON COLUMN raw.weather_daily.region_code IS
    'Code ISO 3166-2:MA (ex: MA-04). Pas de FK : reference geree en Phase 3 (dbt dim_region).';
COMMENT ON COLUMN raw.weather_daily._source_url IS
    'URL exacte de la requete Open-Meteo ayant produit cette ligne (tracabilite).';
COMMENT ON COLUMN raw.weather_daily._ingested_at IS
    'Horodatage du dernier upsert (mis a jour a chaque re-ingestion de la meme cle).';
COMMENT ON COLUMN raw.weather_daily._batch_id IS
    'Identifiant du run d ingestion (uuid4 genere cote Python dans run.py, un par execution).';

CREATE INDEX ix_weather_daily_region_date_desc
    ON raw.weather_daily (region_code, date DESC);

-- ---------------------------------------------------------------------------

CREATE TABLE raw.agriculture_regional (
    region_code                 TEXT          NOT NULL,
    annee                       INTEGER       NOT NULL,
    production_tonnes           NUMERIC(12,2),
    surface_irriguee_ha         NUMERIC(12,2),
    ressource_hydrique_m3       NUMERIC(14,2),
    _source                     TEXT          NOT NULL,
    _source_url                 TEXT,
    _ingested_at                TIMESTAMPTZ   NOT NULL DEFAULT now(),
    _batch_id                   UUID          NOT NULL,
    CONSTRAINT pk_agriculture_regional PRIMARY KEY (region_code, annee),
    CONSTRAINT ck_agriculture_regional_region_code CHECK (region_code ~ '^MA-[0-9]{2}$'),
    CONSTRAINT ck_agriculture_regional_annee_range CHECK (annee BETWEEN 1900 AND 2100)
);

COMMENT ON TABLE raw.agriculture_regional IS
    'Donnees agricoles/hydriques annuelles par region (data.gov.ma ou fixture mock), brutes.';
COMMENT ON COLUMN raw.agriculture_regional._source IS
    'Origine : ''datagovma'' ou ''mock-fallback'' (voir ingestion/datagovma_loader.py).';
COMMENT ON COLUMN raw.agriculture_regional._source_url IS
    'URL du CSV/XLSX source ; NULL si _source = ''mock-fallback''.';
