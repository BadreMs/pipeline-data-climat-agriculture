-- Tables de serving (schema en etoile) pour Power BI.
-- Grain des faits : 1 ligne = 1 region x 1 annee. Memes noms de colonnes que les marts dbt
-- (fct_region_climate_kpi, fct_solar_potential) ; les attributs descriptifs de region (nom,
-- coordonnees) ne sont que dans dim_region.
-- Idempotent. Prerequis : 01_create_schemas.sql.

-- ---------------------------------------------------------------------------
-- Dimensions
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'marts.dim_region', N'U') IS NULL
CREATE TABLE marts.dim_region (
    region_code  CHAR(5)       NOT NULL CONSTRAINT pk_dim_region PRIMARY KEY,  -- ISO 3166-2:MA
    region_name  NVARCHAR(100) NOT NULL,
    capital      NVARCHAR(100) NOT NULL,
    latitude     DECIMAL(9, 6) NOT NULL,  -- point de mesure climatique
    longitude    DECIMAL(9, 6) NOT NULL,
    altitude_m   DECIMAL(7, 1) NOT NULL
);
GO

IF OBJECT_ID(N'marts.dim_year', N'U') IS NULL
CREATE TABLE marts.dim_year (
    [year]           SMALLINT     NOT NULL CONSTRAINT pk_dim_year PRIMARY KEY,
    is_complete_year BIT          NOT NULL,  -- 1 si toutes les regions ont >= 95 % de jours
    year_label       NVARCHAR(20) NOT NULL   -- '2025' ou '2026 (partielle)'
);
GO

-- ---------------------------------------------------------------------------
-- Faits
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'marts.fct_region_climate_kpi', N'U') IS NULL
CREATE TABLE marts.fct_region_climate_kpi (
    region_code            CHAR(5)        NOT NULL,
    [year]                 SMALLINT       NOT NULL,
    is_complete_year       BIT            NOT NULL,
    water_balance_days     SMALLINT       NOT NULL,
    water_balance_coverage DECIMAL(4, 3)  NOT NULL,
    precipitation_total_mm DECIMAL(9, 1)  NULL,
    et0_total_mm           DECIMAL(9, 1)  NULL,
    precip_et0_ratio       DECIMAL(7, 3)  NULL,   -- indice d'aridite (precipitations / ET0)
    drought_class          VARCHAR(10)    NULL,   -- null pour une annee incomplete
    dry_days               SMALLINT       NOT NULL,
    dry_days_ratio         DECIMAL(4, 3)  NULL,
    spi_simplified         DECIMAL(5, 2)  NULL,   -- z-score, pas le SPI normalise (gamma)
    has_agriculture_data   BIT            NOT NULL,
    production_tonnes      DECIMAL(14, 2) NULL,   -- DONNEES SYNTHETIQUES si agriculture_is_mock
    irrigated_area_ha      DECIMAL(14, 2) NULL,
    water_resource_m3      DECIMAL(16, 2) NULL,
    agriculture_source     VARCHAR(40)    NULL,
    agriculture_is_mock    BIT            NULL,
    CONSTRAINT pk_fct_region_climate_kpi PRIMARY KEY (region_code, [year]),
    CONSTRAINT fk_fct_climate_region FOREIGN KEY (region_code) REFERENCES marts.dim_region (region_code),
    CONSTRAINT fk_fct_climate_year FOREIGN KEY ([year]) REFERENCES marts.dim_year ([year]),
    CONSTRAINT ck_fct_climate_class CHECK (
        drought_class IS NULL OR drought_class IN ('tres_sec', 'sec', 'normal', 'humide')
    )
);
GO

IF OBJECT_ID(N'marts.fct_solar_potential', N'U') IS NULL
CREATE TABLE marts.fct_solar_potential (
    region_code                    CHAR(5)        NOT NULL,
    [year]                         SMALLINT       NOT NULL,
    is_complete_year               BIT            NOT NULL,
    solar_days                     SMALLINT       NOT NULL,
    solar_coverage                 DECIMAL(4, 3)  NOT NULL,
    solar_radiation_avg_kwh_m2_day DECIMAL(6, 3)  NULL,
    solar_radiation_total_kwh_m2   DECIMAL(10, 1) NULL,
    sunny_days                     SMALLINT       NOT NULL,
    sunny_days_pct                 DECIMAL(4, 1)  NULL,
    radiation_score                DECIMAL(4, 1)  NULL,  -- 0-100, null si annee incomplete
    regularity_score               DECIMAL(4, 1)  NULL,
    solar_potential_score          DECIMAL(4, 1)  NULL,  -- 0.7 x rayonnement + 0.3 x regularite
    CONSTRAINT pk_fct_solar_potential PRIMARY KEY (region_code, [year]),
    CONSTRAINT fk_fct_solar_region FOREIGN KEY (region_code) REFERENCES marts.dim_region (region_code),
    CONSTRAINT fk_fct_solar_year FOREIGN KEY ([year]) REFERENCES marts.dim_year ([year]),
    CONSTRAINT ck_fct_solar_score CHECK (
        solar_potential_score IS NULL OR solar_potential_score BETWEEN 0 AND 100
    )
);
GO

-- ---------------------------------------------------------------------------
-- Staging : copies sans contraintes (SELECT INTO ne copie ni cles ni index), videes par
-- TRUNCATE a chaque chargement Spark.
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'staging.dim_region', N'U') IS NULL
    SELECT * INTO staging.dim_region FROM marts.dim_region WHERE 1 = 0;
IF OBJECT_ID(N'staging.dim_year', N'U') IS NULL
    SELECT * INTO staging.dim_year FROM marts.dim_year WHERE 1 = 0;
IF OBJECT_ID(N'staging.fct_region_climate_kpi', N'U') IS NULL
    SELECT * INTO staging.fct_region_climate_kpi FROM marts.fct_region_climate_kpi WHERE 1 = 0;
IF OBJECT_ID(N'staging.fct_solar_potential', N'U') IS NULL
    SELECT * INTO staging.fct_solar_potential FROM marts.fct_solar_potential WHERE 1 = 0;
GO

-- ---------------------------------------------------------------------------
-- Swap staging -> marts : une seule transaction, Power BI ne voit jamais de tables vides.
-- Appelee par l'activite ADF "RefreshServingTables" apres 03_gold_to_sql.py.
-- ---------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE marts.usp_refresh_serving
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    -- Garde-fou : un chargement Spark rate ne doit pas vider le serving.
    IF NOT EXISTS (SELECT 1 FROM staging.fct_region_climate_kpi)
        OR NOT EXISTS (SELECT 1 FROM staging.fct_solar_potential)
        OR NOT EXISTS (SELECT 1 FROM staging.dim_region)
        OR NOT EXISTS (SELECT 1 FROM staging.dim_year)
        THROW 50001, 'Tables staging vides : refresh du serving annule.', 1;

    BEGIN TRANSACTION;

    -- Faits d'abord (cles etrangeres), puis dimensions.
    DELETE FROM marts.fct_solar_potential;
    DELETE FROM marts.fct_region_climate_kpi;
    DELETE FROM marts.dim_year;
    DELETE FROM marts.dim_region;

    INSERT INTO marts.dim_region (region_code, region_name, capital, latitude, longitude, altitude_m)
    SELECT region_code, region_name, capital, latitude, longitude, altitude_m
    FROM staging.dim_region;

    INSERT INTO marts.dim_year ([year], is_complete_year, year_label)
    SELECT [year], is_complete_year, year_label FROM staging.dim_year;

    INSERT INTO marts.fct_region_climate_kpi (
        region_code, [year], is_complete_year, water_balance_days, water_balance_coverage,
        precipitation_total_mm, et0_total_mm, precip_et0_ratio, drought_class, dry_days,
        dry_days_ratio, spi_simplified, has_agriculture_data, production_tonnes,
        irrigated_area_ha, water_resource_m3, agriculture_source, agriculture_is_mock
    )
    SELECT
        region_code, [year], is_complete_year, water_balance_days, water_balance_coverage,
        precipitation_total_mm, et0_total_mm, precip_et0_ratio, drought_class, dry_days,
        dry_days_ratio, spi_simplified, has_agriculture_data, production_tonnes,
        irrigated_area_ha, water_resource_m3, agriculture_source, agriculture_is_mock
    FROM staging.fct_region_climate_kpi;

    INSERT INTO marts.fct_solar_potential (
        region_code, [year], is_complete_year, solar_days, solar_coverage,
        solar_radiation_avg_kwh_m2_day, solar_radiation_total_kwh_m2, sunny_days,
        sunny_days_pct, radiation_score, regularity_score, solar_potential_score
    )
    SELECT
        region_code, [year], is_complete_year, solar_days, solar_coverage,
        solar_radiation_avg_kwh_m2_day, solar_radiation_total_kwh_m2, sunny_days,
        sunny_days_pct, radiation_score, regularity_score, solar_potential_score
    FROM staging.fct_solar_potential;

    COMMIT TRANSACTION;
END;
GO

-- ---------------------------------------------------------------------------
-- Vue de confort pour Power BI : une ligne par region x annee, faits + dimensions.
-- ---------------------------------------------------------------------------
CREATE OR ALTER VIEW marts.vw_region_year_kpi
AS
SELECT
    r.region_code,
    r.region_name,
    r.capital,
    r.latitude,
    r.longitude,
    r.altitude_m,
    k.[year],
    y.year_label,
    k.is_complete_year          AS climate_complete_year,
    s.is_complete_year          AS solar_complete_year,
    k.precip_et0_ratio,
    k.drought_class,
    CASE k.drought_class
        WHEN 'tres_sec' THEN N'Très sec'
        WHEN 'sec'      THEN N'Sec'
        WHEN 'normal'   THEN N'Normal'
        WHEN 'humide'   THEN N'Humide'
        ELSE N'Année incomplète'
    END                         AS drought_class_label,
    CASE k.drought_class        -- tri des libelles dans Power BI (colonne "Trier par")
        WHEN 'tres_sec' THEN 1 WHEN 'sec' THEN 2 WHEN 'normal' THEN 3 WHEN 'humide' THEN 4
        ELSE 5
    END                         AS drought_class_sort,
    k.precipitation_total_mm,
    k.et0_total_mm,
    k.dry_days,
    k.dry_days_ratio,
    k.spi_simplified,
    s.solar_radiation_avg_kwh_m2_day,
    s.sunny_days_pct,
    s.solar_potential_score,
    k.production_tonnes,
    k.irrigated_area_ha,
    k.water_resource_m3,
    k.agriculture_is_mock
FROM marts.fct_region_climate_kpi AS k
JOIN marts.fct_solar_potential AS s ON s.region_code = k.region_code AND s.[year] = k.[year]
JOIN marts.dim_region AS r ON r.region_code = k.region_code
JOIN marts.dim_year AS y ON y.[year] = k.[year];
GO

-- Lecteur Power BI : acces en lecture seule au schema marts.
IF DATABASE_PRINCIPAL_ID(N'powerbi_reader') IS NULL CREATE ROLE powerbi_reader;
GRANT SELECT ON SCHEMA::marts TO powerbi_reader;
GO
