-- Schemas medaillon pour la base dwh (raw -> staging -> marts)
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

COMMENT ON SCHEMA raw IS 'Donnees brutes ingerees (Open-Meteo, data.gov.ma) sans transformation.';
COMMENT ON SCHEMA staging IS 'Donnees nettoyees/typees, gerees par dbt (models/staging).';
COMMENT ON SCHEMA marts IS 'Indicateurs metier finaux (KPI secheresse, potentiel solaire).';
