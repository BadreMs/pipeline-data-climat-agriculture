#!/bin/bash
# Cree la base Airflow en plus de la base par defaut (POSTGRES_DB=dwh).
# Execute automatiquement par l'image postgres au premier demarrage du volume.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE "${POSTGRES_AIRFLOW_DB}"'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${POSTGRES_AIRFLOW_DB}')\gexec
EOSQL
