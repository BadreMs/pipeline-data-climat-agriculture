# Charge .env et l'exporte dans l'environnement des recettes : env_var() de
# dbt_project/profiles.yml lit l'env du process, pas le fichier .env.
-include .env
export

.PHONY: up down logs init-db ingest ingest-dry ingest-quick dags-check test lint format streamlit sync
.PHONY: dbt-deps dbt-check dbt-check-docker dbt-seed dbt-run dbt-test

# Demarre Postgres + Airflow (webserver/scheduler)
up:
	docker compose up -d --build

# Arrete et retire les containers (les volumes/donnees sont conserves)
down:
	docker compose down

logs:
	docker compose logs -f

# Rejoue les scripts d'init SQL (schemas raw/staging/marts) sur la base dwh deja demarree
init-db:
	docker compose exec -T postgres psql -U $${POSTGRES_USER} -d $${POSTGRES_DWH_DB} -f /docker-entrypoint-initdb.d/01_schemas.sql

# Lance l'ingestion locale (hors Airflow), cf. ingestion/run.py (Phase 1)
# Periode par defaut = OPENMETEO_START_DATE/OPENMETEO_END_DATE (.env), 12 regions
ingest:
	uv run python -m ingestion.run

# Smoke test : 1 region (MA-04 par defaut), 7 jours, vrai appel Open-Meteo,
# AUCUNE ecriture DB. --start est ignore en --dry-run mais reste accepte.
ingest-dry:
	uv run python -m ingestion.run --dry-run

# Iteration rapide en dev : 1 region, 1 mois, pour ne pas attendre un run complet
ingest-quick:
	uv run python -m ingestion.run --start 2024-01-01 --end 2024-01-31 --regions MA-04

# Verifie que les 3 DAGs s'importent sans erreur (necessite `make up` prealable)
dags-check:
	docker compose exec airflow-scheduler airflow dags list-import-errors

# Installe les packages dbt (dbt_utils) dans dbt_project/dbt_packages
dbt-deps:
	uv run dbt deps --project-dir dbt_project --profiles-dir dbt_project

# Verifie packages + profil + connexion DWH en local (Postgres up, host:5433 via .env)
dbt-check: dbt-deps
	uv run dbt debug --project-dir dbt_project --profiles-dir dbt_project

# Idem dans le container Airflow (necessite `make up`). GATE : POSTGRES_PORT doit valoir
# 5432 (reseau Docker), sinon dbt viserait le port host 5433 et echouerait.
dbt-check-docker:
	docker compose exec -T airflow-scheduler bash -c 'test "$$POSTGRES_PORT" = "5432" || { echo "GATE FAIL: POSTGRES_PORT=$$POSTGRES_PORT (attendu 5432)"; exit 1; }; dbt deps --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project && dbt debug --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project'

# Charge le seed dim_region (marts.dim_region) depuis dbt_project/seeds/dim_region.csv
dbt-seed:
	uv run dbt seed --project-dir dbt_project --profiles-dir dbt_project

# Construit staging (views), intermediate (views) et marts (tables)
dbt-run:
	uv run dbt run --project-dir dbt_project --profiles-dir dbt_project

# Tests dbt (sources, seeds, modeles)
dbt-test:
	uv run dbt test --project-dir dbt_project --profiles-dir dbt_project

test:
	uv run pytest

lint:
	uv run ruff check .
	uv run mypy .

format:
	uv run ruff format .
	uv run ruff check --fix .

streamlit:
	uv run streamlit run streamlit/app.py

sync:
	uv sync
