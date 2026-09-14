.PHONY: up down logs init-db ingest ingest-dry ingest-quick dbt-run dbt-test test lint format streamlit sync

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

dbt-run:
	uv run dbt run --project-dir dbt_project --profiles-dir dbt_project

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
