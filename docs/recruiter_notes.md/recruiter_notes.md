# Pipeline Data Climat & Agriculture — Maroc

## Ce que fait le projet
Pipeline data end-to-end qui croise météo (Open-Meteo) et données agricoles
(open data) pour produire des KPIs de sécheresse et de potentiel solaire
sur les 12 régions du Maroc.

## Ce que ça démontre
- **Ingestion** : API REST (retry, chunking), CSV open data, fallback mock
- **Orchestration** : Apache Airflow (3 DAGs, backfill, CI)
- **Transformation** : dbt (medallion, 51 tests, idempotence)
- **Cloud-ready** : architecture Azure (Data Factory + ADLS + Databricks + SQL)
- **Restitution** : Streamlit interactif (carte, filtres, export CSV)
- **Qualité** : 223 tests, mypy strict, ruff, CI GitHub Actions

## Comment le tester en 5 min
```bash
git clone <url> && cd pipeline-data-climat-agriculture
cp .env.example .env
docker compose up -d postgres
uv sync && make ingest-quick
make streamlit