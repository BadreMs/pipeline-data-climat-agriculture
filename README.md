# Pipeline Data Climat & Agriculture

Pipeline data end-to-end qui croise des données climatiques ([Open-Meteo](https://open-meteo.com/)) et des données agricoles/hydriques ouvertes ([data.gov.ma](https://data.gov.ma/)) pour produire des indicateurs de sécheresse et de potentiel solaire par région du Maroc.

Architecture multi-cloud : orchestration et stockage locaux (Docker Compose : Postgres + Airflow + dbt), puis extension Azure (Data Factory, ADLS Gen2, Databricks, Azure SQL DB) documentée en Phase 5.

> **Note technique** : le dossier racine de ce dépôt (`Pipeline Data Climat & Agriculture`) contient des espaces et un `&`. Le nom de package/projet technique utilisé partout ailleurs (pyproject.toml, Docker Compose, imports Python) est `pipeline-data-climat-agriculture`.

## Stack technique

| Domaine | Outil |
|---|---|
| Langage | Python 3.11+ |
| Packaging | [uv](https://docs.astral.sh/uv/) |
| Orchestration | Apache Airflow (Docker Compose, LocalExecutor) |
| Stockage local | PostgreSQL 15 (Docker) |
| Transformations | dbt-core + dbt-postgres |
| Cloud (phase 2) | Azure Data Factory, ADLS Gen2, Databricks/PySpark, Azure SQL DB |
| Restitution | Streamlit (démo locale) + modèle Power BI documenté |
| Qualité | pytest, ruff, mypy, pre-commit |

## Architecture (medallion)

```
Bronze (raw) → Silver (staging) → Gold (marts)
```

- **Local** : Postgres, schémas `raw` / `staging` / `marts`, transformés via dbt.
- **Azure** (cible phase 2) : ADLS Gen2 (bronze/silver/gold) + Databricks + Azure SQL DB.

## Quickstart

```bash
# 1. Copier et compléter le fichier d'environnement
cp .env.example .env

# 2. Installer les dépendances Python (uv)
uv sync

# 3. Démarrer Postgres + Airflow
make up

# 4. Lancer l'ingestion locale (Phase 1)
make ingest

# 5. Lancer les transformations dbt (Phase 3)
make dbt-run
make dbt-test

# 6. Lancer le dashboard de démo
make streamlit
```

Airflow UI : http://localhost:8080 (admin/admin par défaut, cf. `.env.example`).
Adminer (optionnel, inspection Postgres) : `docker compose --profile tools up -d adminer` puis http://localhost:8081.

## Structure du projet

```
pipeline-data-climat-agriculture/
├── airflow/            # DAGs + image Airflow custom
├── ingestion/          # Clients Open-Meteo / data.gov.ma
├── dbt_project/        # Modèles dbt (staging / intermediate / marts)
├── streamlit/          # Dashboard de démo
├── azure/              # Code/docs extension Azure (phase 2, sans provisionnement)
├── sql/init/           # Scripts d'initialisation Postgres (bases + schémas)
├── tests/              # Tests unitaires
└── docs/               # Documentation d'architecture
```

## Roadmap

- [x] Phase 0 — Setup (repo, Docker Compose, tooling qualité)
- [ ] Phase 1 — Ingestion locale (Open-Meteo, data.gov.ma)
- [ ] Phase 2 — Orchestration Airflow
- [ ] Phase 3 — Transformations dbt
- [ ] Phase 4 — Restitution Streamlit
- [ ] Phase 5 — Extension Azure (code + docs)
- [ ] Phase 6 — Documentation finale

## Licence

MIT
