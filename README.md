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

## Ingestion locale

### CLI

```
uv run python -m ingestion.run [--start YYYY-MM-DD] [--end YYYY-MM-DD]
                                [--regions MA-01,MA-04] [--skip-weather]
                                [--skip-agriculture] [--dry-run] [--verbose]
```

- `--start` (optionnel) : défaut = `OPENMETEO_START_DATE` (`.env`) si omis (un `WARNING` est loggué dans ce cas).
- `--end` (optionnel) : défaut = `OPENMETEO_END_DATE` (`.env`) ou aujourd'hui.
- `--regions` (optionnel) : codes ISO séparés par virgule (`MA-01`..`MA-12`). Défaut = les 12 (ou `MA-04` seul en `--dry-run`).
- `--skip-weather` / `--skip-agriculture` : désactive l'une des deux sources.
- `--dry-run` : smoke test — 1 région (MA-04 par défaut), 7 jours (indépendamment de `--start`/`--end`), vrai appel Open-Meteo, **aucune écriture en base**, sortie JSON sur stdout.
- `--verbose` : logs `DEBUG`.

### Exit codes

- `0` : succès complet (y compris si l'agriculture est en mode `mock-not-configured` — c'est l'état par défaut du projet tant qu'aucune URL data.gov.ma compatible n'a été trouvée, pas une panne).
- `1` : succès partiel/dégradé — au moins un chunk météo en échec définitif, **ou** l'agriculture est tombée en `mock-fallback-network-error` (une URL était configurée mais inaccessible).
- `2` : erreur fatale — code région inconnu, dates incohérentes (`--end` < `--start`), CSV agriculture distant accessible mais dont le schéma a changé (colonnes manquantes), base inaccessible.

### Idempotence

Chaque run génère un `batch_id` (uuid4) unique, partagé entre l'upsert météo et agriculture. Un re-run sur la même période écrase proprement les lignes existantes (`ON CONFLICT DO UPDATE`, cf. `sql/init/02_raw_tables.sql`). Chaque ligne météo garde par ailleurs l'URL exacte de la requête Open-Meteo qui l'a produite (`_source_url`), pas une URL générique.

### Exemples

```bash
make ingest        # 12 regions, periode par defaut (.env)
make ingest-dry     # smoke test, sans ecriture DB
make ingest-quick   # 1 region (MA-04), janvier 2024 - iteration rapide en dev
```

## Orchestration Airflow

### Backfill initial (manuel, une seule fois)

Le backfill historique complet ne passe **pas** par `airflow dags backfill` : `OpenMeteoClient` découpe déjà la période en chunks de 2 ans côté client, donc une seule commande couvre 2015 → aujourd'hui en quelques dizaines de requêtes. Rejouer ça via `airflow dags backfill` exécuterait le DAG quotidien une fois par jour depuis 2015 (~4000 runs) pour un gain nul.

```bash
make ingest   # backfill complet, 12 regions, periode par defaut (.env)
```

### DAGs

| DAG | Schedule | Rôle |
|---|---|---|
| `dag_ingest_openmeteo` | `0 3 * * *` (quotidien, 03h00 Africa/Casablanca) | Fenêtre glissante de 5 jours (couvre le délai de consolidation de l'archive Open-Meteo) — 1 seule tâche pour les 12 régions |
| `dag_ingest_agriculture` | `0 4 1 * *` (mensuel, le 1er) | `--skip-weather`, pas de plage de dates |
| `dag_transform_dbt` | `None` (déclenchement manuel) | Squelette Phase 3 — `dbt run` → `dbt test`, sera activé une fois `dbt_project/` peuplé |

Tous les DAGs : `catchup=False`, `max_active_runs=1`, `retries=2` (délai 5 min). Les tâches d'ingestion appellent `airflow/scripts/run_ingestion.sh` (pas `python -m ingestion.run` directement) : ce wrapper traduit l'exit code **1** (succès partiel — chunks météo en échec, ou agriculture en `mock-fallback-network-error`) en **0**, pour éviter des retries Airflow sur un résultat qui n'est pas un échec. L'exit code **2** (erreur fatale) reste propagé et déclenche bien les retries.

### Vérifier que les DAGs sont valides

```bash
make up          # demarre Postgres + Airflow
make dags-check  # equivalent a: docker compose exec airflow-scheduler airflow dags list-import-errors
```

Aucune dépendance `apache-airflow` n'est installée localement (hors conteneur) : Airflow n'est pas officiellement supporté nativement sur Windows, et les DAGs restent volontairement fins (juste des `BashOperator`) — toute la logique testée (128+ tests) vit dans `ingestion/`. La validation des DAGs se fait via la commande ci-dessus, pas via `make test`.

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
- [x] Phase 1 — Ingestion locale (Open-Meteo, data.gov.ma)
- [x] Phase 2 — Orchestration Airflow
- [ ] Phase 3 — Transformations dbt
- [ ] Phase 4 — Restitution Streamlit
- [ ] Phase 5 — Extension Azure (code + docs)
- [ ] Phase 6 — Documentation finale

## Licence

MIT
