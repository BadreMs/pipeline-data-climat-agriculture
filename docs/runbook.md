# Runbook

Exploitation du pipeline local : démarrage, commandes, ingestion, orchestration, dépannage.
Vue d'ensemble et choix de conception : [architecture.md](architecture.md).

## Quickstart local (5 minutes)

Prérequis : Docker (avec Compose), [uv](https://docs.astral.sh/uv/), Python 3.11+. `make` est
pratique mais optionnel (équivalents plus bas).

```bash
# 1. Configuration (les valeurs par défaut conviennent pour un essai local)
cp .env.example .env

# 2. Dépendances Python (verrouillées par uv.lock)
uv sync

# 3. Postgres + Airflow (le premier build de l'image prend quelques minutes)
make up

# 4. Ingestion : backfill complet 2015 -> aujourd'hui, 12 régions (quelques minutes)
make ingest

# 5. Transformations dbt : packages, seed, modèles, tests
make dbt-deps
make dbt-seed
make dbt-run
make dbt-test

# 6. Dashboard (http://localhost:8501)
make streamlit
```

Interfaces : Airflow http://localhost:8080 (identifiants dans `.env.example`) ; Adminer
(optionnel) `docker compose --profile tools up -d adminer` puis http://localhost:8081.

Sans `make` : `uv run python -m ingestion.run`, puis
`uv run dbt <seed|run|test> --project-dir dbt_project --profiles-dir dbt_project` avec les
variables de `.env` exportées dans le shell, puis `uv run streamlit run streamlit/app.py`.

## Commandes utiles

| Commande | Effet |
|---|---|
| `make up` / `make down` | Démarre (build inclus) / arrête les conteneurs ; les volumes sont conservés |
| `make logs` | Suit les logs Docker Compose |
| `make init-db` | Rejoue `sql/init/01_schemas.sql` sur un Postgres déjà initialisé |
| `make ingest` | Ingestion complète (période par défaut de `.env`) |
| `make ingest-dry` | Smoke test : 1 région, 7 jours, appel réel, **aucune écriture** |
| `make ingest-quick` | 1 région (MA-04), janvier 2024 : itération rapide |
| `make dbt-deps` | Installe `dbt_utils` (versions figées dans `package-lock.yml`) |
| `make dbt-check` | `dbt debug` en local : profil, packages, connexion Postgres |
| `make dbt-check-docker` | Idem dans le conteneur Airflow ; échoue si `POSTGRES_PORT` ≠ 5432 |
| `make dbt-seed` / `dbt-run` / `dbt-test` | Seed `dim_region`, modèles, 51 tests |
| `make dags-check` | Erreurs d'import des DAGs Airflow (nécessite `make up`) |
| `make test` / `make lint` / `make format` | pytest ; ruff + mypy ; formatage |
| `make streamlit` | Lance le dashboard |
| `uv run python -m scripts.export_dim_region` | Régénère `seeds/dim_region.csv` depuis `ingestion/regions.py` |

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

### Vérifier la complétude et réparer un trou

Un chunk en échec définitif donne l'exit code 1, mais le wrapper Airflow le masque (voir
[limites connues](architecture.md#known-limitations)). Après un long backfill, contrôler que
chaque région a le même nombre de jours :

```sql
SELECT region_code, count(*), min(date), max(date)
FROM raw.weather_daily GROUP BY 1 ORDER BY 2, 1;
```

Une région en retrait se répare en relançant l'ingestion sur la plage manquante (upsert
idempotent), puis en rafraîchissant dbt :

```bash
uv run python -m ingestion.run --start 2017-01-01 --end 2018-12-31 --regions MA-11 --skip-agriculture
make dbt-run && make dbt-test
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
| `dag_transform_dbt` | `30 4 * * *` (quotidien, 04h30 Africa/Casablanca) | `dbt deps` → `dbt seed` → `dbt run` → `dbt test`, après l'ingestion météo de 03h00 |

Tous les DAGs : `catchup=False`, `max_active_runs=1`. Retries : 2 (délai 5 min) pour les deux DAGs d'ingestion, 1 (délai 5 min) pour `dag_transform_dbt`. Les tâches d'ingestion appellent `airflow/scripts/run_ingestion.sh` (pas `python -m ingestion.run` directement) : ce wrapper traduit l'exit code **1** (succès partiel — chunks météo en échec, ou agriculture en `mock-fallback-network-error`) en **0**, pour éviter des retries Airflow sur un résultat qui n'est pas un échec. L'exit code **2** (erreur fatale) reste propagé et déclenche bien les retries.

### Vérifier que les DAGs sont valides

```bash
make up          # demarre Postgres + Airflow
make dags-check  # equivalent a: docker compose exec airflow-scheduler airflow dags list-import-errors
```

Aucune dépendance `apache-airflow` n'est installée localement (hors conteneur) : Airflow n'est pas officiellement supporté nativement sur Windows, et les DAGs restent volontairement fins (juste des `BashOperator`) — toute la logique testée vit dans `ingestion/`. La validation des DAGs se fait via la commande ci-dessus, pas via `make test`.

## Dépannage

### Git Bash : les chemins `/opt/...` sont convertis (MSYS)

Symptôme : `docker compose exec ... dbt debug --project-dir /opt/airflow/dbt_project` échoue avec
un chemin du type `C:\Program Files\Git\opt\airflow\...`. Git Bash réécrit les arguments qui
ressemblent à un chemin Unix. Contournements :

```bash
MSYS_NO_PATHCONV=1 docker compose exec airflow-scheduler dbt debug --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project
# ou doubler le slash initial : //opt/airflow/dbt_project
```

Le script `make dbt-check-docker` passe sa commande dans un `bash -c '...'` pour la même raison.

### Port Postgres : 5433 sur l'hôte, 5432 dans Docker

Postgres écoute sur 5432 **dans le réseau Docker** et est publié sur le **5433 de l'hôte**
(`POSTGRES_PORT` de `.env`) pour ne pas entrer en conflit avec un Postgres local.

- Depuis l'hôte (ingestion, dbt, Streamlit) : `localhost:5433`.
- Depuis un conteneur (Airflow) : `postgres:5432`. `docker-compose.yml` impose
  `POSTGRES_HOST=postgres` et `POSTGRES_PORT=5432` aux conteneurs Airflow, quelle que soit la
  valeur de `.env`.
- Symptôme d'une erreur : `connection refused` sur le mauvais port ; `make dbt-check-docker`
  échoue exprès (« GATE FAIL ») si le conteneur ne voit pas 5432.
- `connection refused` sur `localhost:5433` signifie en général que Postgres n'est pas démarré :
  `docker compose up -d postgres`.

### dbt 1.8 et les contraintes d'Airflow

Symptôme : `docker compose build` échoue avec un conflit de dépendances (`protobuf`,
`python-dotenv`, `sqlparse`...) après un changement de version de dbt ou d'une dépendance.
L'image installe le projet avec les contraintes officielles d'Airflow 2.9.3. dbt-core ≥ 1.9 n'y
est pas installable : voir [architecture.md](architecture.md#choix-techniques). Tester **avant**
de reconstruire l'image (quelques secondes au lieu de ~5 minutes) :

```bash
uv pip compile pyproject.toml --python-version 3.11 --no-emit-index-url \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt \
  --output-file /tmp/compile-test.txt
```

Après un changement de dépendances, reconstruire l'image : `docker compose up -d --build`.
Un effet de bord connu : `uv lock` impose des versions plus anciennes (mypy 1.x notamment).

### BOM UTF-8 dans `.env`

Un `.env` enregistré par certains éditeurs Windows commence par un BOM UTF-8 (octets `EF BB BF`).
GNU Make ≥ 4 l'ignore, mais un `source .env` en Bash voit le BOM au début de la première ligne :
celle-ci n'est plus un commentaire valide (erreur « command not found ») ou, si c'est une
variable, son nom est faussé. Pour charger `.env` dans un shell en l'éliminant :

```bash
set -a; source <(sed '1s/^\xEF\xBB\xBF//; s/\r$//' .env); set +a
```

Préférer enregistrer `.env` en UTF-8 **sans** BOM, fins de ligne LF.

### `make` absent sous Windows

`make` n'est pas installé par défaut (ni dans Git Bash). Options : l'installer (par exemple
`choco install make` ou `scoop install make`), ou lancer les commandes équivalentes de
[Quickstart](#quickstart-local-5-minutes). Le Makefile exporte `.env` dans l'environnement des
recettes (`-include .env` + `export`) parce que dbt lit les variables via `env_var()` ; sans
`make`, il faut exporter `.env` soi-même (commande de la section précédente).

### Sous PowerShell, `bash` ouvre un relais WSL vide

Symptôme : des tests qui lancent un script shell (`tests/test_run_ingestion_wrapper.py`,
`tests/test_azure_artifacts.py`) échouent avec `execvpe(/bin/bash) failed` sous PowerShell. Sur
Windows plusieurs `bash.exe` coexistent ; `C:\Windows\System32\bash.exe` est un relais WSL qui
échoue si aucune distribution n'est installée. Les tests passent sous Git Bash, ou en mettant
Git Bash devant dans le `PATH`.

### Autres

- **`dbt debug` : « git [ERROR] » dans le conteneur** : `git` n'est pas dans l'image Airflow.
  Cosmétique, sans effet (les packages viennent du hub dbt, pas d'un dépôt Git).
- **Tests qui échouent seulement avec `.env` exporté** : les tests de configuration neutralisent
  les variables d'environnement du process ; un nouveau test qui lit `Settings()` doit faire de
  même.
- **`dbt parse` : « unused configuration paths »** : n'apparaît que s'il n'y a aucun modèle dans
  un des dossiers staging, intermediate ou marts.
- **Le dashboard affiche « Impossible de lire les marts »** : Postgres arrêté, ou `dbt run` pas
  encore exécuté.
