# Pipelines Data Factory

> Non importés dans une vraie Data Factory à ce jour : validés uniquement comme JSON et pour la
> cohérence de leurs références (`tests/test_azure_artifacts.py`).

Les fichiers sont au **format Git d'ADF** (`{ "name": ..., "properties": ... }`), regroupés par
type : `linked_services/`, `datasets/`, `pipelines/`, `triggers/` (dossier parent). Ils
contiennent des placeholders `${...}` (compte de stockage, serveur SQL, workspace Databricks)
remplacés par `envsubst` dans `scripts/deploy.sh`.

## Pipelines

| Pipeline | Rôle | Déclencheur |
|---|---|---|
| `ingest_openmeteo` | Une requête Open-Meteo par région vers ADLS bronze | `tr_daily_openmeteo` : tous les jours à 03:00 (Maroc) |
| `ingest_agriculture` | CSV agriculture/hydrique vers ADLS bronze (ou fixture mock) | `tr_monthly_agriculture` : le 1er du mois à 04:00 |
| `transform_databricks` | 3 notebooks puis procédure SQL `marts.usp_refresh_serving` | `tr_daily_transform` : tous les jours à 04:30 |

Les horaires reproduisent ceux d'Airflow (`dag_ingest_openmeteo`, `dag_ingest_agriculture`,
`dag_transform_dbt`). Les triggers sont créés à l'état **arrêté** (`runtimeState: Stopped`).

### `ingest_openmeteo`

| Paramètre | Défaut | Rôle |
|---|---|---|
| `regions` | les 12 régions (`code`, `latitude`, `longitude`) | Liste parcourue par le `ForEach` ; identique à `ingestion/regions.py` (test automatique) |
| `lookbackDays` | `5` | Fenêtre glissante : aujourd'hui et les 4 jours précédents (délai de consolidation de l'archive) |
| `startDate`, `endDate` | `""` | Si renseignés, remplacent la fenêtre glissante : sert au **backfill** (ex. `startDate=2015-01-01`) |

- Boucle **séquentielle** (comme l'ingestion locale) pour ménager l'API publique.
- Chaque `Copy` : timeout 10 min, **2 retries espacés de 5 min** (comme Airflow).
- Sortie : `bronze/openmeteo/ingest_date=<jour>/region_code=<MA-xx>/data.json`.
- Les variables demandées (8 variables journalières) et le fuseau `Africa/Casablanca` sont ceux
  du client local.

> **À valider au premier run** : le `Copy` REST → JSON doit conserver la structure
> hiérarchique de la réponse (`daily.time[]`, `daily.<variable>[]`) telle que lue par
> `01_bronze_to_silver.py`. Si ADF aplatissait la réponse, ajuster le mapping du `Copy`.
>
> Le client local découpe les longues périodes en chunks de 2 ans ; ici une seule requête
> couvre toute la plage `startDate`–`endDate`. Les limites de l'API sur une plage aussi longue
> n'ont pas été vérifiées : pour un backfill 2015 → aujourd'hui, lancer le pipeline **par
> tranches de 2 ans**, comme le client local, tant que ce point n'est pas validé.

### `ingest_agriculture`

| Paramètre | Défaut | Rôle |
|---|---|---|
| `sourceUrl` | `""` | URL d'un CSV région × année. **Vide : la fixture mock de `bronze/seed/` est copiée** |

- Même repli que l'ingestion locale : aucune URL data.gov.ma stable ne fournit une série
  région × année.
- Sortie : `bronze/agriculture/ingest_date=<jour>/source=<mock-not-configured|datagovma>/agriculture_regional.csv`.
  La partition `source` devient `data_source` / `is_mock` en silver.

### `transform_databricks`

| Paramètre | Défaut | Rôle |
|---|---|---|
| `storageAccountName`, `sqlServerName`, `sqlDatabaseName` | placeholders `${...}` | Transmis aux notebooks (`baseParameters`) |
| `secretScope` | `kv-climat-agriculture` | Scope Databricks adossé à Key Vault (secrets `sql-user`, `sql-password`) |

Enchaînement : `BronzeToSilver` → `SilverToGold` → `GoldToSql` → `RefreshServingTables`
(procédure stockée). Si un notebook échoue, la procédure de swap n'est pas appelée : le serving
garde les données du run précédent.

## Services liés

| Linked service | Type | Authentification |
|---|---|---|
| `ls_adls_datalake` | ADLS Gen2 | identité managée d'ADF |
| `ls_rest_openmeteo` | REST | anonyme (API publique) |
| `ls_http_datagovma` | HTTP, URL paramétrée | anonyme |
| `ls_databricks` | Databricks, job cluster single-node créé à la volée | identité managée d'ADF (rôle Contributor sur le workspace) |
| `ls_azuresql` | Azure SQL DB | identité managée d'ADF (utilisateur contenu dans la base) |

Aucun secret dans ces fichiers. La version du runtime Databricks (`15.4.x-scala2.12`) est à
aligner sur une version LTS supportée au moment du déploiement.
