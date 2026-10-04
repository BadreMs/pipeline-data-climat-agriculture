# Notebooks Databricks (PySpark)

> **Jamais exécutés.** Aucun cluster Databricks n'a été provisionné et pyspark n'est pas
> installé dans le projet. Les fichiers sont vérifiés par mypy (strict) et ruff, et leurs
> constantes métier sont comparées au SQL dbt par `tests/test_azure_artifacts.py` (le module est
> importé avec un faux `pyspark`). La logique Spark elle-même n'a pas été testée.

Format « source » Databricks : `# Databricks notebook source`, cellules séparées par
`# COMMAND ----------`. Toute l'exécution est dans `main()`, appelé sous
`if __name__ == "__main__"` (vrai dans un notebook), ce qui permet d'importer le module pour les
tests.

## Ordre d'exécution

| # | Notebook | Lit | Écrit | Équivalent dbt |
|---|---|---|---|---|
| 1 | `01_bronze_to_silver.py` | `bronze/openmeteo`, `bronze/agriculture`, `bronze/reference/dim_region.csv` | `silver/weather_daily` (partitionné par `year`), `silver/agriculture_regional` | `stg_weather_daily`, `stg_agriculture_regional` |
| 2 | `02_silver_to_gold.py` | `silver/*`, `bronze/reference/dim_region.csv` | `gold/dim_region`, `gold/dim_year`, `gold/fct_region_climate_kpi`, `gold/fct_solar_potential` | `int_weather_agriculture_join`, `fct_region_climate_kpi`, `fct_solar_potential`, seed `dim_region` |
| 3 | `03_gold_to_sql.py` | `gold/*` | Azure SQL `staging.*` (JDBC, `truncate`) | — (couche serving) |

L'orchestration est assurée par le pipeline ADF `transform_databricks`, qui appelle ensuite la
procédure `marts.usp_refresh_serving`.

## Paramètres (widgets, passés par ADF en `baseParameters`)

| Notebook | Paramètres |
|---|---|
| 01, 02 | `storageAccountName` |
| 03 | `storageAccountName`, `sqlServerName`, `sqlDatabaseName`, `secretScope` |

Le notebook 03 lit `sql-user` et `sql-password` dans le secret scope. Aucun secret dans le code.

## Cluster recommandé

- **Job cluster single-node** `Standard_DS3_v2` (créé par ADF, détruit après le run), runtime
  LTS récent (15.4 LTS au moment de l'écriture), mode d'accès *single user* pour Unity Catalog.
- Volume d'environ 50 000 lignes de météo : un seul nœud suffit largement.
- Accès au lake : *external locations* Unity Catalog sur `bronze`, `silver` et `gold`
  (connecteur d'accès à identité managée). Chemins `abfss://<conteneur>@<compte>.dfs.core.windows.net`.
- Pilote JDBC SQL Server inclus dans le runtime Databricks.

## Garde-fous

- Le notebook 03 **refuse de charger** une table gold vide (levée d'exception avant tout
  `truncate`), et la procédure SQL refuse de swapper si le staging est vide.
- Silver : en cas de doublon (`region_code`, `observation_date`), la **dernière ingestion
  gagne**, comme l'upsert du pipeline local.
- Rapprochement agriculture : un libellé de région absent de `dim_region` est écarté
  silencieusement par la jointure (limite connue, voir `azure/README.md`).

## Pourquoi la duplication ?

> La logique métier (seuils, classes de sécheresse, score solaire) existe **deux fois** : en SQL
> dans les modèles dbt (pipeline local) et en PySpark dans `02_silver_to_gold.py`.
>
> **C'est un choix conscient, pas un oubli.**
> - **En production**, on n'écrirait pas deux fois la même logique : on utiliserait
>   **`dbt-databricks`** et les mêmes modèles dbt s'exécuteraient directement sur Databricks
>   (ou sur un SQL warehouse), avec un seul code, un seul jeu de tests.
> - **Ici**, la phase Azure est un exercice de démonstration : on écrit du **PySpark brut** pour
>   montrer la maîtrise de Spark (lecture de JSON imbriqué, `arrays_zip`/`explode`, fonctions de
>   fenêtre, jointures, écriture Parquet partitionnée, JDBC).
>
> **Mitigation du risque de dérive** : les seuils sont des constantes nommées dans le notebook,
> et `tests/test_azure_artifacts.py` vérifie qu'elles sont **identiques** aux littéraux du SQL
> dbt (jour sec < 1 mm, jour ensoleillé ≥ 5 kWh/m²/j, année complète ≥ 95 %, seuils d'aridité
> 0,20 / 0,50 / 0,65, bornes 3,0-7,0, pondération 0,7 / 0,3, conversion /3,6). Ce test ne
> garantit pas que les **calculs** produisent les mêmes résultats : seule une comparaison des
> sorties sur les mêmes données le ferait (non réalisée).

Tableau de correspondance complet : [docs/architecture.md](../../../docs/architecture.md).
