# Azure SQL Database — couche de serving

> DDL jamais exécuté sur Azure SQL à ce jour. Seules les tables attendues dans
> `02_create_serving_tables.sql` sont vérifiées automatiquement (`tests/test_azure_artifacts.py`).

## Scripts

| Script | Contenu |
|---|---|
| `01_create_schemas.sql` | Schémas `staging`, `intermediate` (réservé, reflète dbt, non utilisé) et `marts` |
| `02_create_serving_tables.sql` | Schéma en étoile dans `marts`, copies `staging`, procédure `marts.usp_refresh_serving`, vue `marts.vw_region_year_kpi`, rôle `powerbi_reader` |

Les deux scripts sont idempotents. À exécuter dans l'ordre, connecté à la **base** (pas à
`master`), par exemple `sqlcmd -S <serveur>.database.windows.net -d <base> -G -i 01_create_schemas.sql`.
Les séparateurs `GO` sont requis (`CREATE OR ALTER PROCEDURE/VIEW` doivent être seuls dans leur lot).

## Modèle en étoile

```
dim_region (region_code PK)         dim_year (year PK)
        \                             /
         fct_region_climate_kpi (region_code, year)
         fct_solar_potential    (region_code, year)
```

- Grain des deux faits : **1 ligne = 1 région × 1 année**, mêmes colonnes que les marts dbt (sauf
  `region_name` et les coordonnées, portés par `dim_region`).
- `drought_class` est nul pour les années incomplètes, comme les scores et le SPI : voir
  `dim_year.is_complete_year` / `year_label` (`2026 (partielle)`).
- `agriculture_is_mock = 1` signale des colonnes agricoles **synthétiques** : à afficher
  explicitement dans Power BI.

## Chargement

1. `03_gold_to_sql.py` écrit `staging.*` (JDBC, `truncate` : la structure est conservée).
2. L'activité ADF `RefreshServingTables` appelle `marts.usp_refresh_serving` : une seule
   transaction vide les faits puis les dimensions et les recharge depuis `staging`. Les lecteurs
   ne voient jamais de tables vides, et la procédure lève une erreur (`THROW 50001`) si le
   staging est vide.

Droits à accorder (hors scripts, car dépendants de l'environnement) :

```sql
CREATE USER [<nom-de-la-data-factory>] FROM EXTERNAL PROVIDER;   -- identité managée d'ADF
GRANT EXECUTE ON OBJECT::marts.usp_refresh_serving TO [<nom-de-la-data-factory>];
```

Le login SQL lu par Databricks (secrets `sql-user` / `sql-password`) a besoin de
`INSERT`, `SELECT` et `ALTER` (pour `TRUNCATE`) sur le schéma `staging`. Hardening possible :
remplacer ce login par un principal Entra ID.

## Intégration Power BI

- **Source** : la vue `marts.vw_region_year_kpi` (une ligne par région × année, libellés français
  et colonne de tri `drought_class_sort`), ou les tables du schéma en étoile pour un modèle
  sémantique avec relations `dim_region` 1—* faits et `dim_year` 1—* faits.
- **Mode Import** recommandé (volume ≈ 144 lignes par fait) ; rafraîchissement planifié après la
  fin du pipeline (~05:00). Le rafraîchissement réveille la base serverless (pause
  automatique après 60 min d'inactivité).
- **Compte de lecture** : membre du rôle `powerbi_reader` (lecture seule sur `marts`).
- **Pistes de mesures** : score solaire moyen par région, part d'années `tres_sec`, évolution du
  SPI. Filtrer sur `is_complete_year` pour ne comparer que des années complètes.
- **Avertissement** : afficher un bandeau quand `agriculture_is_mock = 1`.
