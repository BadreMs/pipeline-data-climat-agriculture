# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold -> Azure SQL (serving)
# MAGIC
# MAGIC Charge les tables gold dans les tables `staging.*` d'Azure SQL DB (JDBC, `truncate`).
# MAGIC Le swap vers `marts.*` est fait ensuite par la procedure `marts.usp_refresh_serving`
# MAGIC (activite ADF suivante) : transaction unique, Power BI ne voit jamais de table vide.
# MAGIC
# MAGIC **Parametres** : `storageAccountName`, `sqlServerName`, `sqlDatabaseName`, `secretScope`
# MAGIC (scope Key Vault avec les secrets `sql-user` et `sql-password`).
# MAGIC
# MAGIC Code NON EXECUTE a ce jour (aucun cluster Databricks n'a ete provisionne).

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession

# Colonnes chargees par table : schema en etoile (cf. azure/sql/02_create_serving_tables.sql).
# Les attributs descriptifs de region (nom, coordonnees) vivent dans dim_region seulement.
SERVING_COLUMNS: dict[str, list[str]] = {
    "dim_region": ["region_code", "region_name", "capital", "latitude", "longitude", "altitude_m"],
    "dim_year": ["year", "is_complete_year", "year_label"],
    "fct_region_climate_kpi": [
        "region_code",
        "year",
        "is_complete_year",
        "water_balance_days",
        "water_balance_coverage",
        "precipitation_total_mm",
        "et0_total_mm",
        "precip_et0_ratio",
        "drought_class",
        "dry_days",
        "dry_days_ratio",
        "spi_simplified",
        "has_agriculture_data",
        "production_tonnes",
        "irrigated_area_ha",
        "water_resource_m3",
        "agriculture_source",
        "agriculture_is_mock",
    ],
    "fct_solar_potential": [
        "region_code",
        "year",
        "is_complete_year",
        "solar_days",
        "solar_coverage",
        "solar_radiation_avg_kwh_m2_day",
        "solar_radiation_total_kwh_m2",
        "sunny_days",
        "sunny_days_pct",
        "radiation_score",
        "regularity_score",
        "solar_potential_score",
    ],
}

JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"


def get_param(name: str, default: str = "") -> str:
    """Lit un widget Databricks (parametre passe par ADF) ; `default` si absent."""
    from pyspark.dbutils import DBUtils

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    dbutils.widgets.text(name, default)
    return str(dbutils.widgets.get(name))


def get_secret(scope: str, key: str) -> str:
    from pyspark.dbutils import DBUtils

    return str(DBUtils(SparkSession.builder.getOrCreate()).secrets.get(scope, key))


def jdbc_url(server: str, database: str) -> str:
    return (
        f"jdbc:sqlserver://{server}.database.windows.net:1433;database={database};"
        "encrypt=true;trustServerCertificate=false;"
        "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
    )


def write_staging(frame: DataFrame, url: str, user: str, password: str, table: str) -> int:
    """Remplace le contenu de staging.<table> (schema conserve grace a `truncate`)."""
    selected = frame.select(*SERVING_COLUMNS[table])
    rows = int(selected.count())
    if rows == 0:
        # Garde-fou : ne jamais vider le serving avec un gold vide.
        raise ValueError(f"gold/{table} est vide : chargement SQL annule")
    (
        selected.write.format("jdbc")
        .option("url", url)
        .option("dbtable", f"staging.{table}")
        .option("user", user)
        .option("password", password)
        .option("driver", JDBC_DRIVER)
        .option("truncate", "true")
        .option("batchsize", 5000)
        .mode("overwrite")
        .save()
    )
    return rows


def main() -> None:
    account = get_param("storageAccountName")
    server = get_param("sqlServerName")
    database = get_param("sqlDatabaseName")
    scope = get_param("secretScope")
    if not all((account, server, database, scope)):
        raise ValueError("storageAccountName, sqlServerName, sqlDatabaseName, secretScope requis")

    spark = SparkSession.builder.getOrCreate()
    gold = f"abfss://gold@{account}.dfs.core.windows.net"
    url = jdbc_url(server, database)
    user = get_secret(scope, "sql-user")
    password = get_secret(scope, "sql-password")

    for table in SERVING_COLUMNS:
        rows = write_staging(spark.read.parquet(f"{gold}/{table}"), url, user, password, table)
        print(f"staging.{table} : {rows} lignes")


# COMMAND ----------

if __name__ == "__main__":
    main()
