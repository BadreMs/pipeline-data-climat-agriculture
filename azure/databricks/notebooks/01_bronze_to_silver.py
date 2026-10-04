# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze -> Silver
# MAGIC
# MAGIC Lit le bronze ADLS Gen2 (JSON Open-Meteo, CSV agriculture), type et nettoie, puis ecrit
# MAGIC le silver en Parquet. Equivalent PySpark des modeles dbt `stg_weather_daily` et
# MAGIC `stg_agriculture_regional`.
# MAGIC
# MAGIC **Parametres (widgets / baseParameters ADF)** : `storageAccountName`.
# MAGIC
# MAGIC Code NON EXECUTE a ce jour (aucun cluster Databricks n'a ete provisionne).

# COMMAND ----------

import re
import unicodedata

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Variables journalieres demandees a Open-Meteo (identiques a ingestion/openmeteo_client.py).
DAILY_VARIABLES = (
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "et0_fao_evapotranspiration",
    "shortwave_radiation_sum",
    "relative_humidity_2m_mean",
    "wind_speed_10m_max",
)

# Formule solaire : kWh/m2 = MJ/m2 / 3.6 (1 kWh = 3.6 MJ), identique a stg_weather_daily.sql.
MJ_PER_KWH = 3.6


def get_param(name: str, default: str = "") -> str:
    """Lit un widget Databricks (parametre passe par ADF) ; `default` si absent."""
    from pyspark.dbutils import DBUtils

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    dbutils.widgets.text(name, default)
    return str(dbutils.widgets.get(name))


def container_root(account: str, container: str) -> str:
    return f"abfss://{container}@{account}.dfs.core.windows.net"


def normalize_label(label: str) -> str:
    """'Béni Mellal-Khénifra' -> 'beni mellal khenifra' (cle de rapprochement des libelles)."""
    decomposed = unicodedata.normalize("NFKD", label)
    without_accents = "".join(char for char in decomposed if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", " ", without_accents.lower()).strip()


def build_silver_weather(spark: SparkSession, bronze: str) -> DataFrame:
    """1 ligne = 1 region x 1 jour. Derniere ingestion gagnante en cas de doublon."""
    base = f"{bronze}/openmeteo"
    raw = (
        spark.read.option("multiLine", True)
        .option("basePath", base)
        .json(f"{base}/ingest_date=*/region_code=*/data.json")
    )
    # L'API renvoie des tableaux paralleles sous `daily` : on les zippe puis on explose.
    zipped = F.arrays_zip(
        *[F.col(f"daily.{name}").alias(name) for name in ("time", *DAILY_VARIABLES)]
    )
    rows = raw.select("region_code", "ingest_date", F.explode(zipped).alias("d"))

    def num(name: str) -> "F.Column":
        return F.col(f"d.{name}").cast("double")

    typed = rows.select(
        "region_code",
        F.to_date(F.col("d.time")).alias("observation_date"),
        num("temperature_2m_max").alias("temp_max_c"),
        num("temperature_2m_min").alias("temp_min_c"),
        num("temperature_2m_mean").alias("temp_mean_c"),
        num("precipitation_sum").alias("precipitation_mm"),
        num("et0_fao_evapotranspiration").alias("et0_mm"),
        num("shortwave_radiation_sum").alias("solar_radiation_mj_m2"),
        F.round(num("shortwave_radiation_sum") / F.lit(MJ_PER_KWH), 3).alias(
            "solar_radiation_kwh_m2"
        ),
        num("relative_humidity_2m_mean").alias("humidity_mean_pct"),
        num("wind_speed_10m_max").alias("wind_max_kmh"),
        F.to_timestamp(F.col("ingest_date")).alias("ingested_at"),
    )
    flagged = typed.withColumn(
        "has_water_balance_data",
        F.col("precipitation_mm").isNotNull() & F.col("et0_mm").isNotNull(),
    ).withColumn("has_solar_data", F.col("solar_radiation_mj_m2").isNotNull())

    latest_first = Window.partitionBy("region_code", "observation_date").orderBy(
        F.col("ingested_at").desc()
    )
    return (
        flagged.withColumn("rank", F.row_number().over(latest_first))
        .filter(F.col("rank") == 1)
        .drop("rank")
        .withColumn("year", F.year("observation_date"))
    )


def build_silver_agriculture(spark: SparkSession, bronze: str) -> DataFrame:
    """1 ligne = 1 region x 1 annee. Les libelles region sont rapproches de dim_region."""
    base = f"{bronze}/agriculture"
    raw = (
        spark.read.option("header", True)
        .option("encoding", "UTF-8")
        .option("basePath", base)
        .csv(f"{base}/ingest_date=*/source=*/agriculture_regional.csv")
    )
    normalize = F.udf(normalize_label, "string")
    regions = (
        spark.read.option("header", True)
        .csv(f"{bronze}/reference/dim_region.csv")
        .select("region_code", normalize(F.col("region_name")).alias("region_key"))
    )
    with_code = raw.withColumn("region_key", normalize(F.col("region"))).join(
        regions, on="region_key", how="inner"
    )
    latest_first = Window.partitionBy("region_code", "annee").orderBy(F.col("ingest_date").desc())
    return (
        with_code.withColumn("rank", F.row_number().over(latest_first))
        .filter(F.col("rank") == 1)
        .select(
            "region_code",
            F.col("annee").cast("int").alias("year"),
            F.col("production_tonnes").cast("double").alias("production_tonnes"),
            F.col("surface_irriguee_ha").cast("double").alias("irrigated_area_ha"),
            F.col("ressource_hydrique_m3").cast("double").alias("water_resource_m3"),
            F.col("source").alias("data_source"),
            F.col("source").startswith("mock").alias("is_mock"),
            F.to_timestamp(F.col("ingest_date")).alias("ingested_at"),
        )
    )


def main() -> None:
    account = get_param("storageAccountName")
    if not account:
        raise ValueError("Parametre storageAccountName obligatoire")
    spark = SparkSession.builder.getOrCreate()
    # Garde region_code / ingest_date / source en texte (pas d'inference de type de partition).
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

    bronze = container_root(account, "bronze")
    silver = container_root(account, "silver")

    weather = build_silver_weather(spark, bronze)
    weather.write.mode("overwrite").partitionBy("year").parquet(f"{silver}/weather_daily")
    print(f"silver/weather_daily : {weather.count()} lignes")

    agriculture = build_silver_agriculture(spark, bronze)
    agriculture.write.mode("overwrite").parquet(f"{silver}/agriculture_regional")
    print(f"silver/agriculture_regional : {agriculture.count()} lignes")


# COMMAND ----------

if __name__ == "__main__":
    main()
