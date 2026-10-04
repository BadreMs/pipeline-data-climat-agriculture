# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver -> Gold
# MAGIC
# MAGIC Agregations annuelles et KPI par region. Equivalent PySpark des modeles dbt
# MAGIC `int_weather_agriculture_join`, `fct_region_climate_kpi` et `fct_solar_potential`
# MAGIC (memes seuils, memes formules : voir le tableau de correspondance dans
# MAGIC `docs/architecture.md`, verifie par `tests/test_azure_artifacts.py`).
# MAGIC
# MAGIC **Parametres** : `storageAccountName`.
# MAGIC
# MAGIC Code NON EXECUTE a ce jour (aucun cluster Databricks n'a ete provisionne).

# COMMAND ----------

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- Seuils metier : doivent rester identiques au SQL dbt (cf. test de coherence) ------------
DRY_DAY_THRESHOLD_MM = 1.0  # jour sec : precipitations < 1 mm (int_weather_agriculture_join)
SUNNY_DAY_THRESHOLD_KWH = 5.0  # jour ensoleille : >= 5 kWh/m2/j (int_weather_agriculture_join)
COMPLETE_YEAR_COVERAGE = 0.95  # annee complete : couverture >= 95 %
ARIDITY_THRESHOLDS = (0.20, 0.50, 0.65)  # P/ET0 : tres_sec | sec | normal | humide (UNEP)
MIN_SPI_REFERENCE_YEARS = 5  # SPI simplifie : au moins 5 annees completes de reference
SOLAR_SCORE_BOUNDS = (3.0, 7.0)  # kWh/m2/j -> score de rayonnement 0 .. 100
SOLAR_SCORE_WEIGHTS = (0.7, 0.3)  # (score de rayonnement, score de regularite)


def get_param(name: str, default: str = "") -> str:
    """Lit un widget Databricks (parametre passe par ADF) ; `default` si absent."""
    from pyspark.dbutils import DBUtils

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    dbutils.widgets.text(name, default)
    return str(dbutils.widgets.get(name))


def container_root(account: str, container: str) -> str:
    return f"abfss://{container}@{account}.dfs.core.windows.net"


def _count_if(condition: Column) -> Column:
    return F.sum(F.when(condition, 1).otherwise(0))


def build_region_year(weather: DataFrame, agriculture: DataFrame) -> DataFrame:
    """1 ligne = 1 region x 1 annee de meteo, avec agriculture en LEFT JOIN."""
    water = F.col("has_water_balance_data")
    yearly = weather.groupBy("region_code", F.year("observation_date").alias("year")).agg(
        F.count(F.lit(1)).alias("days_observed"),
        _count_if(water).alias("water_balance_days"),
        F.sum(F.when(water, F.col("precipitation_mm"))).alias("precipitation_total_mm"),
        F.sum(F.when(water, F.col("et0_mm"))).alias("et0_total_mm"),
        _count_if(water & (F.col("precipitation_mm") < DRY_DAY_THRESHOLD_MM)).alias("dry_days"),
        _count_if(F.col("has_solar_data")).alias("solar_days"),
        F.avg("solar_radiation_kwh_m2").alias("solar_radiation_avg_kwh_m2_day"),
        F.sum("solar_radiation_kwh_m2").alias("solar_radiation_total_kwh_m2"),
        _count_if(F.col("solar_radiation_kwh_m2") >= SUNNY_DAY_THRESHOLD_KWH).alias("sunny_days"),
    )
    days_in_year = (
        F.datediff(
            F.make_date(F.col("year"), F.lit(12), F.lit(31)),
            F.make_date(F.col("year"), F.lit(1), F.lit(1)),
        )
        + 1
    )
    with_coverage = (
        yearly.withColumn("days_in_year", days_in_year)
        .withColumn(
            "water_balance_coverage",
            F.round(F.col("water_balance_days") / F.col("days_in_year"), 3),
        )
        .withColumn("solar_coverage", F.round(F.col("solar_days") / F.col("days_in_year"), 3))
    )
    agri = agriculture.select(
        "region_code",
        "year",
        F.lit(True).alias("has_agriculture_data"),
        "production_tonnes",
        "irrigated_area_ha",
        "water_resource_m3",
        F.col("data_source").alias("agriculture_source"),
        F.col("is_mock").alias("agriculture_is_mock"),
    )
    return with_coverage.join(agri, on=["region_code", "year"], how="left").withColumn(
        "has_agriculture_data", F.coalesce(F.col("has_agriculture_data"), F.lit(False))
    )


def build_climate_kpi(region_year: DataFrame, dim_region: DataFrame) -> DataFrame:
    """KPI secheresse par region x annee (classes, SPI : annees completes uniquement)."""
    very_dry, dry, normal = ARIDITY_THRESHOLDS
    base = (
        region_year.join(dim_region.select("region_code", "region_name"), on="region_code")
        .withColumn(
            "is_complete_year", F.col("water_balance_coverage") >= F.lit(COMPLETE_YEAR_COVERAGE)
        )
        .withColumn(
            "precip_et0_ratio",
            F.when(
                F.col("et0_total_mm") > 0, F.col("precipitation_total_mm") / F.col("et0_total_mm")
            ),
        )
    )
    by_region = Window.partitionBy("region_code")
    complete_precip = F.when(F.col("is_complete_year"), F.col("precipitation_total_mm"))
    with_reference = (
        base.withColumn("precip_ref_mean_mm", F.avg(complete_precip).over(by_region))
        .withColumn("precip_ref_std_mm", F.stddev_samp(complete_precip).over(by_region))
        .withColumn("precip_ref_years", _count_if(F.col("is_complete_year")).over(by_region))
    )
    ratio = F.col("precip_et0_ratio")
    drought_class = (
        F.when(~F.col("is_complete_year") | ratio.isNull(), F.lit(None).cast("string"))
        .when(ratio < very_dry, "tres_sec")
        .when(ratio < dry, "sec")
        .when(ratio < normal, "normal")
        .otherwise("humide")
    )
    spi = F.when(
        F.col("is_complete_year")
        & (F.col("precip_ref_years") >= MIN_SPI_REFERENCE_YEARS)
        & (F.col("precip_ref_std_mm") > 0),
        F.round(
            (F.col("precipitation_total_mm") - F.col("precip_ref_mean_mm"))
            / F.col("precip_ref_std_mm"),
            2,
        ),
    )
    return with_reference.select(
        "region_code",
        "region_name",
        "year",
        "is_complete_year",
        "water_balance_days",
        "water_balance_coverage",
        F.round("precipitation_total_mm", 1).alias("precipitation_total_mm"),
        F.round("et0_total_mm", 1).alias("et0_total_mm"),
        F.round(ratio, 3).alias("precip_et0_ratio"),
        drought_class.alias("drought_class"),
        "dry_days",
        F.round(
            F.col("dry_days")
            / F.when(F.col("water_balance_days") != 0, F.col("water_balance_days")),
            3,
        ).alias("dry_days_ratio"),
        spi.alias("spi_simplified"),
        "has_agriculture_data",
        "production_tonnes",
        "irrigated_area_ha",
        "water_resource_m3",
        "agriculture_source",
        "agriculture_is_mock",
    )


def build_solar_potential(region_year: DataFrame, dim_region: DataFrame) -> DataFrame:
    """Potentiel solaire par region x annee : 0.7 x rayonnement + 0.3 x regularite (0-100)."""
    low, high = SOLAR_SCORE_BOUNDS
    weight_radiation, weight_regularity = SOLAR_SCORE_WEIGHTS
    base = (
        region_year.join(
            dim_region.select("region_code", "region_name", "latitude", "longitude", "altitude_m"),
            on="region_code",
        )
        .withColumn("is_complete_year", F.col("solar_coverage") >= F.lit(COMPLETE_YEAR_COVERAGE))
        .withColumn(
            "sunny_days_pct",
            F.round(
                F.lit(100.0)
                * F.col("sunny_days")
                / F.when(F.col("solar_days") != 0, F.col("solar_days")),
                1,
            ),
        )
        .withColumn(
            "radiation_score_raw",
            F.round(
                F.lit(100)
                * F.least(
                    F.greatest(
                        (F.col("solar_radiation_avg_kwh_m2_day") - low) / (high - low), F.lit(0)
                    ),
                    F.lit(1),
                ),
                1,
            ),
        )
    )
    complete = F.col("is_complete_year")
    return base.select(
        "region_code",
        "region_name",
        "latitude",
        "longitude",
        "altitude_m",
        "year",
        "is_complete_year",
        "solar_days",
        "solar_coverage",
        F.round("solar_radiation_avg_kwh_m2_day", 3).alias("solar_radiation_avg_kwh_m2_day"),
        F.round("solar_radiation_total_kwh_m2", 1).alias("solar_radiation_total_kwh_m2"),
        "sunny_days",
        "sunny_days_pct",
        F.when(complete, F.col("radiation_score_raw")).alias("radiation_score"),
        F.when(complete, F.col("sunny_days_pct")).alias("regularity_score"),
        F.when(
            complete,
            F.round(
                weight_radiation * F.col("radiation_score_raw")
                + weight_regularity * F.col("sunny_days_pct"),
                1,
            ),
        ).alias("solar_potential_score"),
    )


def build_dim_year(region_year: DataFrame) -> DataFrame:
    """Une ligne par annee ; complete seulement si TOUTES les regions le sont (climat, solaire)."""
    complete = (F.col("water_balance_coverage") >= COMPLETE_YEAR_COVERAGE) & (
        F.col("solar_coverage") >= COMPLETE_YEAR_COVERAGE
    )
    return (
        region_year.groupBy("year")
        .agg((F.min(F.when(complete, 1).otherwise(0)) == 1).alias("is_complete_year"))
        .withColumn(
            "year_label",
            F.when(F.col("is_complete_year"), F.col("year").cast("string")).otherwise(
                F.concat(F.col("year").cast("string"), F.lit(" (partielle)"))
            ),
        )
    )


def main() -> None:
    account = get_param("storageAccountName")
    if not account:
        raise ValueError("Parametre storageAccountName obligatoire")
    spark = SparkSession.builder.getOrCreate()
    bronze = container_root(account, "bronze")
    silver = container_root(account, "silver")
    gold = container_root(account, "gold")

    weather = spark.read.parquet(f"{silver}/weather_daily")
    agriculture = spark.read.parquet(f"{silver}/agriculture_regional")
    dim_region = (
        spark.read.option("header", True)
        .csv(f"{bronze}/reference/dim_region.csv")
        .select(
            "region_code",
            "region_name",
            "capital",
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
            F.col("altitude_m").cast("double").alias("altitude_m"),
        )
    )

    region_year = build_region_year(weather, agriculture).cache()
    outputs = {
        "dim_region": dim_region,
        "dim_year": build_dim_year(region_year),
        "fct_region_climate_kpi": build_climate_kpi(region_year, dim_region),
        "fct_solar_potential": build_solar_potential(region_year, dim_region),
    }
    for name, frame in outputs.items():
        frame.write.mode("overwrite").parquet(f"{gold}/{name}")
        print(f"gold/{name} : {frame.count()} lignes")


# COMMAND ----------

if __name__ == "__main__":
    main()
