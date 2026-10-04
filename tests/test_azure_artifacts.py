"""Coherence des artefacts Azure (azure/) : JSON ADF, DDL T-SQL, notebooks, scripts shell.

Aucun de ces artefacts n'est execute (pas d'abonnement Azure, pas de cluster, pas de pyspark) :
ces tests verifient ce qui est verifiable localement. Les seuils metier des notebooks sont
compares aux litteraux du SQL dbt (duplication assumee, cf. azure/databricks/notebooks/README.md).
"""

import csv
import importlib.util
import json
import os
import re
import shutil
import stat
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch

from ingestion.regions import REGIONS

ROOT = Path(__file__).resolve().parent.parent
AZURE = ROOT / "azure"
ADF = AZURE / "data_factory"
NOTEBOOKS = AZURE / "databricks" / "notebooks"
SQL_DIR = AZURE / "sql"
SCRIPTS = AZURE / "scripts"
DBT_MODELS = ROOT / "dbt_project" / "models"

ADF_KINDS = {
    "linked_services": "LinkedServiceReference",
    "datasets": "DatasetReference",
    "pipelines": "PipelineReference",
}
BASH_EXE = shutil.which("bash")


def _adf_files() -> list[Path]:
    return sorted(ADF.rglob("*.json"))


def _load(path: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return loaded


def _names(folder: str) -> set[str]:
    return {path.stem for path in (ADF / folder).glob("*.json")}


def _walk(node: Any) -> Iterator[Any]:
    yield node
    if isinstance(node, dict):
        for value in node.values():
            yield from _walk(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


# --- Data Factory ---------------------------------------------------------------------------


def test_adf_resources_exist() -> None:
    assert {"ingest_openmeteo", "ingest_agriculture", "transform_databricks"} <= _names("pipelines")
    assert len(_names("triggers")) == 3


@pytest.mark.parametrize("path", _adf_files(), ids=lambda p: p.stem)
def test_adf_json_is_valid_and_named_after_its_file(path: Path) -> None:
    resource = _load(path)
    assert resource["name"] == path.stem
    assert isinstance(resource["properties"], dict)


def test_adf_references_point_to_existing_resources() -> None:
    known = {kind: _names(folder) for folder, kind in ADF_KINDS.items()}
    for path in _adf_files():
        for node in _walk(_load(path)):
            if isinstance(node, dict) and "referenceName" in node:
                kind = node["type"]
                assert node["referenceName"] in known[kind], f"{path.name}: {node}"


def test_adf_expressions_use_declared_parameters_and_variables() -> None:
    for path in (ADF / "pipelines").glob("*.json"):
        properties = _load(path)["properties"]
        text = json.dumps(properties)
        declared_parameters = set(properties.get("parameters", {}))
        declared_variables = set(properties.get("variables", {}))
        assert set(re.findall(r"pipeline\(\)\.parameters\.(\w+)", text)) <= declared_parameters
        assert set(re.findall(r"variables\('(\w+)'\)", text)) <= declared_variables
    for path in (ADF / "datasets").glob("*.json"):
        properties = _load(path)["properties"]
        used = set(re.findall(r"dataset\(\)\.(\w+)", json.dumps(properties)))
        assert used <= set(properties.get("parameters", {})), path.name
    properties = _load(ADF / "linked_services" / "ls_http_datagovma.json")["properties"]
    used = set(re.findall(r"linkedService\(\)\.(\w+)", json.dumps(properties)))
    assert used <= set(properties["parameters"])


def test_adf_openmeteo_regions_match_regions_py() -> None:
    pipeline = _load(ADF / "pipelines" / "ingest_openmeteo.json")["properties"]
    declared = pipeline["parameters"]["regions"]["defaultValue"]
    expected = [
        {"code": r.iso_code, "latitude": r.latitude, "longitude": r.longitude} for r in REGIONS
    ]
    assert declared == expected


def test_adf_openmeteo_requests_same_variables_as_local_client() -> None:
    from ingestion.openmeteo_client import DAILY_VARIABLES, TIMEZONE

    dataset = _load(ADF / "datasets" / "ds_openmeteo_archive.json")["properties"]
    url = dataset["typeProperties"]["relativeUrl"]
    for variable in DAILY_VARIABLES:
        assert variable in url
    assert f"timezone={TIMEZONE}" in url


def test_adf_triggers_start_stopped_and_target_known_pipelines() -> None:
    for path in (ADF / "triggers").glob("*.json"):
        properties = _load(path)["properties"]
        assert properties["runtimeState"] == "Stopped"
        assert properties["typeProperties"]["recurrence"]["timeZone"] == "Morocco Standard Time"
        for target in properties["pipelines"]:
            assert target["pipelineReference"]["referenceName"] in _names("pipelines")


def test_adf_placeholders_are_all_substituted_by_deploy_script() -> None:
    placeholders: set[str] = set()
    for path in _adf_files():
        placeholders |= set(re.findall(r"\$\{(\w+)\}", _read(path)))
    match = re.search(r"^SUBSTITUTIONS='([^']*)'", _read(SCRIPTS / "deploy.sh"), re.MULTILINE)
    assert match is not None
    substituted = set(re.findall(r"\$\{(\w+)\}", match.group(1)))
    assert placeholders, "aucun placeholder trouve : test sans objet"
    assert placeholders <= substituted


def test_adf_has_no_embedded_secrets() -> None:
    text = "\n".join(_read(path).lower() for path in _adf_files())
    for forbidden in ("password=", "accountkey", "sas_token", "client_secret", "sig="):
        assert forbidden not in text


# --- Azure SQL ------------------------------------------------------------------------------


def _create_table_columns(sql: str, table: str) -> list[str]:
    block = re.search(rf"CREATE TABLE {re.escape(table)} \((.*?)\n\);", sql, re.DOTALL)
    assert block is not None, table
    columns: list[str] = []
    for raw_line in block.group(1).splitlines():
        line = raw_line.split("--")[0].strip()
        if not line:
            continue
        if line.upper().startswith("CONSTRAINT"):
            break  # les contraintes de table suivent toujours les colonnes
        columns.append(line.split()[0].strip("[]"))
    return columns


SERVING_TABLES = {"dim_region", "dim_year", "fct_region_climate_kpi", "fct_solar_potential"}


def test_sql_creates_the_three_schemas() -> None:
    sql = _read(SQL_DIR / "01_create_schemas.sql")
    for schema in ("staging", "intermediate", "marts"):
        assert f"CREATE SCHEMA {schema}" in sql


def test_sql_serving_tables_and_staging_copies() -> None:
    sql = _read(SQL_DIR / "02_create_serving_tables.sql")
    assert set(re.findall(r"CREATE TABLE marts\.(\w+)", sql)) == SERVING_TABLES
    assert set(re.findall(r"SELECT \* INTO staging\.(\w+) FROM marts\.", sql)) == SERVING_TABLES
    assert "CREATE OR ALTER PROCEDURE marts.usp_refresh_serving" in sql
    assert "CREATE OR ALTER VIEW marts.vw_region_year_kpi" in sql


def test_sql_refresh_procedure_is_the_one_called_by_adf() -> None:
    pipeline = _load(ADF / "pipelines" / "transform_databricks.json")["properties"]
    procedures = [
        a["typeProperties"]["storedProcedureName"]
        for a in pipeline["activities"]
        if a["type"] == "SqlServerStoredProcedure"
    ]
    assert procedures == ["[marts].[usp_refresh_serving]"]


def test_sql_fact_columns_exist_in_dbt_marts() -> None:
    sql = _read(SQL_DIR / "02_create_serving_tables.sql")
    for table in ("fct_region_climate_kpi", "fct_solar_potential"):
        dbt_sql = _read(DBT_MODELS / "marts" / f"{table}.sql")
        for column in _create_table_columns(sql, f"marts.{table}"):
            assert re.search(rf"\b{column}\b", dbt_sql), f"{table}.{column} absent du modele dbt"


# --- Notebooks ------------------------------------------------------------------------------


def _load_notebook(name: str, monkeypatch: MonkeyPatch) -> ModuleType:
    """Importe un notebook avec un faux pyspark (tout est execute dans main(), pas a l'import)."""
    for module_name in (
        "pyspark",
        "pyspark.sql",
        "pyspark.sql.functions",
        "pyspark.sql.window",
        "pyspark.dbutils",
    ):
        monkeypatch.setitem(sys.modules, module_name, MagicMock())
    spec = importlib.util.spec_from_file_location(f"notebook_{name}", NOTEBOOKS / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


NOTEBOOK_NAMES = ["01_bronze_to_silver", "02_silver_to_gold", "03_gold_to_sql"]


@pytest.mark.parametrize("name", NOTEBOOK_NAMES)
def test_notebook_has_databricks_header_and_guarded_main(name: str) -> None:
    text = _read(NOTEBOOKS / f"{name}.py")
    assert text.splitlines()[0] == "# Databricks notebook source"
    assert 'if __name__ == "__main__":' in text
    assert "# COMMAND ----------" in text


def _float(pattern: str, text: str) -> float:
    match = re.search(pattern, text)
    assert match is not None, pattern
    return float(match.group(1))


def test_gold_notebook_thresholds_match_dbt_sql(monkeypatch: MonkeyPatch) -> None:
    gold = _load_notebook("02_silver_to_gold", monkeypatch)
    intermediate = _read(DBT_MODELS / "intermediate" / "int_weather_agriculture_join.sql")
    climate = _read(DBT_MODELS / "marts" / "fct_region_climate_kpi.sql")
    solar = _read(DBT_MODELS / "marts" / "fct_solar_potential.sql")

    assert _float(r"precipitation_mm < ([\d.]+)", intermediate) == gold.DRY_DAY_THRESHOLD_MM
    assert (
        _float(r"solar_radiation_kwh_m2 >= ([\d.]+)", intermediate) == gold.SUNNY_DAY_THRESHOLD_KWH
    )
    assert _float(r"water_balance_coverage >= ([\d.]+)", climate) == gold.COMPLETE_YEAR_COVERAGE
    assert _float(r"solar_coverage >= ([\d.]+)", solar) == gold.COMPLETE_YEAR_COVERAGE
    assert (
        tuple(float(value) for value in re.findall(r"precip_et0_ratio < ([\d.]+)", climate))
        == gold.ARIDITY_THRESHOLDS
    )
    assert int(_float(r"precip_ref_years >= (\d+)", climate)) == gold.MIN_SPI_REFERENCE_YEARS

    bounds = re.search(
        r"\(solar_radiation_avg_kwh_m2_day - ([\d.]+)\) / \(([\d.]+) - ([\d.]+)\)", solar
    )
    assert bounds is not None
    low, high, low_again = (float(value) for value in bounds.groups())
    assert low == low_again
    assert (low, high) == gold.SOLAR_SCORE_BOUNDS

    weights = re.search(r"([\d.]+) \* radiation_score \+ ([\d.]+) \* regularity_score", solar)
    assert weights is not None
    assert tuple(float(value) for value in weights.groups()) == gold.SOLAR_SCORE_WEIGHTS
    assert sum(gold.SOLAR_SCORE_WEIGHTS) == pytest.approx(1.0)


def test_silver_notebook_uses_the_same_solar_conversion_as_dbt(monkeypatch: MonkeyPatch) -> None:
    silver = _load_notebook("01_bronze_to_silver", monkeypatch)
    staging = _read(DBT_MODELS / "staging" / "stg_weather_daily.sql")
    assert _float(r"shortwave_radiation_sum / ([\d.]+)", staging) == silver.MJ_PER_KWH


def test_silver_notebook_requests_the_same_daily_variables(monkeypatch: MonkeyPatch) -> None:
    from ingestion.openmeteo_client import DAILY_VARIABLES

    silver = _load_notebook("01_bronze_to_silver", monkeypatch)
    assert tuple(silver.DAILY_VARIABLES) == tuple(DAILY_VARIABLES)


def test_silver_label_normalization_resolves_every_mock_agriculture_row(
    monkeypatch: MonkeyPatch,
) -> None:
    silver = _load_notebook("01_bronze_to_silver", monkeypatch)
    assert silver.normalize_label("Béni Mellal-Khénifra") == "beni mellal khenifra"
    by_key = {silver.normalize_label(r.name): r.iso_code for r in REGIONS}
    assert len(by_key) == len(REGIONS), "libelles normalises ambigus"
    fixture = ROOT / "ingestion" / "fixtures" / "agriculture_maroc_mock.csv"
    with fixture.open(encoding="utf-8", newline="") as handle:
        labels = {row["region"] for row in csv.DictReader(handle)}
    assert {by_key[silver.normalize_label(label)] for label in labels} == {
        r.iso_code for r in REGIONS
    }


def test_sql_notebook_columns_match_the_serving_ddl(monkeypatch: MonkeyPatch) -> None:
    to_sql = _load_notebook("03_gold_to_sql", monkeypatch)
    sql = _read(SQL_DIR / "02_create_serving_tables.sql")
    assert set(to_sql.SERVING_COLUMNS) == SERVING_TABLES
    for table, columns in to_sql.SERVING_COLUMNS.items():
        assert columns == _create_table_columns(sql, f"marts.{table}"), table


# --- Scripts shell --------------------------------------------------------------------------

pytestmark_bash = pytest.mark.skipif(BASH_EXE is None, reason="bash introuvable sur le PATH")


@pytestmark_bash
@pytest.mark.parametrize("script", ["deploy.sh", "teardown.sh"])
def test_shell_scripts_have_valid_syntax(script: str) -> None:
    assert BASH_EXE is not None
    result = subprocess.run(
        [BASH_EXE, "-n", str(SCRIPTS / script)], capture_output=True, text=True, check=False
    )
    assert result.returncode == 0, result.stderr


@pytestmark_bash
@pytest.mark.parametrize("script", ["deploy.sh", "teardown.sh"])
def test_shell_scripts_never_call_az_without_apply(script: str, tmp_path: Path) -> None:
    """Mode simulation par defaut : un faux `az` laisserait une trace s'il etait appele."""
    assert BASH_EXE is not None
    marker = tmp_path / "az_was_called"
    fake_bin = tmp_path / "fakebin"
    fake_bin.mkdir()
    fake_az = fake_bin / "az"
    fake_az.write_text(f'#!/usr/bin/env bash\ntouch "{marker.as_posix()}"\nexit 1\n')
    fake_az.chmod(fake_az.stat().st_mode | stat.S_IEXEC)
    env = dict(os.environ)
    env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
    env.update({"SUBSCRIPTION_ID": "test-subscription", "UNIQUE_SUFFIX": "abc123"})
    env.pop("SQL_ADMIN_PASSWORD", None)

    result = subprocess.run(
        [BASH_EXE, str(SCRIPTS / script)], capture_output=True, text=True, env=env, check=False
    )

    assert result.returncode == 0, result.stderr
    assert "[simulation]" in result.stdout
    assert not marker.exists(), "az a ete appele sans --apply"


@pytestmark_bash
def test_deploy_script_requires_mandatory_variables() -> None:
    assert BASH_EXE is not None
    env = {k: v for k, v in os.environ.items() if k not in ("SUBSCRIPTION_ID", "UNIQUE_SUFFIX")}
    result = subprocess.run(
        [BASH_EXE, str(SCRIPTS / "deploy.sh")], capture_output=True, text=True, env=env, check=False
    )
    assert result.returncode != 0
    assert "SUBSCRIPTION_ID" in result.stderr


def test_deploy_script_has_no_hardcoded_credentials() -> None:
    # Lignes de code seulement : l'en-tete contient des exemples d'usage en commentaire.
    code = "\n".join(
        line
        for line in _read(SCRIPTS / "deploy.sh").splitlines()
        if not line.lstrip().startswith("#")
    )
    # Une affectation de mot de passe ne peut venir que de l'environnement ou d'un masque.
    for assignment in re.findall(r"SQL_ADMIN_PASSWORD=(\S+)", code):
        assert assignment.strip("\"'") == "<redacted>", assignment
    assert not re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", code)
