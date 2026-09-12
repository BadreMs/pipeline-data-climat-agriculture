"""Tests de ingestion.db : construction des requetes upsert (mock) + get_connection."""

import uuid
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest
from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql

from ingestion.config import Settings
from ingestion.db import (
    agriculture_regional,
    get_connection,
    get_engine,
    upsert_agriculture_regional,
    upsert_weather_daily,
    weather_daily,
)

BATCH_ID = uuid.uuid4()


def _compiled_sql(stmt: Any) -> str:
    dialect = postgresql.dialect()  # type: ignore[no-untyped-call]
    return str(stmt.compile(dialect=dialect))


def test_get_engine_uses_settings_dsn() -> None:
    settings = Settings(
        postgres_user="u", postgres_password="p", postgres_host="h", postgres_dwh_db="d"
    )
    engine = get_engine(settings)
    assert engine.url.render_as_string(hide_password=False) == "postgresql+psycopg://u:p@h:5432/d"


def test_upsert_weather_daily_compiles_on_conflict_clause() -> None:
    conn = MagicMock()
    df = pd.DataFrame([{"region_code": "MA-04", "date": "2024-01-01", "temperature_2m_max": 20.5}])

    upsert_weather_daily(conn, df, batch_id=BATCH_ID)

    conn.execute.assert_called_once()
    stmt = conn.execute.call_args.args[0]
    sql = _compiled_sql(stmt)
    assert "INSERT INTO raw.weather_daily" in sql
    assert "ON CONFLICT (region_code, date) DO UPDATE SET" in sql


def test_upsert_weather_daily_injects_audit_columns() -> None:
    conn = MagicMock()
    df = pd.DataFrame([{"region_code": "MA-04", "date": "2024-01-01", "temperature_2m_max": 20.5}])

    upsert_weather_daily(conn, df, batch_id=BATCH_ID, source="open-meteo-archive", source_url="https://x")

    stmt = conn.execute.call_args.args[0]
    dialect = postgresql.dialect()  # type: ignore[no-untyped-call]
    compiled_params = stmt.compile(dialect=dialect).params
    assert "open-meteo-archive" in compiled_params.values()
    assert "https://x" in compiled_params.values()
    assert BATCH_ID in compiled_params.values()


def test_upsert_weather_daily_empty_dataframe_is_noop() -> None:
    conn = MagicMock()
    result = upsert_weather_daily(conn, pd.DataFrame(), batch_id=BATCH_ID)
    assert result == 0
    conn.execute.assert_not_called()


def test_upsert_weather_daily_returns_row_count() -> None:
    conn = MagicMock()
    df = pd.DataFrame(
        [
            {"region_code": "MA-04", "date": "2024-01-01", "temperature_2m_max": 20.5},
            {"region_code": "MA-04", "date": "2024-01-02", "temperature_2m_max": 21.0},
        ]
    )
    result = upsert_weather_daily(conn, df, batch_id=BATCH_ID)
    assert result == 2


def test_upsert_agriculture_regional_compiles_on_conflict_clause() -> None:
    conn = MagicMock()
    df = pd.DataFrame([{"region_code": "MA-04", "annee": 2024, "production_tonnes": 1000.0}])

    upsert_agriculture_regional(conn, df, batch_id=BATCH_ID, source="datagovma")

    stmt = conn.execute.call_args.args[0]
    sql = _compiled_sql(stmt)
    assert "INSERT INTO raw.agriculture_regional" in sql
    assert "ON CONFLICT (region_code, annee) DO UPDATE SET" in sql


def test_upsert_agriculture_regional_requires_source() -> None:
    conn = MagicMock()
    df = pd.DataFrame([{"region_code": "MA-04", "annee": 2024}])
    with pytest.raises(TypeError):
        upsert_agriculture_regional(conn, df, batch_id=BATCH_ID)  # type: ignore[call-arg]


def test_get_connection_commits_on_success() -> None:
    engine = create_engine("sqlite:///:memory:")
    with get_connection(engine=engine) as conn:
        conn.exec_driver_sql("CREATE TABLE t (id INTEGER)")
        conn.exec_driver_sql("INSERT INTO t VALUES (1)")

    with engine.connect() as check_conn:
        rows = check_conn.exec_driver_sql("SELECT COUNT(*) FROM t").scalar()
    assert rows == 1


def test_get_connection_rolls_back_on_exception() -> None:
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as setup_conn:
        setup_conn.exec_driver_sql("CREATE TABLE t (id INTEGER)")
        setup_conn.commit()

    with pytest.raises(ValueError), get_connection(engine=engine) as conn:
        conn.exec_driver_sql("INSERT INTO t VALUES (1)")
        raise ValueError("boom")

    with engine.connect() as check_conn:
        rows = check_conn.exec_driver_sql("SELECT COUNT(*) FROM t").scalar()
    assert rows == 0


def test_weather_daily_and_agriculture_regional_tables_have_expected_schema() -> None:
    assert {c.name for c in weather_daily.columns} >= {"region_code", "date", "_batch_id"}
    assert {c.name for c in agriculture_regional.columns} >= {"region_code", "annee", "_batch_id"}
