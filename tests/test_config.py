"""Tests des settings typees (ingestion.config)."""

from datetime import date
from pathlib import Path

import pytest
from pytest import MonkeyPatch

from ingestion.config import Settings


@pytest.fixture(autouse=True)
def _isolated_cwd(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    """Evite qu'un vrai .env (fichier ou env process) n'interfere avec les
    valeurs par defaut testees."""
    monkeypatch.chdir(tmp_path)
    for field in Settings.model_fields:
        monkeypatch.delenv(field.upper(), raising=False)


def test_defaults_match_env_example_when_no_env_file() -> None:
    settings = Settings()
    assert settings.postgres_host == "localhost"
    assert settings.postgres_port == 5432
    assert settings.openmeteo_start_date == date(2015, 1, 1)
    assert settings.openmeteo_end_date is None
    assert settings.datagovma_use_mock_fallback is True


def test_dwh_dsn_format() -> None:
    settings = Settings()
    assert settings.dwh_dsn == "postgresql+psycopg2://pipeline:changeme@localhost:5432/dwh"


def test_blank_openmeteo_end_date_is_none(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("OPENMETEO_END_DATE", "")
    settings = Settings()
    assert settings.openmeteo_end_date is None


def test_openmeteo_end_date_parses_iso_date(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("OPENMETEO_END_DATE", "2026-01-31")
    settings = Settings()
    assert settings.openmeteo_end_date == date(2026, 1, 31)


def test_env_vars_override_defaults(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_USER", "custom_user")
    monkeypatch.setenv("POSTGRES_DWH_DB", "custom_db")
    settings = Settings()
    assert settings.postgres_user == "custom_user"
    assert settings.postgres_dwh_db == "custom_db"


@pytest.mark.parametrize("raw", ["true", "1", "yes"])
def test_datagovma_mock_fallback_accepts_truthy_strings(monkeypatch: MonkeyPatch, raw: str) -> None:
    monkeypatch.setenv("DATAGOVMA_USE_MOCK_FALLBACK", raw)
    settings = Settings()
    assert settings.datagovma_use_mock_fallback is True
