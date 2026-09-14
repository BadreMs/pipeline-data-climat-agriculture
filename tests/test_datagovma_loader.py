"""Tests du loader data.gov.ma (ingestion.datagovma_loader) : respx mock, aucun reseau reel."""

from pathlib import Path
from typing import Any

import httpx
import pytest
from pytest import LogCaptureFixture, MonkeyPatch

import ingestion.datagovma_loader as loader_module
from ingestion.config import Settings
from ingestion.datagovma_loader import (
    REGION_ALIASES,
    _parse_decimal,
    _parse_year,
    _resolve_region_code,
    load_agriculture_regional,
)
from ingestion.regions import iso_codes

TEST_URL = "https://data.gov.ma.test/agriculture.csv"


@pytest.fixture(autouse=True)
def _isolated_cwd(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    """Evite qu'un vrai .env du repo n'interfere (meme pattern que test_config.py)."""
    monkeypatch.chdir(tmp_path)


def _csv_bytes(text: str, encoding: str = "utf-8") -> bytes:
    return text.encode(encoding)


VALID_CSV = (
    "region_code,annee,production_tonnes,surface_irriguee_ha,ressource_hydrique_m3\n"
    "MA-04,2020,410000.0,98000.0,780000000.0\n"
    "MA-06,2020,830000.0,210000.0,1200000000.0\n"
)


# --- source = mock vs distant --------------------------------------------------


def test_no_url_configured_uses_mock() -> None:
    settings = Settings(datagovma_agriculture_url=None)
    df, source = load_agriculture_regional(settings)
    assert source == "mock-fallback"
    assert not df.empty


def test_remote_download_success(respx_mock: Any) -> None:
    respx_mock.get(TEST_URL).mock(return_value=httpx.Response(200, content=_csv_bytes(VALID_CSV)))
    settings = Settings(datagovma_agriculture_url=TEST_URL)

    df, source = load_agriculture_regional(settings)

    assert source == "datagovma"
    assert set(df["region_code"]) == {"MA-04", "MA-06"}
    assert df.loc[df["region_code"] == "MA-04", "production_tonnes"].iloc[0] == 410000.0


def test_remote_download_network_error_falls_back_to_mock(respx_mock: Any) -> None:
    respx_mock.get(TEST_URL).mock(side_effect=httpx.ConnectError("boom"))
    settings = Settings(datagovma_agriculture_url=TEST_URL)

    df, source = load_agriculture_regional(settings)

    assert source == "mock-fallback"
    assert not df.empty


def test_remote_404_falls_back_to_mock(respx_mock: Any) -> None:
    respx_mock.get(TEST_URL).mock(return_value=httpx.Response(404))
    settings = Settings(datagovma_agriculture_url=TEST_URL)

    df, source = load_agriculture_regional(settings)

    assert source == "mock-fallback"


def test_remote_missing_columns_raises_value_error(respx_mock: Any) -> None:
    bad_csv = "region_code,annee,production_tonnes\nMA-04,2020,410000.0\n"
    respx_mock.get(TEST_URL).mock(return_value=httpx.Response(200, content=_csv_bytes(bad_csv)))
    settings = Settings(datagovma_agriculture_url=TEST_URL)

    with pytest.raises(ValueError, match="Colonnes manquantes"):
        load_agriculture_regional(settings)


def test_mock_fixture_missing_columns_raises_value_error(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    corrupted = tmp_path / "corrupted_mock.csv"
    corrupted.write_text("region,annee,production_tonnes\nRabat,2020,1.0\n", encoding="utf-8")
    monkeypatch.setattr(loader_module, "MOCK_FIXTURE_PATH", corrupted)
    settings = Settings(datagovma_agriculture_url=None)

    with pytest.raises(ValueError, match="Colonnes manquantes"):
        load_agriculture_regional(settings)


# --- formats : separateur, encodage, decimales ---------------------------------


def test_separator_auto_detection_semicolon(respx_mock: Any) -> None:
    csv_text = VALID_CSV.replace(",", ";")
    respx_mock.get(TEST_URL).mock(return_value=httpx.Response(200, content=_csv_bytes(csv_text)))
    settings = Settings(datagovma_agriculture_url=TEST_URL)

    df, _ = load_agriculture_regional(settings)

    assert len(df) == 2


def test_encoding_fallback_latin1(respx_mock: Any) -> None:
    csv_text = (
        "region_code,annee,production_tonnes,surface_irriguee_ha,ressource_hydrique_m3\n"
        "MA-04,2020,410000.0,98000.0,780000000.0\n"
    )
    respx_mock.get(TEST_URL).mock(
        return_value=httpx.Response(200, content=_csv_bytes(csv_text, encoding="latin-1"))
    )
    settings = Settings(datagovma_agriculture_url=TEST_URL)

    df, _ = load_agriculture_regional(settings)

    assert len(df) == 1


def test_decimal_comma_and_ascii_space_parsing() -> None:
    assert _parse_decimal("1 234,56") == 1234.56
    assert _parse_decimal("1234.56") == 1234.56
    assert _parse_decimal("1234,56") == 1234.56


def test_decimal_narrow_and_nobreak_space_parsing() -> None:
    assert _parse_decimal("1 234,56") == 1234.56  # espace insecable
    assert _parse_decimal("1 234,56") == 1234.56  # espace fine insecable


def test_decimal_numeric_input_passthrough() -> None:
    assert _parse_decimal(1234.5) == 1234.5
    assert _parse_decimal(1234) == 1234.0


def test_decimal_blank_or_none_returns_none() -> None:
    assert _parse_decimal("") is None
    assert _parse_decimal(None) is None


# --- mapping regions / annees invalides -----------------------------------------


def test_unknown_region_label_is_skipped_with_warning(
    respx_mock: Any, caplog: LogCaptureFixture
) -> None:
    csv_text = (
        "region_code,annee,production_tonnes,surface_irriguee_ha,ressource_hydrique_m3\n"
        "Atlantis,2020,1.0,1.0,1.0\n"
        "MA-04,2020,410000.0,98000.0,780000000.0\n"
    )
    respx_mock.get(TEST_URL).mock(return_value=httpx.Response(200, content=_csv_bytes(csv_text)))
    settings = Settings(datagovma_agriculture_url=TEST_URL)

    with caplog.at_level("WARNING"):
        df, _ = load_agriculture_regional(settings)

    assert len(df) == 1
    assert "Atlantis" in caplog.text


def test_missing_region_in_source_logs_warning(respx_mock: Any, caplog: LogCaptureFixture) -> None:
    respx_mock.get(TEST_URL).mock(return_value=httpx.Response(200, content=_csv_bytes(VALID_CSV)))
    settings = Settings(datagovma_agriculture_url=TEST_URL)

    with caplog.at_level("WARNING"):
        load_agriculture_regional(settings)

    assert "MA-01" in caplog.text


def test_invalid_or_blank_year_is_skipped(respx_mock: Any, caplog: LogCaptureFixture) -> None:
    csv_text = (
        "region_code,annee,production_tonnes,surface_irriguee_ha,ressource_hydrique_m3\n"
        "MA-04,,410000.0,98000.0,780000000.0\n"
        "MA-06,N/A,830000.0,210000.0,1200000000.0\n"
        "MA-07,2020,470000.0,165000.0,980000000.0\n"
    )
    respx_mock.get(TEST_URL).mock(return_value=httpx.Response(200, content=_csv_bytes(csv_text)))
    settings = Settings(datagovma_agriculture_url=TEST_URL)

    with caplog.at_level("WARNING"):
        df, _ = load_agriculture_regional(settings)

    assert len(df) == 1
    assert df.iloc[0]["region_code"] == "MA-07"


def test_campaign_year_format_parsed_as_start_year() -> None:
    assert _parse_year("2010/2011") == 2010
    assert _parse_year("2010") == 2010
    assert _parse_year("") is None
    assert _parse_year("N/A") is None
    assert _parse_year(2010) == 2010


# --- normalisation region --------------------------------------------------------


def test_normalize_label_handles_accents_and_apostrophe() -> None:
    assert _resolve_region_code("L'Oriental") == "MA-02"
    assert _resolve_region_code("Béni Mellal-Khénifra") == "MA-05"


def test_normalize_label_passthrough_iso_code() -> None:
    assert _resolve_region_code("ma-04") == "MA-04"
    assert _resolve_region_code("MA-04") == "MA-04"


def test_unknown_label_returns_none() -> None:
    assert _resolve_region_code("Atlantis") is None


@pytest.mark.parametrize(
    ("legacy_label", "expected_code"),
    [
        ("Tanger-Tétouan", "MA-01"),
        ("Meknès-Tafilalet", "MA-03"),
        ("Gharb-Chrarda-Beni Hssen", "MA-04"),
        ("Chaouia-Ouardigha", "MA-06"),
        ("Marrakech-Tensift-Al Haouz", "MA-07"),
        ("Souss-Massa-Drâa", "MA-09"),
        ("Oued Ed-Dahab-Lagouira", "MA-12"),
    ],
)
def test_pre_2015_region_alias_maps_to_modern_code(legacy_label: str, expected_code: str) -> None:
    assert _resolve_region_code(legacy_label) == expected_code


def test_region_aliases_cover_all_modern_iso_codes() -> None:
    assert set(REGION_ALIASES.values()) == set(iso_codes())


# --- fixture mock -----------------------------------------------------------------


def test_mock_fixture_covers_all_twelve_regions() -> None:
    settings = Settings(datagovma_agriculture_url=None)
    df, _ = load_agriculture_regional(settings)
    assert set(df["region_code"]) == set(iso_codes())
