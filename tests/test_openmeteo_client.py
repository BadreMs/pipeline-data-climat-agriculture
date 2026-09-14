"""Tests du client Open-Meteo (ingestion.openmeteo_client) : respx mock, aucun appel reel."""

import time
from datetime import date
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
from pytest import MonkeyPatch

import ingestion.openmeteo_client as openmeteo_client
from ingestion.config import Settings
from ingestion.openmeteo_client import (
    DAILY_VARIABLES,
    OpenMeteoClient,
    WeatherDailyRecord,
    _split_into_chunks,
    records_to_dataframe,
)
from ingestion.regions import Region

TEST_REGION = Region("MA-04", "Rabat-Salé-Kénitra", "Rabat", 34.0211, -6.8414, 75)
TEST_URL = "https://archive-api.test/v1/archive"


@pytest.fixture
def test_settings() -> Settings:
    return Settings(openmeteo_base_url=TEST_URL, openmeteo_sleep_between_requests=0.0)


@pytest.fixture(autouse=True)
def _no_real_retry_backoff(monkeypatch: MonkeyPatch) -> None:
    """Neutralise le sleep interne de tenacity pendant les retries (backoff 2..32s)."""
    monkeypatch.setattr(time, "sleep", lambda seconds: None)


def _daily_payload(dates: list[str], **overrides: list[Any]) -> dict[str, Any]:
    daily: dict[str, Any] = {"time": dates}
    for variable in DAILY_VARIABLES:
        daily[variable] = overrides.get(variable, [10.0] * len(dates))
    return {
        "latitude": 33.989452,
        "longitude": -6.8539124,
        "elevation": 18.0,
        "timezone": "Africa/Casablanca",
        "daily_units": {},
        "daily": daily,
    }


# --- _split_into_chunks (fonction pure, pas de HTTP) ------------------------


def test_chunk_exactly_two_years_is_a_single_chunk() -> None:
    assert _split_into_chunks(date(2015, 1, 1), date(2016, 12, 31)) == [
        (date(2015, 1, 1), date(2016, 12, 31))
    ]


def test_chunk_shorter_than_two_years_is_a_single_chunk() -> None:
    assert _split_into_chunks(date(2015, 1, 1), date(2015, 6, 30)) == [
        (date(2015, 1, 1), date(2015, 6, 30))
    ]


def test_chunk_four_years_splits_into_two_chunks() -> None:
    assert _split_into_chunks(date(2015, 1, 1), date(2018, 12, 31)) == [
        (date(2015, 1, 1), date(2016, 12, 31)),
        (date(2017, 1, 1), date(2018, 12, 31)),
    ]


def test_chunk_leap_day_start_falls_back_to_feb_28() -> None:
    assert _split_into_chunks(date(2016, 2, 29), date(2020, 1, 1)) == [
        (date(2016, 2, 29), date(2018, 2, 27)),
        (date(2018, 2, 28), date(2020, 1, 1)),
    ]


def test_chunk_start_after_end_returns_empty_list() -> None:
    assert _split_into_chunks(date(2020, 1, 1), date(2019, 1, 1)) == []


# --- fetch_region : succes nominal, chunking, parsing -----------------------


def test_fetch_region_success_single_chunk(respx_mock: Any, test_settings: Settings) -> None:
    payload = _daily_payload(["2024-01-01", "2024-01-02"])
    respx_mock.get(TEST_URL).mock(return_value=httpx.Response(200, json=payload))

    with OpenMeteoClient(test_settings) as client:
        records = client.fetch_region(TEST_REGION, date(2024, 1, 1), date(2024, 1, 2))

    assert len(records) == 2
    record = records[0]
    assert record.region_code == "MA-04"
    assert record.date == date(2024, 1, 1)
    assert record.temperature_2m_max == 10.0
    assert record.temperature_2m_min == 10.0
    assert record.temperature_2m_mean == 10.0
    assert record.precipitation_sum == 10.0
    assert record.et0_fao_evapotranspiration == 10.0
    assert record.shortwave_radiation_sum == 10.0
    assert record.relative_humidity_2m_mean == 10.0
    assert record.wind_speed_10m_max == 10.0
    assert record.source_url.startswith(TEST_URL)


def test_source_url_is_exact_per_chunk_request_url(
    respx_mock: Any, test_settings: Settings
) -> None:
    route = respx_mock.get(TEST_URL).mock(
        side_effect=[
            httpx.Response(200, json=_daily_payload(["2015-01-01"])),
            httpx.Response(200, json=_daily_payload(["2017-01-01"])),
        ]
    )

    with OpenMeteoClient(test_settings) as client:
        records = client.fetch_region(TEST_REGION, date(2015, 1, 1), date(2018, 12, 31))

    assert route.call_count == 2
    first_request_url = str(route.calls[0].request.url)
    second_request_url = str(route.calls[1].request.url)
    assert first_request_url != second_request_url
    assert records[0].source_url == first_request_url
    assert records[1].source_url == second_request_url
    assert "start_date=2015-01-01" in first_request_url
    assert "end_date=2016-12-31" in first_request_url
    assert "start_date=2017-01-01" in second_request_url
    assert "end_date=2018-12-31" in second_request_url
    assert f"latitude={TEST_REGION.latitude}" in first_request_url


def test_fetch_region_splits_into_two_year_chunks(respx_mock: Any, test_settings: Settings) -> None:
    route = respx_mock.get(TEST_URL).mock(
        return_value=httpx.Response(200, json=_daily_payload(["2015-01-01"]))
    )

    with OpenMeteoClient(test_settings) as client:
        client.fetch_region(TEST_REGION, date(2015, 1, 1), date(2018, 12, 31))

    assert route.call_count == 2
    first_url = str(route.calls[0].request.url)
    second_url = str(route.calls[1].request.url)
    assert "start_date=2015-01-01" in first_url and "end_date=2016-12-31" in first_url
    assert "start_date=2017-01-01" in second_url and "end_date=2018-12-31" in second_url


def test_region_code_from_input_not_response(respx_mock: Any, test_settings: Settings) -> None:
    payload = _daily_payload(["2024-01-01"])  # latitude/longitude/elevation != TEST_REGION
    respx_mock.get(TEST_URL).mock(return_value=httpx.Response(200, json=payload))

    with OpenMeteoClient(test_settings) as client:
        records = client.fetch_region(TEST_REGION, date(2024, 1, 1), date(2024, 1, 1))

    assert records[0].region_code == "MA-04"
    assert not hasattr(records[0], "latitude")
    assert not hasattr(records[0], "longitude")


def test_null_values_become_none(respx_mock: Any, test_settings: Settings) -> None:
    payload = _daily_payload(["2024-01-01"], precipitation_sum=[None])
    respx_mock.get(TEST_URL).mock(return_value=httpx.Response(200, json=payload))

    with OpenMeteoClient(test_settings) as client:
        records = client.fetch_region(TEST_REGION, date(2024, 1, 1), date(2024, 1, 1))

    assert records[0].precipitation_sum is None


def test_integer_json_value_coerced_to_float(respx_mock: Any, test_settings: Settings) -> None:
    payload = _daily_payload(["2024-01-01"], relative_humidity_2m_mean=[68])
    respx_mock.get(TEST_URL).mock(return_value=httpx.Response(200, json=payload))

    with OpenMeteoClient(test_settings) as client:
        records = client.fetch_region(TEST_REGION, date(2024, 1, 1), date(2024, 1, 1))

    assert records[0].relative_humidity_2m_mean == 68.0
    assert isinstance(records[0].relative_humidity_2m_mean, float)


# --- retry / echecs ----------------------------------------------------------


def test_retry_on_http_500_then_success(respx_mock: Any, test_settings: Settings) -> None:
    payload = _daily_payload(["2024-01-01"])
    route = respx_mock.get(TEST_URL).mock(
        side_effect=[httpx.Response(500), httpx.Response(200, json=payload)]
    )

    with OpenMeteoClient(test_settings) as client:
        records = client.fetch_region(TEST_REGION, date(2024, 1, 1), date(2024, 1, 1))

    assert route.call_count == 2
    assert len(records) == 1
    assert client.failed_chunks == []


def test_retry_on_429_then_success(respx_mock: Any, test_settings: Settings) -> None:
    payload = _daily_payload(["2024-01-01"])
    route = respx_mock.get(TEST_URL).mock(
        side_effect=[httpx.Response(429), httpx.Response(200, json=payload)]
    )

    with OpenMeteoClient(test_settings) as client:
        records = client.fetch_region(TEST_REGION, date(2024, 1, 1), date(2024, 1, 1))

    assert route.call_count == 2
    assert len(records) == 1


def test_retry_exhausted_after_five_attempts(respx_mock: Any, test_settings: Settings) -> None:
    route = respx_mock.get(TEST_URL).mock(return_value=httpx.Response(500))

    with OpenMeteoClient(test_settings) as client:
        records = client.fetch_region(TEST_REGION, date(2024, 1, 1), date(2024, 1, 1))

    assert route.call_count == 5
    assert records == []


def test_retry_exhausted_adds_to_failed_chunks(respx_mock: Any, test_settings: Settings) -> None:
    respx_mock.get(TEST_URL).mock(return_value=httpx.Response(503))

    with OpenMeteoClient(test_settings) as client:
        client.fetch_region(TEST_REGION, date(2024, 1, 1), date(2024, 1, 1))
        failed = client.failed_chunks

    assert len(failed) == 1
    assert failed[0].region_code == "MA-04"
    assert failed[0].chunk_start == date(2024, 1, 1)
    assert failed[0].chunk_end == date(2024, 1, 1)
    assert failed[0].error


def test_no_retry_on_4xx_client_error(respx_mock: Any, test_settings: Settings) -> None:
    route = respx_mock.get(TEST_URL).mock(return_value=httpx.Response(404))

    with OpenMeteoClient(test_settings) as client:
        client.fetch_region(TEST_REGION, date(2024, 1, 1), date(2024, 1, 1))

    assert route.call_count == 1
    assert len(client.failed_chunks) == 1


def test_timeout_exception_is_retried(respx_mock: Any, test_settings: Settings) -> None:
    payload = _daily_payload(["2024-01-01"])
    route = respx_mock.get(TEST_URL).mock(
        side_effect=[httpx.TimeoutException("boom"), httpx.Response(200, json=payload)]
    )

    with OpenMeteoClient(test_settings) as client:
        records = client.fetch_region(TEST_REGION, date(2024, 1, 1), date(2024, 1, 1))

    assert route.call_count == 2
    assert len(records) == 1


def test_fetch_regions_continues_after_one_region_fails(
    respx_mock: Any, test_settings: Settings
) -> None:
    other_region = Region("MA-01", "Tanger-Tétouan-Al Hoceïma", "Tanger-Assilah", 35.7669, -5.8, 20)
    payload = _daily_payload(["2024-01-01"])

    def _responder(request: httpx.Request) -> httpx.Response:
        if "34.0211" in str(request.url):  # TEST_REGION (Rabat) -> echec permanent
            return httpx.Response(404)
        return httpx.Response(200, json=payload)

    respx_mock.get(TEST_URL).mock(side_effect=_responder)

    with OpenMeteoClient(test_settings) as client:
        records = client.fetch_regions(
            [TEST_REGION, other_region], date(2024, 1, 1), date(2024, 1, 1)
        )

    assert len(client.failed_chunks) == 1
    assert client.failed_chunks[0].region_code == "MA-04"
    assert len(records) == 1
    assert records[0].region_code == "MA-01"


# --- sleep entre requetes ------------------------------------------------------


def test_sleep_called_after_each_request(
    respx_mock: Any, monkeypatch: MonkeyPatch
) -> None:
    settings = Settings(openmeteo_base_url=TEST_URL, openmeteo_sleep_between_requests=1.5)
    payload = _daily_payload(["2024-01-01"])
    respx_mock.get(TEST_URL).mock(return_value=httpx.Response(200, json=payload))
    fake_sleep = MagicMock()
    monkeypatch.setattr(openmeteo_client, "sleep", fake_sleep)

    with OpenMeteoClient(settings) as client:
        client.fetch_region(TEST_REGION, date(2015, 1, 1), date(2018, 12, 31))  # 2 chunks

    assert fake_sleep.call_count == 2
    fake_sleep.assert_called_with(1.5)


# --- records_to_dataframe / to_row --------------------------------------------


def test_to_row_returns_expected_dict() -> None:
    record = WeatherDailyRecord(
        region_code="MA-04",
        date=date(2024, 1, 1),
        temperature_2m_max=20.0,
        temperature_2m_min=10.0,
        temperature_2m_mean=15.0,
        precipitation_sum=0.0,
        et0_fao_evapotranspiration=2.0,
        shortwave_radiation_sum=11.0,
        relative_humidity_2m_mean=60.0,
        wind_speed_10m_max=14.0,
        source_url=TEST_URL,
    )
    row = record.to_row()
    assert row["region_code"] == "MA-04"
    assert row["date"] == date(2024, 1, 1)
    assert row["temperature_2m_max"] == 20.0
    assert row["source_url"] == TEST_URL


def test_records_to_dataframe_columns() -> None:
    records = [
        WeatherDailyRecord(
            region_code="MA-04",
            date=date(2024, 1, 1),
            temperature_2m_max=20.0,
            temperature_2m_min=10.0,
            temperature_2m_mean=15.0,
            precipitation_sum=0.0,
            et0_fao_evapotranspiration=2.0,
            shortwave_radiation_sum=11.0,
            relative_humidity_2m_mean=60.0,
            wind_speed_10m_max=14.0,
            source_url=TEST_URL,
        )
    ]
    df = records_to_dataframe(records)
    assert list(df.columns) == [
        "region_code",
        "date",
        "temperature_2m_max",
        "temperature_2m_min",
        "temperature_2m_mean",
        "precipitation_sum",
        "et0_fao_evapotranspiration",
        "shortwave_radiation_sum",
        "relative_humidity_2m_mean",
        "wind_speed_10m_max",
        "source_url",
    ]
    assert len(df) == 1


def test_records_to_dataframe_empty_list_returns_empty_dataframe() -> None:
    df = records_to_dataframe([])
    assert df.empty
    assert "region_code" in df.columns


# --- gestion du cycle de vie httpx.Client -------------------------------------


def test_close_closes_owned_client() -> None:
    client = OpenMeteoClient(Settings(openmeteo_base_url=TEST_URL))
    inner = client._client  # noqa: SLF001
    assert inner.is_closed is False
    client.close()
    assert inner.is_closed is True


def test_close_does_not_close_injected_client() -> None:
    injected = httpx.Client()
    client = OpenMeteoClient(Settings(openmeteo_base_url=TEST_URL), client=injected)
    client.close()
    assert injected.is_closed is False
    injected.close()
