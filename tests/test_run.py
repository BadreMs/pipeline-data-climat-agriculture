"""Tests de ingestion.run (CLI d'orchestration) : tout mocke, aucun reseau/DB reel."""

import contextlib
import json
import uuid
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
from pytest import CaptureFixture, LogCaptureFixture, MonkeyPatch

import ingestion.run as run_module
from ingestion.config import Settings
from ingestion.db import upsert_agriculture_regional, upsert_weather_daily
from ingestion.openmeteo_client import FailedChunk, WeatherDailyRecord
from ingestion.regions import REGIONS, get_region
from ingestion.run import (
    CliArgs,
    main,
    orchestrate,
    parse_args,
    print_summary,
    run_agriculture,
    run_dry_run,
    run_weather,
)

TEST_URL = "https://archive-api.test/v1/archive"


@pytest.fixture(autouse=True)
def _isolated_cwd(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    """Evite qu'un vrai .env du repo n'interfere (meme pattern que les autres tests)."""
    monkeypatch.chdir(tmp_path)


def _settings(**overrides: Any) -> Settings:
    defaults: dict[str, Any] = {
        "openmeteo_base_url": TEST_URL,
        "openmeteo_start_date": date(2015, 1, 1),
        "openmeteo_end_date": None,
        "openmeteo_sleep_between_requests": 0.0,
        "datagovma_agriculture_url": None,
    }
    defaults.update(overrides)
    return Settings(**defaults)


def _record(region_code: str = "MA-04", day: date = date(2024, 1, 1)) -> WeatherDailyRecord:
    return WeatherDailyRecord(
        region_code=region_code,
        date=day,
        temperature_2m_max=20.0,
        temperature_2m_min=10.0,
        temperature_2m_mean=15.0,
        precipitation_sum=0.0,
        et0_fao_evapotranspiration=2.0,
        shortwave_radiation_sum=11.0,
        relative_humidity_2m_mean=60.0,
        wind_speed_10m_max=14.0,
        source_url=f"{TEST_URL}?region={region_code}",
    )


def _daily_payload(dates: list[str]) -> dict[str, Any]:
    daily: dict[str, Any] = {"time": dates}
    for variable in (
        "temperature_2m_max",
        "temperature_2m_min",
        "temperature_2m_mean",
        "precipitation_sum",
        "et0_fao_evapotranspiration",
        "shortwave_radiation_sum",
        "relative_humidity_2m_mean",
        "wind_speed_10m_max",
    ):
        daily[variable] = [10.0] * len(dates)
    return {"latitude": 33.99, "longitude": -6.85, "elevation": 18.0, "daily": daily}


class _FakeClient:
    """Double minimal d'OpenMeteoClient pour les tests d'orchestrate (pas de reseau)."""

    def __init__(self, records: list[WeatherDailyRecord], failed_chunks: list[FailedChunk]) -> None:
        self._records = records
        self.failed_chunks = failed_chunks

    def __enter__(self) -> "_FakeClient":
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def fetch_regions(self, regions: Any, start: Any, end: Any) -> list[WeatherDailyRecord]:
        return self._records


# --- parse_args ------------------------------------------------------------------


def test_start_defaults_to_env_when_omitted(caplog: LogCaptureFixture) -> None:
    settings = _settings(openmeteo_start_date=date(2015, 1, 1))
    with caplog.at_level("WARNING"):
        args = parse_args(["--end", "2015-01-10"], settings=settings)
    assert args.start == date(2015, 1, 1)
    assert "defaut" in caplog.text


def test_start_explicit_overrides_env_default() -> None:
    settings = _settings(openmeteo_start_date=date(2015, 1, 1))
    args = parse_args(["--start", "2020-01-01", "--end", "2020-01-10"], settings=settings)
    assert args.start == date(2020, 1, 1)


def test_end_defaults_to_settings_end_date() -> None:
    settings = _settings(openmeteo_end_date=date(2020, 6, 1))
    args = parse_args(["--start", "2020-01-01"], settings=settings)
    assert args.end == date(2020, 6, 1)


def test_end_defaults_to_today_when_nothing_set() -> None:
    settings = _settings(openmeteo_end_date=None)
    args = parse_args(["--start", "2020-01-01"], settings=settings)
    assert args.end == date.today()


def test_end_before_start_exits_2() -> None:
    settings = _settings()
    with pytest.raises(SystemExit) as exc_info:
        parse_args(["--start", "2020-06-01", "--end", "2020-01-01"], settings=settings)
    assert exc_info.value.code == 2


def test_defaults_to_twelve_regions_ordered() -> None:
    settings = _settings()
    args = parse_args(["--start", "2020-01-01"], settings=settings)
    assert [r.iso_code for r in args.regions] == [r.iso_code for r in REGIONS]


def test_custom_regions_parsed() -> None:
    settings = _settings()
    args = parse_args(["--start", "2020-01-01", "--regions", "MA-01,MA-04"], settings=settings)
    assert [r.iso_code for r in args.regions] == ["MA-01", "MA-04"]


def test_unknown_region_exits_2() -> None:
    settings = _settings()
    with pytest.raises(SystemExit) as exc_info:
        parse_args(["--start", "2020-01-01", "--regions", "MA-99"], settings=settings)
    assert exc_info.value.code == 2


def test_dry_run_defaults_region_to_ma04() -> None:
    settings = _settings()
    args = parse_args(["--dry-run"], settings=settings)
    assert [r.iso_code for r in args.regions] == ["MA-04"]


def test_dry_run_regions_overridable() -> None:
    settings = _settings()
    args = parse_args(["--dry-run", "--regions", "MA-07"], settings=settings)
    assert [r.iso_code for r in args.regions] == ["MA-07"]


def test_verbose_flag_parsed() -> None:
    settings = _settings()
    args = parse_args(["--start", "2020-01-01", "--verbose"], settings=settings)
    assert args.verbose is True


# --- run_weather / run_agriculture (unitaire, doubles legers) -------------------


def test_run_weather_upserts_and_reports_failed_chunks() -> None:
    failed = [FailedChunk("MA-09", date(2024, 1, 1), date(2024, 12, 31), "503")]
    client = MagicMock()
    client.fetch_regions.return_value = [_record(), _record(day=date(2024, 1, 2))]
    client.failed_chunks = failed
    conn = MagicMock()

    rows, returned_failed = run_weather(
        client, [get_region("MA-04")], date(2024, 1, 1), date(2024, 1, 2), conn, uuid.uuid4()
    )

    assert rows == 2
    assert returned_failed == failed
    conn.execute.assert_called_once()


def test_run_agriculture_returns_rows_and_source(monkeypatch: MonkeyPatch) -> None:
    import pandas as pd

    fake_df = pd.DataFrame([{"region_code": "MA-04", "annee": 2024, "production_tonnes": 1.0}])
    monkeypatch.setattr(
        run_module, "load_agriculture_regional", lambda settings: (fake_df, "datagovma")
    )
    conn = MagicMock()

    rows, source = run_agriculture(_settings(), conn, uuid.uuid4())

    assert rows == 1
    assert source == "datagovma"
    conn.execute.assert_called_once()


# --- run_dry_run (respx : vrai chemin HTTP intercepte) ---------------------------


def test_dry_run_window_is_seven_days_ending_today(respx_mock: Any) -> None:
    route = respx_mock.get(TEST_URL).mock(
        return_value=httpx.Response(200, json=_daily_payload(["2024-01-01"]))
    )
    settings = _settings()

    run_dry_run([get_region("MA-04")], settings)

    request_url = str(route.calls[0].request.url)
    today = date.today()
    expected_start = today - timedelta(days=6)
    assert f"start_date={expected_start.isoformat()}" in request_url
    assert f"end_date={today.isoformat()}" in request_url


def test_dry_run_prints_valid_json(respx_mock: Any, capsys: CaptureFixture[str]) -> None:
    respx_mock.get(TEST_URL).mock(
        return_value=httpx.Response(200, json=_daily_payload(["2024-01-01"]))
    )
    settings = _settings()

    run_dry_run([get_region("MA-04")], settings)

    payload = json.loads(capsys.readouterr().out)
    assert isinstance(payload, list)
    assert payload[0]["region_code"] == "MA-04"


def test_dry_run_never_opens_db_connection(respx_mock: Any, monkeypatch: MonkeyPatch) -> None:
    respx_mock.get(TEST_URL).mock(
        return_value=httpx.Response(200, json=_daily_payload(["2024-01-01"]))
    )
    get_connection_mock = MagicMock()
    monkeypatch.setattr(run_module, "get_connection", get_connection_mock)

    run_dry_run([get_region("MA-04")], _settings())

    get_connection_mock.assert_not_called()


def test_dry_run_zero_records_still_returns_zero(respx_mock: Any) -> None:
    respx_mock.get(TEST_URL).mock(return_value=httpx.Response(200, json=_daily_payload([])))

    result = run_dry_run([get_region("MA-04")], _settings())

    assert result == 0


# --- orchestrate (mock aux frontieres du module) ---------------------------------


def _patch_client(
    monkeypatch: MonkeyPatch, records: list[WeatherDailyRecord], failed: list[FailedChunk]
) -> None:
    monkeypatch.setattr(
        run_module, "OpenMeteoClient", lambda settings: _FakeClient(records, failed)
    )


def _patch_connection(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(
        run_module, "get_connection", lambda **kwargs: contextlib.nullcontext(MagicMock())
    )


def _cli_args(**overrides: Any) -> CliArgs:
    defaults: dict[str, Any] = {
        "start": date(2024, 1, 1),
        "end": date(2024, 1, 31),
        "regions": [get_region("MA-04")],
        "skip_weather": False,
        "skip_agriculture": False,
        "dry_run": False,
        "verbose": False,
    }
    defaults.update(overrides)
    return CliArgs(**defaults)


def test_orchestrate_exit_0_full_success(monkeypatch: MonkeyPatch) -> None:
    _patch_client(monkeypatch, [_record()], [])
    _patch_connection(monkeypatch)
    monkeypatch.setattr(
        run_module, "load_agriculture_regional", lambda settings: (_agri_df(), "datagovma")
    )

    result = orchestrate(_cli_args(), settings=_settings())

    assert result == 0


def test_orchestrate_exit_1_on_failed_chunks(monkeypatch: MonkeyPatch) -> None:
    failed = [FailedChunk("MA-04", date(2024, 1, 1), date(2024, 1, 31), "boom")]
    _patch_client(monkeypatch, [], failed)
    _patch_connection(monkeypatch)
    monkeypatch.setattr(
        run_module, "load_agriculture_regional", lambda settings: (_agri_df(), "datagovma")
    )

    result = orchestrate(_cli_args(), settings=_settings())

    assert result == 1


def test_orchestrate_exit_0_on_mock_not_configured(monkeypatch: MonkeyPatch) -> None:
    _patch_client(monkeypatch, [_record()], [])
    _patch_connection(monkeypatch)
    monkeypatch.setattr(
        run_module,
        "load_agriculture_regional",
        lambda settings: (_agri_df(), "mock-not-configured"),
    )

    result = orchestrate(_cli_args(), settings=_settings())

    assert result == 0


def test_orchestrate_exit_1_on_mock_network_error(monkeypatch: MonkeyPatch) -> None:
    _patch_client(monkeypatch, [_record()], [])
    _patch_connection(monkeypatch)
    monkeypatch.setattr(
        run_module,
        "load_agriculture_regional",
        lambda settings: (_agri_df(), "mock-fallback-network-error"),
    )

    result = orchestrate(_cli_args(), settings=_settings())

    assert result == 1


def test_orchestrate_exit_2_on_loader_value_error(monkeypatch: MonkeyPatch) -> None:
    _patch_client(monkeypatch, [_record()], [])
    _patch_connection(monkeypatch)

    def _raise(settings: Settings) -> Any:
        raise ValueError("colonnes manquantes")

    monkeypatch.setattr(run_module, "load_agriculture_regional", _raise)

    result = orchestrate(_cli_args(), settings=_settings())

    assert result == 2


def test_orchestrate_skip_weather_skips_client(monkeypatch: MonkeyPatch) -> None:
    client_factory = MagicMock()
    monkeypatch.setattr(run_module, "OpenMeteoClient", client_factory)
    _patch_connection(monkeypatch)
    monkeypatch.setattr(
        run_module, "load_agriculture_regional", lambda settings: (_agri_df(), "datagovma")
    )

    orchestrate(_cli_args(skip_weather=True), settings=_settings())

    client_factory.assert_not_called()


def test_orchestrate_skip_agriculture_skips_loader(monkeypatch: MonkeyPatch) -> None:
    _patch_client(monkeypatch, [_record()], [])
    _patch_connection(monkeypatch)
    loader_mock = MagicMock()
    monkeypatch.setattr(run_module, "load_agriculture_regional", loader_mock)

    orchestrate(_cli_args(skip_agriculture=True), settings=_settings())

    loader_mock.assert_not_called()


def test_batch_id_shared_between_weather_and_agriculture(monkeypatch: MonkeyPatch) -> None:
    _patch_client(monkeypatch, [_record()], [])
    _patch_connection(monkeypatch)
    captured_batch_ids: list[uuid.UUID] = []

    def _fake_loader(settings: Settings) -> Any:
        return _agri_df(), "datagovma"

    monkeypatch.setattr(run_module, "load_agriculture_regional", _fake_loader)

    def _capture_agriculture(conn: Any, df: Any, *, batch_id: uuid.UUID, **kwargs: Any) -> int:
        captured_batch_ids.append(batch_id)
        return upsert_agriculture_regional(conn, df, batch_id=batch_id, **kwargs)

    monkeypatch.setattr(run_module, "upsert_agriculture_regional", _capture_agriculture)

    def _capture_weather(conn: Any, df: Any, *, batch_id: uuid.UUID, **kwargs: Any) -> int:
        captured_batch_ids.append(batch_id)
        return upsert_weather_daily(conn, df, batch_id=batch_id, **kwargs)

    monkeypatch.setattr(run_module, "upsert_weather_daily", _capture_weather)

    orchestrate(_cli_args(), settings=_settings())

    assert len(captured_batch_ids) == 2
    assert captured_batch_ids[0] == captured_batch_ids[1]


def _agri_df() -> Any:
    import pandas as pd

    return pd.DataFrame([{"region_code": "MA-04", "annee": 2024, "production_tonnes": 1.0}])


# --- print_summary -----------------------------------------------------------------


def test_print_summary_format(capsys: CaptureFixture[str]) -> None:
    batch_id = uuid.uuid4()
    print_summary(
        100,
        12,
        [FailedChunk("MA-09", date(2024, 1, 1), date(2024, 1, 2), "x")],
        "datagovma",
        batch_id,
    )

    out = capsys.readouterr().out
    assert (
        f"100 lignes weather upsertees / 12 lignes agriculture upsertees / "
        f"1 chunks en echec / batch_id={batch_id} / agriculture_source=datagovma"
    ) in out


# --- main (fumee, argparse -> SystemExit) ------------------------------------------


def test_main_exits_2_on_unknown_region(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(run_module, "get_settings", lambda: _settings())
    with pytest.raises(SystemExit) as exc_info:
        main(["--start", "2020-01-01", "--regions", "MA-99"])
    assert exc_info.value.code == 2
