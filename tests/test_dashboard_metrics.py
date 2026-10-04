"""Tests des fonctions pures du dashboard (dashboard.metrics) : pas de rendu Streamlit."""

import math
from decimal import Decimal

import pandas as pd
import pytest

from dashboard import metrics

CLIMATE_ROWS = [
    # region_code, region_name, year, complete, ratio, class, dry_days, spi, agri, mock
    ("MA-01", "Nord", 2024, True, Decimal("0.60"), "normal", 250, Decimal("0.5"), True, True),
    ("MA-01", "Nord", 2025, True, Decimal("0.70"), "humide", 240, Decimal("1.5"), False, None),
    ("MA-01", "Nord", 2026, False, Decimal("0.30"), None, 150, None, False, None),
    ("MA-02", "Sud", 2024, True, Decimal("0.10"), "tres_sec", 330, Decimal("-0.5"), True, True),
    ("MA-02", "Sud", 2025, True, Decimal("0.30"), "sec", 320, Decimal("0.5"), False, None),
    ("MA-02", "Sud", 2026, False, Decimal("0.05"), None, 200, None, False, None),
]
SOLAR_ROWS = [
    # region_code, year, lat, lon, alt, complete, kwh, sunny_pct, score
    ("MA-01", 2024, 35.0, -5.0, 20.0, True, Decimal("5.0"), Decimal("50.0"), Decimal("50.0")),
    ("MA-01", 2025, 35.0, -5.0, 20.0, True, Decimal("5.2"), Decimal("55.0"), Decimal("56.0")),
    ("MA-01", 2026, 35.0, -5.0, 20.0, False, Decimal("5.5"), Decimal("60.0"), None),
    ("MA-02", 2024, 24.0, -15.0, 5.0, True, Decimal("6.0"), Decimal("75.0"), Decimal("75.0")),
    ("MA-02", 2025, 24.0, -15.0, 5.0, True, Decimal("6.2"), Decimal("80.0"), Decimal("80.0")),
    ("MA-02", 2026, 24.0, -15.0, 5.0, False, Decimal("6.4"), Decimal("85.0"), None),
]


def _climate_frame() -> pd.DataFrame:
    frame = pd.DataFrame(
        [
            {
                "region_code": code,
                "region_name": name,
                "year": year,
                "is_complete_year": ok,
                "precipitation_total_mm": Decimal("300.0"),
                "et0_total_mm": Decimal("1000.0"),
                "precip_et0_ratio": ratio,
                "drought_class": drought_class,
                "dry_days": dry,
                "dry_days_ratio": Decimal("0.7"),
                "spi_simplified": spi,
                "has_agriculture_data": has_agri,
                "production_tonnes": Decimal("1000.0") if has_agri else None,
                "irrigated_area_ha": Decimal("50.0") if has_agri else None,
                "water_resource_m3": Decimal("9000.0") if has_agri else None,
                "agriculture_is_mock": mock,
            }
            for code, name, year, ok, ratio, drought_class, dry, spi, has_agri, mock in CLIMATE_ROWS
        ]
    )
    return frame


def _solar_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "region_code": code,
                "year": year,
                "latitude": lat,
                "longitude": lon,
                "altitude_m": alt,
                "is_complete_year": complete,
                "solar_radiation_avg_kwh_m2_day": kwh,
                "sunny_days_pct": sunny,
                "radiation_score": score,
                "solar_potential_score": score,
            }
            for code, year, lat, lon, alt, complete, kwh, sunny, score in SOLAR_ROWS
        ]
    )


@pytest.fixture
def kpis() -> pd.DataFrame:
    return metrics.merge_kpis(_climate_frame(), _solar_frame())


# --- merge_kpis ----------------------------------------------------------------------------


def test_merge_keeps_one_row_per_region_and_year(kpis: pd.DataFrame) -> None:
    assert len(kpis) == 6
    assert not kpis.duplicated(["region_code", "year"]).any()
    assert list(kpis["region_code"]) == ["MA-01"] * 3 + ["MA-02"] * 3


def test_merge_converts_decimals_to_float(kpis: pd.DataFrame) -> None:
    for column in ("precip_et0_ratio", "solar_potential_score", "latitude"):
        assert pd.api.types.is_float_dtype(kpis[column]), column


def test_merge_keeps_both_completeness_flags(kpis: pd.DataFrame) -> None:
    assert {"climate_complete_year", "solar_complete_year"} <= set(kpis.columns)
    assert "is_complete_year" not in kpis.columns


def test_merge_fills_missing_mock_flag_with_false(kpis: pd.DataFrame) -> None:
    assert kpis["agriculture_is_mock"].dtype == bool
    assert bool(kpis.loc[kpis["year"] == 2025, "agriculture_is_mock"].any()) is False


def test_merge_drops_rows_missing_from_one_mart() -> None:
    solar = _solar_frame().iloc[:-1]
    merged = metrics.merge_kpis(_climate_frame(), solar)
    assert len(merged) == 5


# --- annees, filtres -----------------------------------------------------------------------


def test_available_years_sorted_descending(kpis: pd.DataFrame) -> None:
    assert metrics.available_years(kpis) == [2026, 2025, 2024]


def test_latest_complete_year_ignores_partial_year(kpis: pd.DataFrame) -> None:
    assert metrics.latest_complete_year(kpis) == 2025


def test_latest_complete_year_none_when_nothing_complete(kpis: pd.DataFrame) -> None:
    only_partial = kpis[kpis["year"] == 2026]
    assert metrics.latest_complete_year(only_partial) is None


def test_year_is_partial_if_one_region_is_incomplete(kpis: pd.DataFrame) -> None:
    mixed = kpis.copy()
    mixed.loc[
        (mixed["region_code"] == "MA-02") & (mixed["year"] == 2025), "solar_complete_year"
    ] = False
    assert metrics.is_partial_year(mixed, 2025)
    assert not metrics.is_partial_year(mixed, 2024)
    assert metrics.latest_complete_year(mixed) == 2024


def test_year_label_marks_partial_years(kpis: pd.DataFrame) -> None:
    assert metrics.year_label(kpis, 2025) == "2025"
    assert metrics.year_label(kpis, 2026) == "2026 (partielle)"
    assert metrics.is_partial_year(kpis, 1999)  # annee absente = non complete


def test_filter_by_year_and_regions(kpis: pd.DataFrame) -> None:
    view = metrics.filter_kpis(kpis, 2025, ["MA-02"])
    assert list(view["region_code"]) == ["MA-02"]
    assert list(view["year"]) == [2025]


def test_filter_without_year_returns_all_years(kpis: pd.DataFrame) -> None:
    view = metrics.filter_kpis(kpis, None, ["MA-01"])
    assert list(view["year"]) == [2024, 2025, 2026]


def test_filter_with_no_region_is_empty(kpis: pd.DataFrame) -> None:
    assert metrics.filter_kpis(kpis, 2025, []).empty


# --- agregats ------------------------------------------------------------------------------


def test_summarize_means_over_selection(kpis: pd.DataFrame) -> None:
    summary = metrics.summarize(metrics.filter_kpis(kpis, 2025, ["MA-01", "MA-02"]))
    assert summary["spi"] == pytest.approx(1.0)
    assert summary["ratio"] == pytest.approx(0.5)
    assert summary["solar_score"] == pytest.approx(68.0)
    assert summary["solar_kwh"] == pytest.approx(5.7)
    assert summary["dry_days"] == pytest.approx(280.0)


def test_summarize_returns_none_when_values_are_null(kpis: pd.DataFrame) -> None:
    summary = metrics.summarize(metrics.filter_kpis(kpis, 2026, ["MA-01", "MA-02"]))
    assert summary["spi"] is None
    assert summary["solar_score"] is None
    assert summary["ratio"] == pytest.approx(0.175)


def test_summarize_empty_selection_is_all_none(kpis: pd.DataFrame) -> None:
    assert set(metrics.summarize(kpis.iloc[0:0]).values()) == {None}


def test_delta() -> None:
    assert metrics.delta(0.7, 0.5) == pytest.approx(0.2)
    assert metrics.delta(None, 0.5) is None
    assert metrics.delta(0.7, None) is None


def test_rank_regions_descending_skips_nulls(kpis: pd.DataFrame) -> None:
    ranked = metrics.rank_regions(
        metrics.filter_kpis(kpis, 2025, ["MA-01", "MA-02"]),
        "solar_potential_score",
        ascending=False,
    )
    assert list(ranked["region_name"]) == ["Sud", "Nord"]
    partial = metrics.rank_regions(
        metrics.filter_kpis(kpis, 2026, ["MA-01", "MA-02"]),
        "solar_potential_score",
        ascending=False,
    )
    assert partial.empty


def test_rank_regions_ascending_and_limit(kpis: pd.DataFrame) -> None:
    ranked = metrics.rank_regions(
        metrics.filter_kpis(kpis, 2024, ["MA-01", "MA-02"]),
        "precip_et0_ratio",
        ascending=True,
        n=1,
    )
    assert list(ranked["region_name"]) == ["Sud"]


# --- couleurs et formatage -----------------------------------------------------------------


def test_hex_to_rgb() -> None:
    assert metrics.hex_to_rgb("#8c510a") == (140, 81, 10)


def test_drought_color_known_and_unknown_classes() -> None:
    assert metrics.drought_color("tres_sec")[:3] == [140, 81, 10]
    grey = [*metrics.hex_to_rgb(metrics.NO_DATA_COLOR), 220]
    assert metrics.drought_color(None) == grey
    assert metrics.drought_color(float("nan")) == grey  # type: ignore[arg-type]
    assert metrics.drought_color("inconnue") == grey


def test_every_drought_class_has_label_and_color() -> None:
    assert set(metrics.DROUGHT_CLASS_LABELS) == set(metrics.DROUGHT_CLASS_COLORS)
    assert set(metrics.DROUGHT_CLASS_LABELS) == {"tres_sec", "sec", "normal", "humide"}


def test_solar_color_ramp_endpoints_and_clamping() -> None:
    assert metrics.solar_color(0)[:3] == list(metrics.hex_to_rgb(metrics.SOLAR_RAMP_LOW))
    assert metrics.solar_color(100)[:3] == list(metrics.hex_to_rgb(metrics.SOLAR_RAMP_HIGH))
    assert metrics.solar_color(-20) == metrics.solar_color(0)
    assert metrics.solar_color(250) == metrics.solar_color(100)


def test_solar_color_missing_score_is_grey() -> None:
    grey = [*metrics.hex_to_rgb(metrics.NO_DATA_COLOR), 220]
    assert metrics.solar_color(None) == grey
    assert metrics.solar_color(math.nan) == grey


def test_format_number() -> None:
    assert metrics.format_number(1.2345, 2) == "1.23"
    assert metrics.format_number(60.44, 1, " / 100") == "60.4 / 100"
    assert metrics.format_number(None) == "—"
    assert metrics.format_number(math.nan) == "—"


def test_drought_label() -> None:
    assert metrics.drought_label("tres_sec") == "Très sec"
    assert metrics.drought_label(None) == metrics.NO_DATA_LABEL


# --- carte, tableau, export ----------------------------------------------------------------


def test_build_map_data_drought_mode(kpis: pd.DataFrame) -> None:
    map_data = metrics.build_map_data(
        metrics.filter_kpis(kpis, 2025, ["MA-01", "MA-02"]), "drought"
    )
    assert list(map_data["region_code"]) == ["MA-01", "MA-02"]
    assert list(map_data.columns) == [
        "region_code",
        "region_name",
        "latitude",
        "longitude",
        "color",
        "label",
    ]
    assert map_data.loc[0, "label"] == "Humide — P/ET0 0.70"
    assert map_data.loc[1, "color"] == metrics.drought_color("sec")


def test_build_map_data_solar_mode_handles_missing_score(kpis: pd.DataFrame) -> None:
    complete = metrics.build_map_data(metrics.filter_kpis(kpis, 2025, ["MA-02"]), "solar")
    assert complete.loc[0, "label"] == "Score solaire 80.0 — 6.20 kWh/m²/j"
    partial = metrics.build_map_data(metrics.filter_kpis(kpis, 2026, ["MA-02"]), "solar")
    assert str(partial.loc[0, "label"]).startswith("Score solaire —")
    assert partial.loc[0, "color"] == metrics.solar_color(None)


def test_build_map_data_empty_selection(kpis: pd.DataFrame) -> None:
    assert metrics.build_map_data(kpis.iloc[0:0], "drought").empty


def test_has_mock_agriculture(kpis: pd.DataFrame) -> None:
    assert metrics.has_mock_agriculture(metrics.filter_kpis(kpis, 2024, ["MA-01"]))
    assert not metrics.has_mock_agriculture(metrics.filter_kpis(kpis, 2025, ["MA-01"]))


def test_display_table_uses_french_labels_and_translates_class(kpis: pd.DataFrame) -> None:
    table = metrics.display_table(metrics.filter_kpis(kpis, 2025, ["MA-01", "MA-02"]))
    assert list(table.columns) == list(metrics.DISPLAY_COLUMNS.values())
    assert list(table["Classe de sécheresse"]) == ["Humide", "Sec"]


def test_display_table_marks_incomplete_year_class(kpis: pd.DataFrame) -> None:
    table = metrics.display_table(metrics.filter_kpis(kpis, 2026, ["MA-01"]))
    assert list(table["Classe de sécheresse"]) == [metrics.NO_DATA_LABEL]


def test_csv_export_round_trips_with_accents(kpis: pd.DataFrame) -> None:
    table = metrics.display_table(metrics.filter_kpis(kpis, 2025, ["MA-01", "MA-02"]))
    payload = metrics.to_csv_bytes(table)
    assert payload.startswith(b"\xef\xbb\xbf")  # BOM UTF-8 pour Excel
    text = payload.decode("utf-8-sig")
    header = text.splitlines()[0]
    assert header.startswith("Code région,Région,Année,Classe de sécheresse")
    assert len(text.splitlines()) == 3
