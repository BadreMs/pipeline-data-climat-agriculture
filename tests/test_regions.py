"""Tests du referentiel des 12 regions marocaines."""

import re

import pytest

from ingestion.regions import REGIONS, Region, get_region, iso_codes

ISO_CODE_PATTERN = re.compile(r"^MA-(0[1-9]|1[0-2])$")

# Bornes larges du territoire marocain (hors iles Canaries), en degres decimaux.
MOROCCO_LAT_BOUNDS = (20.0, 36.0)
MOROCCO_LON_BOUNDS = (-18.0, -1.0)


def test_exactly_twelve_regions() -> None:
    assert len(REGIONS) == 12


def test_iso_codes_are_unique() -> None:
    codes = [region.iso_code for region in REGIONS]
    assert len(codes) == len(set(codes))


def test_iso_codes_match_ma_pattern() -> None:
    for region in REGIONS:
        assert ISO_CODE_PATTERN.match(region.iso_code), region.iso_code


def test_iso_codes_cover_01_to_12() -> None:
    assert iso_codes() == tuple(f"MA-{i:02d}" for i in range(1, 13))


@pytest.mark.parametrize("region", REGIONS, ids=lambda r: r.iso_code)
def test_coordinates_within_morocco_bounds(region: Region) -> None:
    assert MOROCCO_LAT_BOUNDS[0] <= region.latitude <= MOROCCO_LAT_BOUNDS[1]
    assert MOROCCO_LON_BOUNDS[0] <= region.longitude <= MOROCCO_LON_BOUNDS[1]


@pytest.mark.parametrize("region", REGIONS, ids=lambda r: r.iso_code)
def test_altitude_is_non_negative(region: Region) -> None:
    assert region.altitude_m >= 0


def test_get_region_returns_expected_region() -> None:
    region = get_region("MA-04")
    assert region.capital == "Rabat"


def test_get_region_unknown_code_raises_key_error() -> None:
    with pytest.raises(KeyError):
        get_region("MA-99")
