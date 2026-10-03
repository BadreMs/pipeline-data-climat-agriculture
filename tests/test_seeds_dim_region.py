"""Coherence stricte entre ingestion.regions.REGIONS (source de verite) et le seed dim_region."""

import csv
from pathlib import Path

import pytest

from ingestion.regions import REGIONS, Region
from scripts.export_dim_region import export_dim_region

SEED_PATH = Path(__file__).resolve().parents[1] / "dbt_project" / "seeds" / "dim_region.csv"
EXPECTED_HEADER = [
    "region_code",
    "region_name",
    "capital",
    "latitude",
    "longitude",
    "altitude_m",
]


def _read_seed() -> tuple[list[str], list[dict[str, str]]]:
    with SEED_PATH.open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        header = list(reader.fieldnames or [])
        return header, list(reader)


def test_seed_header_is_exact() -> None:
    header, _ = _read_seed()
    assert header == EXPECTED_HEADER


def test_seed_has_same_codes_in_same_order() -> None:
    _, rows = _read_seed()
    assert [row["region_code"] for row in rows] == [region.iso_code for region in REGIONS]


@pytest.mark.parametrize("region", REGIONS, ids=lambda region: region.iso_code)
def test_seed_row_matches_region_field_by_field(region: Region) -> None:
    _, rows = _read_seed()
    row = next(r for r in rows if r["region_code"] == region.iso_code)
    assert row["region_name"] == region.name
    assert row["capital"] == region.capital
    assert float(row["latitude"]) == region.latitude
    assert float(row["longitude"]) == region.longitude
    assert float(row["altitude_m"]) == region.altitude_m


def test_seed_has_no_blank_values() -> None:
    _, rows = _read_seed()
    for row in rows:
        assert all(value.strip() for value in row.values()), row


def test_export_script_regenerates_committed_seed(tmp_path: Path) -> None:
    generated = export_dim_region(tmp_path / "dim_region.csv")
    # splitlines() neutralise un eventuel CRLF introduit par un checkout git sous Windows.
    assert generated.read_text(encoding="utf-8").splitlines() == (
        SEED_PATH.read_text(encoding="utf-8").splitlines()
    )
