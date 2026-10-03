"""Exporte ingestion.regions.REGIONS vers le seed dbt dim_region.csv.

Source de verite = ingestion/regions.py : ne pas editer le CSV a la main, relancer
`uv run python -m scripts.export_dim_region`. Idempotent (ecrase le fichier).
"""

import csv
from pathlib import Path

from ingestion.regions import REGIONS

SEED_PATH = Path(__file__).resolve().parents[1] / "dbt_project" / "seeds" / "dim_region.csv"
COLUMNS = ("region_code", "region_name", "capital", "latitude", "longitude", "altitude_m")


def export_dim_region(output: Path = SEED_PATH) -> Path:
    """Ecrit le seed (UTF-8, fins de ligne LF) et retourne son chemin."""
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh, lineterminator="\n")
        writer.writerow(COLUMNS)
        for r in REGIONS:
            writer.writerow(
                (r.iso_code, r.name, r.capital, r.latitude, r.longitude, float(r.altitude_m))
            )
    return output


def main() -> None:
    path = export_dim_region()
    print(f"{len(REGIONS)} regions ecrites dans {path}")


if __name__ == "__main__":
    main()
