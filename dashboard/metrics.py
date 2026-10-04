"""Fonctions pures du dashboard : fusion, filtrage, agregation, couleurs, formatage, export.

Aucune dependance a Streamlit ni a la base : tout est testable avec de simples DataFrames.
"""

import math
from collections.abc import Sequence
from typing import Any, Literal

import pandas as pd

MapMode = Literal["drought", "solar"]

# --- Classes de secheresse (cf. fct_region_climate_kpi.drought_class) -----------------------
DROUGHT_CLASS_LABELS: dict[str, str] = {
    "tres_sec": "Très sec",
    "sec": "Sec",
    "normal": "Normal",
    "humide": "Humide",
}
# Palette brun -> bleu-vert (lisible en daltonisme rouge/vert).
DROUGHT_CLASS_COLORS: dict[str, str] = {
    "tres_sec": "#8c510a",
    "sec": "#d8b365",
    "normal": "#80cdc1",
    "humide": "#01665e",
}
NO_DATA_COLOR = "#9e9e9e"
NO_DATA_LABEL = "Année incomplète"

# Rampe du score solaire (0 -> 100) : jaune clair -> rouge fonce.
SOLAR_RAMP_LOW = "#ffffb2"
SOLAR_RAMP_HIGH = "#bd0026"

# --- Colonnes des marts utilisees par le dashboard ---------------------------------------------
CLIMATE_COLUMNS: tuple[str, ...] = (
    "region_code",
    "region_name",
    "year",
    "is_complete_year",
    "precipitation_total_mm",
    "et0_total_mm",
    "precip_et0_ratio",
    "drought_class",
    "dry_days",
    "dry_days_ratio",
    "spi_simplified",
    "has_agriculture_data",
    "production_tonnes",
    "irrigated_area_ha",
    "water_resource_m3",
    "agriculture_is_mock",
)
SOLAR_COLUMNS: tuple[str, ...] = (
    "region_code",
    "year",
    "latitude",
    "longitude",
    "altitude_m",
    "is_complete_year",
    "solar_radiation_avg_kwh_m2_day",
    "sunny_days_pct",
    "radiation_score",
    "solar_potential_score",
)
NUMERIC_COLUMNS: tuple[str, ...] = (
    "precipitation_total_mm",
    "et0_total_mm",
    "precip_et0_ratio",
    "dry_days",
    "dry_days_ratio",
    "spi_simplified",
    "production_tonnes",
    "irrigated_area_ha",
    "water_resource_m3",
    "latitude",
    "longitude",
    "altitude_m",
    "solar_radiation_avg_kwh_m2_day",
    "sunny_days_pct",
    "radiation_score",
    "solar_potential_score",
)

# Libelle -> colonne pour les series temporelles.
TIMESERIES_METRICS: dict[str, str] = {
    "SPI simplifié": "spi_simplified",
    "Ratio précipitations / ET0": "precip_et0_ratio",
    "Précipitations annuelles (mm)": "precipitation_total_mm",
    "Jours secs": "dry_days",
    "Score de potentiel solaire (0-100)": "solar_potential_score",
    "Rayonnement moyen (kWh/m²/jour)": "solar_radiation_avg_kwh_m2_day",
}

# Colonnes affichees/exportees dans le tableau detaille, avec leur libelle.
DISPLAY_COLUMNS: dict[str, str] = {
    "region_code": "Code région",
    "region_name": "Région",
    "year": "Année",
    "drought_class": "Classe de sécheresse",
    "precip_et0_ratio": "Ratio P/ET0",
    "spi_simplified": "SPI simplifié",
    "precipitation_total_mm": "Précipitations (mm)",
    "et0_total_mm": "ET0 (mm)",
    "dry_days": "Jours secs",
    "solar_radiation_avg_kwh_m2_day": "Rayonnement (kWh/m²/j)",
    "sunny_days_pct": "Jours ensoleillés (%)",
    "solar_potential_score": "Score solaire (0-100)",
    "production_tonnes": "Production (t) [mock]",
    "irrigated_area_ha": "Surface irriguée (ha) [mock]",
    "water_resource_m3": "Ressource hydrique (m³) [mock]",
}


def merge_kpis(climate: pd.DataFrame, solar: pd.DataFrame) -> pd.DataFrame:
    """Fusionne les deux marts (1 ligne = region x annee) et normalise les types.

    Les NUMERIC Postgres arrivent en `Decimal` (dtype object) : on les convertit en float.
    `is_complete_year` existe dans les deux marts (couvertures differentes) : on garde les deux.
    """
    climate_part = climate.loc[:, list(CLIMATE_COLUMNS)].rename(
        columns={"is_complete_year": "climate_complete_year"}
    )
    solar_part = solar.loc[:, list(SOLAR_COLUMNS)].rename(
        columns={"is_complete_year": "solar_complete_year"}
    )
    merged = climate_part.merge(solar_part, on=["region_code", "year"], how="inner")
    for column in NUMERIC_COLUMNS:
        merged[column] = pd.to_numeric(merged[column], errors="coerce")
    merged["agriculture_is_mock"] = merged["agriculture_is_mock"].fillna(False).astype(bool)
    return merged.sort_values(["region_code", "year"]).reset_index(drop=True)


def available_years(df: pd.DataFrame) -> list[int]:
    """Annees presentes, de la plus recente a la plus ancienne."""
    return sorted((int(y) for y in df["year"].unique()), reverse=True)


def _complete_by_year(df: pd.DataFrame) -> dict[int, bool]:
    """Pour chaque annee : True si TOUTES les regions sont completes (climat et solaire)."""
    status: dict[int, bool] = {}
    for record in df.to_dict("records"):
        year = int(record["year"])
        complete = bool(record["climate_complete_year"]) and bool(record["solar_complete_year"])
        status[year] = status.get(year, True) and complete
    return status


def latest_complete_year(df: pd.DataFrame) -> int | None:
    """Annee complete la plus recente (None si aucune)."""
    complete = [year for year, ok in _complete_by_year(df).items() if ok]
    return max(complete) if complete else None


def is_partial_year(df: pd.DataFrame, year: int) -> bool:
    """True si l'annee est absente ou incomplete pour au moins une region."""
    return not _complete_by_year(df).get(year, False)


def year_label(df: pd.DataFrame, year: int) -> str:
    """Libelle d'annee pour les selecteurs ('2026 (partielle)' si l'annee est incomplete)."""
    return f"{year} (partielle)" if is_partial_year(df, year) else str(year)


def filter_kpis(df: pd.DataFrame, year: int | None, regions: Sequence[str]) -> pd.DataFrame:
    """Filtre par annee (None = toutes) et par codes region."""
    mask = df["region_code"].isin(list(regions))
    if year is not None:
        mask &= df["year"] == year
    return df.loc[mask].reset_index(drop=True)


def _mean(series: pd.Series) -> float | None:
    value = series.mean()
    return None if pd.isna(value) else float(value)


def summarize(df: pd.DataFrame) -> dict[str, float | None]:
    """Moyennes des KPI sur la selection (None si aucune valeur)."""
    return {
        "spi": _mean(df["spi_simplified"]),
        "ratio": _mean(df["precip_et0_ratio"]),
        "solar_score": _mean(df["solar_potential_score"]),
        "solar_kwh": _mean(df["solar_radiation_avg_kwh_m2_day"]),
        "dry_days": _mean(df["dry_days"]),
    }


def delta(current: float | None, previous: float | None) -> float | None:
    """Ecart courant - precedent (None si l'une des valeurs manque)."""
    if current is None or previous is None:
        return None
    return current - previous


def rank_regions(df: pd.DataFrame, column: str, *, ascending: bool, n: int = 5) -> pd.DataFrame:
    """Top-n des regions sur `column` (valeurs nulles exclues)."""
    ranked = df.loc[df[column].notna(), ["region_name", column]]
    return ranked.sort_values(column, ascending=ascending).head(n).reset_index(drop=True)


# --- Couleurs -------------------------------------------------------------------------------


def hex_to_rgb(color: str) -> tuple[int, int, int]:
    """'#rrggbb' -> (r, g, b)."""
    value = color.lstrip("#")
    return int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16)


def drought_color(drought_class: str | None) -> list[int]:
    """Couleur RGBA (liste d'entiers, format pydeck) d'une classe de secheresse."""
    hex_color = DROUGHT_CLASS_COLORS.get(drought_class or "", NO_DATA_COLOR)
    return [*hex_to_rgb(hex_color), 220]


def solar_color(score: float | None) -> list[int]:
    """Couleur RGBA interpolee sur la rampe jaune -> rouge pour un score 0-100."""
    if score is None or math.isnan(score):
        return [*hex_to_rgb(NO_DATA_COLOR), 220]
    t = min(max(score / 100.0, 0.0), 1.0)
    low, high = hex_to_rgb(SOLAR_RAMP_LOW), hex_to_rgb(SOLAR_RAMP_HIGH)
    r, g, b = (round(lo + (hi - lo) * t) for lo, hi in zip(low, high, strict=True))
    return [r, g, b, 220]


# --- Formatage ------------------------------------------------------------------------------


def format_number(value: float | None, digits: int = 2, suffix: str = "") -> str:
    """'—' si absent, sinon le nombre arrondi suivi du suffixe."""
    if value is None or math.isnan(value):
        return "—"
    return f"{value:.{digits}f}{suffix}"


def drought_label(drought_class: str | None) -> str:
    """Libelle francais d'une classe (NO_DATA_LABEL si absente)."""
    return DROUGHT_CLASS_LABELS.get(drought_class or "", NO_DATA_LABEL)


def _optional_float(value: Any) -> float | None:
    return None if pd.isna(value) else float(value)


def build_map_data(df: pd.DataFrame, mode: MapMode) -> pd.DataFrame:
    """Une ligne par region : position, couleur (RGBA) et texte d'infobulle."""
    rows: list[dict[str, Any]] = []
    for record in df.to_dict("records"):
        ratio = _optional_float(record["precip_et0_ratio"])
        score = _optional_float(record["solar_potential_score"])
        kwh = _optional_float(record["solar_radiation_avg_kwh_m2_day"])
        if mode == "drought":
            color = drought_color(record["drought_class"])
            label = f"{drought_label(record['drought_class'])} — P/ET0 {format_number(ratio, 2)}"
        else:
            color = solar_color(score)
            label = f"Score solaire {format_number(score, 1)} — {format_number(kwh, 2)} kWh/m²/j"
        rows.append(
            {
                "region_code": record["region_code"],
                "region_name": record["region_name"],
                "latitude": float(record["latitude"]),
                "longitude": float(record["longitude"]),
                "color": color,
                "label": label,
            }
        )
    return pd.DataFrame(
        rows, columns=["region_code", "region_name", "latitude", "longitude", "color", "label"]
    )


# --- Tableau et export ----------------------------------------------------------------------


def has_mock_agriculture(df: pd.DataFrame) -> bool:
    """True si une ligne de la vue porte des donnees agricoles synthetiques."""
    return bool(df["agriculture_is_mock"].any())


def display_table(df: pd.DataFrame) -> pd.DataFrame:
    """Vue detaillee : colonnes ordonnees et libelles francais (classe traduite)."""
    table = df.loc[:, list(DISPLAY_COLUMNS)].copy()
    table["drought_class"] = [drought_label(c) for c in df["drought_class"]]
    return table.rename(columns=DISPLAY_COLUMNS)


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    """CSV UTF-8 avec BOM (accents lisibles a l'ouverture dans Excel)."""
    return df.to_csv(index=False).encode("utf-8-sig")
