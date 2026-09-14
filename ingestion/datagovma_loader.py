"""Loader agriculture/hydrique data.gov.ma, avec fallback sur une fixture mock versionnee.

Aucune URL data.gov.ma stable ne fournit aujourd'hui une serie region x annee
compatible avec raw.agriculture_regional (le dataset national "Production
vegetale 2010-2022" n'a pas de dimension region). Le mode mock est donc le
comportement par defaut, pas un repli exceptionnel.
"""

import io
import logging
import math
import re
import unicodedata
from pathlib import Path
from typing import cast

import httpx
import pandas as pd

from ingestion.config import Settings, get_settings
from ingestion.regions import iso_codes

logger = logging.getLogger(__name__)

MOCK_FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "agriculture_maroc_mock.csv"

EXPECTED_COLUMNS = (
    "region_code",
    "annee",
    "production_tonnes",
    "surface_irriguee_ha",
    "ressource_hydrique_m3",
)

# Alias de colonnes -> nom canonique attendu par le loader.
COLUMN_ALIASES: dict[str, str] = {
    "region": "region_code",
    "region_code": "region_code",
    "region_ code": "region_code",
    "annee": "annee",
    "campagne": "annee",
    "occurrence": "annee",
    "production_tonnes": "production_tonnes",
    "production": "production_tonnes",
    "surface_irriguee_ha": "surface_irriguee_ha",
    "superficie_irriguee": "surface_irriguee_ha",
    "superficie": "surface_irriguee_ha",
    "ressource_hydrique_m3": "ressource_hydrique_m3",
    "eau_m3": "ressource_hydrique_m3",
    "ressource_hydrique": "ressource_hydrique_m3",
}

# Alias de libelles region -> code ISO 3166-2:MA. Inclut les anciennes regions
# (decoupage pre-2015) : mappees vers la region moderne correspondante par
# agregation historique assumee (ex: Souss-Massa-Draa -> Souss-Massa, MA-09).
REGION_ALIASES: dict[str, str] = {
    "tanger tetouan al hoceima": "MA-01",
    "tanger tetouan": "MA-01",
    "tanger assilah": "MA-01",
    "tanger": "MA-01",
    "l oriental": "MA-02",
    "oriental": "MA-02",
    "oujda": "MA-02",
    "fes meknes": "MA-03",
    "meknes tafilalet": "MA-03",
    "fes": "MA-03",
    "rabat sale kenitra": "MA-04",
    "gharb chrarda beni hssen": "MA-04",
    "rabat": "MA-04",
    "beni mellal khenifra": "MA-05",
    "beni mellal": "MA-05",
    "casablanca settat": "MA-06",
    "chaouia ouardigha": "MA-06",
    "casablanca": "MA-06",
    "marrakech safi": "MA-07",
    "marrakech tensift al haouz": "MA-07",
    "marrakech": "MA-07",
    "daraa tafilalet": "MA-08",
    "draa tafilalet": "MA-08",
    "errachidia": "MA-08",
    "souss massa": "MA-09",
    "souss massa draa": "MA-09",
    "agadir": "MA-09",
    "guelmim oued noun": "MA-10",
    "guelmim": "MA-10",
    "laayoune sakia el hamra": "MA-11",
    "laayoune saguia el hamra": "MA-11",
    "laayoune": "MA-11",
    "dakhla oued ed dahab": "MA-12",
    "oued ed dahab dakhla": "MA-12",
    "oued ed dahab lagouira": "MA-12",
    "oued eddahab": "MA-12",
    "dakhla": "MA-12",
}

_ISO_CODE_PATTERN = re.compile(r"^MA-\d{2}$")
_ENCODINGS_TO_TRY = ("utf-8", "latin-1", "cp1252")
_CAMPAIGN_YEAR_PATTERN = re.compile(r"^\s*(\d{4})\s*/\s*\d{4}\s*$")

# Espaces "exotiques" rencontres dans les nombres formates a la francaise.
_NARROW_NO_BREAK_SPACE = " "
_NO_BREAK_SPACE = " "


def _normalize_label(label: str) -> str:
    """Normalise un libelle : NFKD, minuscule, apostrophes/tirets -> espace, trim."""
    text = unicodedata.normalize("NFKD", label)
    text = "".join(char for char in text if not unicodedata.combining(char))
    text = text.lower()
    text = re.sub(r"[-'’]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _resolve_region_code(label: str) -> str | None:
    """Resout un libelle de region (nom FR, ancienne region, ou code ISO) en MA-xx.

    Retourne None si le libelle ne correspond a aucune region connue (l'appelant
    doit logger un WARNING et ignorer la ligne, pas lever d'exception).
    """
    stripped = label.strip().upper()
    if _ISO_CODE_PATTERN.match(stripped):
        return stripped
    return REGION_ALIASES.get(_normalize_label(label))


def _parse_decimal(raw: object) -> float | None:
    """Parse un nombre au format anglo-saxon ou francais ('1 234,56', '1234.56').

    `raw` est heterogene par nature (une colonne pandas melange str, NaN,
    numpy.float64/int64 selon ce que le CSV source contient). Retourne None
    si vide/NaN (a la charge de l'appelant de decider si c'est bloquant).
    Gere l'espace ASCII, l'espace insecable (U+00A0) et l'espace fine
    insecable (U+202F) comme separateurs de milliers.
    """
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        for space_char in (_NO_BREAK_SPACE, _NARROW_NO_BREAK_SPACE, " "):
            text = text.replace(space_char, "")
        if "," in text and "." in text:
            text = text.replace(".", "").replace(",", ".")
        elif "," in text:
            text = text.replace(",", ".")
        return float(text)
    if raw is None or (isinstance(raw, float) and math.isnan(raw)):
        return None
    return float(cast("int | float", raw))


def _parse_year(raw: object) -> int | None:
    """Parse une annee, y compris le format campagne agricole '2010/2011' (-> 2010).

    `raw` est heterogene (str, NaN, numpy.int64/float64), cf. _parse_decimal.
    Retourne None si vide/invalide (ligne a ignorer par l'appelant).
    """
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return None
        campaign_match = _CAMPAIGN_YEAR_PATTERN.match(text)
        if campaign_match:
            return int(campaign_match.group(1))
        try:
            return int(float(text))
        except ValueError:
            return None
    if raw is None or (isinstance(raw, float) and math.isnan(raw)):
        return None
    return int(cast("int | float", raw))


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Renomme les colonnes source vers les noms canoniques via COLUMN_ALIASES."""
    rename_map: dict[str, str] = {}
    for column in df.columns:
        key = _normalize_label(str(column)).replace(" ", "_")
        if key in COLUMN_ALIASES:
            rename_map[column] = COLUMN_ALIASES[key]
    return df.rename(columns=rename_map)


def _read_tabular_bytes(content: bytes) -> pd.DataFrame:
    """Lit un CSV depuis des bytes, avec sniffing du separateur et cascade d'encodage."""
    last_error: UnicodeDecodeError | None = None
    for encoding in _ENCODINGS_TO_TRY:
        try:
            text = content.decode(encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
            continue
        return pd.read_csv(io.StringIO(text), sep=None, engine="python")
    raise ValueError(f"Impossible de decoder le CSV avec {_ENCODINGS_TO_TRY}") from last_error


def _validate_columns(df: pd.DataFrame, origin: str) -> None:
    missing = [col for col in EXPECTED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans le CSV {origin}: {missing}")


def _clean_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Resout les codes region et parse annee/valeurs numeriques ; ignore les lignes invalides."""
    resolved_codes: list[str | None] = [_resolve_region_code(str(v)) for v in df["region_code"]]
    parsed_years: list[int | None] = [_parse_year(v) for v in df["annee"]]

    unknown_labels = {
        str(label)
        for label, code in zip(df["region_code"], resolved_codes, strict=True)
        if code is None
    }
    for label in unknown_labels:
        logger.warning("Region inconnue ignoree dans le CSV agriculture: %r", label)

    invalid_year_rows = sum(1 for year in parsed_years if year is None)
    if invalid_year_rows:
        logger.warning("%d ligne(s) avec annee invalide/vide ignoree(s)", invalid_year_rows)

    cleaned = df.assign(
        region_code=pd.Series(resolved_codes, index=df.index),
        annee=pd.Series(parsed_years, index=df.index),
    )
    cleaned = cleaned.dropna(subset=["region_code", "annee"])

    for column in ("production_tonnes", "surface_irriguee_ha", "ressource_hydrique_m3"):
        cleaned[column] = cleaned[column].map(_parse_decimal)

    cleaned["annee"] = cleaned["annee"].astype(int)

    missing_regions = set(iso_codes()) - set(cleaned["region_code"])
    if missing_regions:
        logger.warning("Regions absentes du CSV agriculture: %s", sorted(missing_regions))

    return cleaned[list(EXPECTED_COLUMNS)].reset_index(drop=True)


def _load_mock_fixture() -> pd.DataFrame:
    """Charge la fixture mock versionnee (ingestion/fixtures/agriculture_maroc_mock.csv)."""
    df = pd.read_csv(MOCK_FIXTURE_PATH)
    df = _normalize_columns(df)
    _validate_columns(df, origin=str(MOCK_FIXTURE_PATH))
    return _clean_rows(df)


def load_agriculture_regional(settings: Settings | None = None) -> tuple[pd.DataFrame, str]:
    """Charge les donnees agricoles/hydriques annuelles par region.

    Comportement :
    - `settings.datagovma_agriculture_url` vide -> fixture mock, source
      'mock-fallback'.
    - URL configuree mais inaccessible (reseau, timeout, 404, etc.) -> fixture
      mock, source 'mock-fallback', avec un WARNING logue.
    - URL configuree ET accessible mais colonnes attendues manquantes dans le
      CSV distant -> ValueError (fail fast, PAS de fallback : un CSV distant
      qui repond mais dont le schema a change est une erreur de configuration
      a corriger, pas une panne reseau a masquer).
    - Fixture mock elle-meme corrompue (colonnes manquantes) -> ValueError.

    Retourne (df, source) avec source in {'datagovma', 'mock-fallback'}, a
    propager tel quel dans upsert_agriculture_regional(df, source=source, ...).
    df expose exactement region_code, annee, production_tonnes,
    surface_irriguee_ha, ressource_hydrique_m3 ; les lignes a region ou annee
    invalide sont ignorees (WARNING), jamais une exception.
    """
    settings = settings or get_settings()
    url = settings.datagovma_agriculture_url

    if not url:
        logger.warning("DATAGOVMA_AGRICULTURE_URL non configuree, utilisation du mock.")
        return _load_mock_fixture(), "mock-fallback"

    try:
        response = httpx.get(url, timeout=30.0, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("Telechargement data.gov.ma echoue (%s), utilisation du mock.", exc)
        return _load_mock_fixture(), "mock-fallback"

    df = _read_tabular_bytes(response.content)
    df = _normalize_columns(df)
    _validate_columns(df, origin=url)
    return _clean_rows(df), "datagovma"
