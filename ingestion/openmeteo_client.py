"""Client Open-Meteo archive API : ingestion climatique journaliere par region."""

import logging
from collections.abc import Sequence
from dataclasses import dataclass, fields
from datetime import date, timedelta
from time import sleep
from types import TracebackType
from typing import Any

import httpx
import pandas as pd
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from ingestion.config import Settings, get_settings
from ingestion.regions import Region

logger = logging.getLogger(__name__)

RowValue = str | float | date | None

DAILY_VARIABLES = (
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "et0_fao_evapotranspiration",
    "shortwave_radiation_sum",
    "relative_humidity_2m_mean",
    "wind_speed_10m_max",
)

RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
CHUNK_MAX_YEARS = 2
TIMEZONE = "Africa/Casablanca"


@dataclass(frozen=True)
class WeatherDailyRecord:
    """Une observation climatique journaliere pour une region (raw.weather_daily)."""

    region_code: str
    date: date
    temperature_2m_max: float | None
    temperature_2m_min: float | None
    temperature_2m_mean: float | None
    precipitation_sum: float | None
    et0_fao_evapotranspiration: float | None
    shortwave_radiation_sum: float | None
    relative_humidity_2m_mean: float | None
    wind_speed_10m_max: float | None
    source_url: str

    def to_row(self) -> dict[str, RowValue]:
        """Represente l'enregistrement en dict plat (cle = colonne raw.weather_daily)."""
        return {
            "region_code": self.region_code,
            "date": self.date,
            "temperature_2m_max": self.temperature_2m_max,
            "temperature_2m_min": self.temperature_2m_min,
            "temperature_2m_mean": self.temperature_2m_mean,
            "precipitation_sum": self.precipitation_sum,
            "et0_fao_evapotranspiration": self.et0_fao_evapotranspiration,
            "shortwave_radiation_sum": self.shortwave_radiation_sum,
            "relative_humidity_2m_mean": self.relative_humidity_2m_mean,
            "wind_speed_10m_max": self.wind_speed_10m_max,
            "source_url": self.source_url,
        }


@dataclass(frozen=True)
class FailedChunk:
    """Un chunk (region, periode) ayant echoue definitivement apres retries epuises."""

    region_code: str
    chunk_start: date
    chunk_end: date
    error: str


def _is_retryable(exc: BaseException) -> bool:
    """Retry sur timeout et sur 5xx/429 uniquement ; jamais sur une erreur 4xx definitive."""
    if isinstance(exc, httpx.TimeoutException):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in RETRYABLE_STATUS_CODES
    return False


def _split_into_chunks(start: date, end: date) -> list[tuple[date, date]]:
    """Decoupe [start, end] (bornes incluses) en chunks fermes de 2 ans maximum."""
    if start > end:
        return []

    chunks: list[tuple[date, date]] = []
    chunk_start = start
    while chunk_start <= end:
        try:
            naive_chunk_end = chunk_start.replace(
                year=chunk_start.year + CHUNK_MAX_YEARS
            ) - timedelta(days=1)
        except ValueError:
            # 29 fevrier sans equivalent bissextile 2 ans plus tard : repli au 28 fevrier.
            naive_chunk_end = chunk_start.replace(
                month=2, day=28, year=chunk_start.year + CHUNK_MAX_YEARS
            ) - timedelta(days=1)
        chunk_end = min(naive_chunk_end, end)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end + timedelta(days=1)
    return chunks


def _to_optional_float(value: float | int | None) -> float | None:
    return None if value is None else float(value)


def _parse_daily_payload(
    region_code: str, source_url: str, payload: dict[str, Any]
) -> list[WeatherDailyRecord]:
    """Parse le payload JSON Open-Meteo (cle 'daily') en WeatherDailyRecord.

    Ignore volontairement latitude/longitude/elevation de la reponse (l'API renvoie
    les coordonnees de sa maille modele la plus proche, pas celles demandees) ;
    seul region_code, fourni par l'appelant, est propage. `source_url` est l'URL
    exacte (avec query string) de la requete ayant produit ce payload : propagee
    telle quelle sur chaque WeatherDailyRecord du chunk pour tracabilite exacte.
    """
    daily = payload["daily"]
    return [
        WeatherDailyRecord(
            region_code=region_code,
            date=date.fromisoformat(iso_date),
            temperature_2m_max=_to_optional_float(daily["temperature_2m_max"][i]),
            temperature_2m_min=_to_optional_float(daily["temperature_2m_min"][i]),
            temperature_2m_mean=_to_optional_float(daily["temperature_2m_mean"][i]),
            precipitation_sum=_to_optional_float(daily["precipitation_sum"][i]),
            et0_fao_evapotranspiration=_to_optional_float(
                daily["et0_fao_evapotranspiration"][i]
            ),
            shortwave_radiation_sum=_to_optional_float(daily["shortwave_radiation_sum"][i]),
            relative_humidity_2m_mean=_to_optional_float(
                daily["relative_humidity_2m_mean"][i]
            ),
            wind_speed_10m_max=_to_optional_float(daily["wind_speed_10m_max"][i]),
            source_url=source_url,
        )
        for i, iso_date in enumerate(daily["time"])
    ]


def records_to_dataframe(records: Sequence[WeatherDailyRecord]) -> pd.DataFrame:
    """Convertit une liste de WeatherDailyRecord en DataFrame pret pour upsert_weather_daily."""
    if not records:
        return pd.DataFrame(columns=[f.name for f in fields(WeatherDailyRecord)])
    return pd.DataFrame([record.to_row() for record in records])


class OpenMeteoClient:
    """Client pour l'API archive Open-Meteo, avec retry et decoupage temporel par chunks."""

    def __init__(
        self, settings: Settings | None = None, client: httpx.Client | None = None
    ) -> None:
        """Initialise le client.

        `client` est injectable (tests via respx, ou reutilisation d'un client
        existant) : dans ce cas il n'est jamais ferme par cette instance. Sinon,
        un httpx.Client interne est cree et ferme par `close()`/`__exit__`.
        """
        self._settings = settings or get_settings()
        self._owns_client = client is None
        self._client = client if client is not None else httpx.Client(timeout=30.0)
        self._failed_chunks: list[FailedChunk] = []

    def __enter__(self) -> "OpenMeteoClient":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Ferme le httpx.Client interne (no-op si le client a ete injecte par l'appelant)."""
        if self._owns_client:
            self._client.close()

    @property
    def failed_chunks(self) -> list[FailedChunk]:
        """Chunks (region, periode) ayant echoue definitivement apres retries epuises."""
        return list(self._failed_chunks)

    def fetch_region(self, region: Region, start: date, end: date) -> list[WeatherDailyRecord]:
        """Recupere l'historique meteo d'une region sur [start, end] (bornes incluses).

        Decoupe automatiquement en chunks fermes de 2 ans maximum. Un chunk en echec
        definitif (retries epuises ou erreur 4xx) est loggue et ajoute a
        `failed_chunks` ; les chunks suivants de la region continuent d'etre tentes.
        """
        records: list[WeatherDailyRecord] = []
        for chunk_start, chunk_end in _split_into_chunks(start, end):
            try:
                records.extend(self._fetch_chunk(region, chunk_start, chunk_end))
            except Exception as exc:  # tenacity a deja retente : ceci est l'echec final
                logger.error(
                    "Echec definitif Open-Meteo region=%s chunk=%s..%s: %s",
                    region.iso_code,
                    chunk_start,
                    chunk_end,
                    exc,
                )
                self._failed_chunks.append(
                    FailedChunk(region.iso_code, chunk_start, chunk_end, str(exc))
                )
            finally:
                sleep(self._settings.openmeteo_sleep_between_requests)
        return records

    def fetch_regions(
        self, regions: Sequence[Region], start: date, end: date
    ) -> list[WeatherDailyRecord]:
        """Enchaine fetch_region sur plusieurs regions.

        Une region entierement en echec n'empeche pas le traitement des suivantes.
        """
        records: list[WeatherDailyRecord] = []
        for region in regions:
            records.extend(self.fetch_region(region, start, end))
        return records

    @retry(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=2, max=32),
        reraise=True,
    )
    def _fetch_chunk(
        self, region: Region, chunk_start: date, chunk_end: date
    ) -> list[WeatherDailyRecord]:
        """Effectue une requete HTTP pour un chunk et parse la reponse. Retry gere ici."""
        params: dict[str, str | float] = {
            "latitude": region.latitude,
            "longitude": region.longitude,
            "start_date": chunk_start.isoformat(),
            "end_date": chunk_end.isoformat(),
            "daily": ",".join(DAILY_VARIABLES),
            "timezone": TIMEZONE,
        }
        response = self._client.get(self._settings.openmeteo_base_url, params=params)
        response.raise_for_status()
        source_url = str(response.request.url)
        return _parse_daily_payload(region.iso_code, source_url, response.json())
