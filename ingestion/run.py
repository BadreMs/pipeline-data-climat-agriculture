"""CLI d'ingestion : Open-Meteo (climat) + data.gov.ma (agriculture) -> Postgres raw."""

import argparse
import json
import logging
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, timedelta

from sqlalchemy.engine import Connection
from sqlalchemy.exc import SQLAlchemyError

from ingestion.config import Settings, get_settings
from ingestion.datagovma_loader import load_agriculture_regional
from ingestion.db import get_connection, upsert_agriculture_regional, upsert_weather_daily
from ingestion.openmeteo_client import FailedChunk, OpenMeteoClient, records_to_dataframe
from ingestion.regions import REGIONS, Region, get_region

logger = logging.getLogger(__name__)

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
DRY_RUN_WINDOW_DAYS = 7
DEGRADED_AGRICULTURE_SOURCE = "mock-fallback-network-error"


@dataclass(frozen=True)
class CliArgs:
    """Arguments CLI valides et resolus (post argparse)."""

    start: date
    end: date
    regions: list[Region]
    skip_weather: bool
    skip_agriculture: bool
    dry_run: bool
    verbose: bool


def parse_args(argv: Sequence[str] | None = None, settings: Settings | None = None) -> CliArgs:
    """Parse et valide les arguments CLI.

    `--start` est optionnel : si omis, on utilise settings.openmeteo_start_date
    (.env) et un WARNING est loggue. `--end` optionnel, defaut =
    settings.openmeteo_end_date ou date.today(). `--regions` optionnel,
    defaut = les 12 (ordre nord-sud), ou [MA-04] seul si --dry-run est present
    et --regions omis. Un code region inconnu ou --end < --start declenche
    parser.error(...), qui leve SystemExit(2).
    """
    settings = settings or get_settings()
    parser = argparse.ArgumentParser(
        prog="python -m ingestion.run",
        description=(
            "Ingestion Open-Meteo (climat) et data.gov.ma (agriculture) vers Postgres raw."
        ),
    )
    parser.add_argument("--start", type=date.fromisoformat, default=None)
    parser.add_argument("--end", type=date.fromisoformat, default=None)
    parser.add_argument("--regions", type=str, default=None)
    parser.add_argument("--skip-weather", action="store_true")
    parser.add_argument("--skip-agriculture", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    namespace = parser.parse_args(argv)

    if namespace.start is None:
        start = settings.openmeteo_start_date
        logger.warning("--start omis, utilisation du defaut %s depuis .env", start)
    else:
        start = namespace.start

    end = namespace.end or settings.openmeteo_end_date or date.today()
    if end < start:
        parser.error(f"--end ({end}) est anterieur a --start ({start})")

    regions: list[Region]
    if namespace.regions is None:
        regions = [get_region("MA-04")] if namespace.dry_run else list(REGIONS)
    else:
        codes = [code.strip().upper() for code in namespace.regions.split(",") if code.strip()]
        regions = []
        for code in codes:
            try:
                regions.append(get_region(code))
            except KeyError:
                parser.error(f"Code region inconnu: {code!r} (attendu: MA-01..MA-12)")
        if not regions:
            parser.error("--regions ne peut pas etre une liste vide")

    return CliArgs(
        start=start,
        end=end,
        regions=regions,
        skip_weather=namespace.skip_weather,
        skip_agriculture=namespace.skip_agriculture,
        dry_run=namespace.dry_run,
        verbose=namespace.verbose,
    )


def run_weather(
    client: OpenMeteoClient,
    regions: Sequence[Region],
    start: date,
    end: date,
    conn: Connection,
    batch_id: uuid.UUID,
) -> tuple[int, list[FailedChunk]]:
    """Fetch Open-Meteo pour les regions donnees puis upsert vers raw.weather_daily.

    Chaque WeatherDailyRecord porte deja sa propre `source_url` exacte (URL du
    chunk qui l'a produit) : upsert_weather_daily l'utilise ligne par ligne,
    aucun `source_url` generique n'est passe ici. Retourne (lignes upsertees,
    chunks en echec definitif).
    """
    records = client.fetch_regions(regions, start, end)
    df = records_to_dataframe(records)
    rows = upsert_weather_daily(conn, df, batch_id=batch_id)
    return rows, client.failed_chunks


def run_agriculture(settings: Settings, conn: Connection, batch_id: uuid.UUID) -> tuple[int, str]:
    """Charge (load_agriculture_regional) puis upsert vers raw.agriculture_regional.

    Retourne (lignes upsertees, source) avec source in {'datagovma',
    'mock-not-configured', 'mock-fallback-network-error'}.
    """
    df, source = load_agriculture_regional(settings)
    rows = upsert_agriculture_regional(
        conn, df, batch_id=batch_id, source=source, source_url=settings.datagovma_agriculture_url
    )
    return rows, source


def run_dry_run(regions: Sequence[Region], settings: Settings) -> int:
    """Mode --dry-run : vrai appel Open-Meteo sur 7 jours (today-6..today).

    AUCUNE ecriture DB, AUCUN appel au loader agriculture (pas pertinent ici).
    Affiche les enregistrements en JSON pretty sur stdout. Retourne toujours 0
    (c'est un test technique de connectivite, pas une validation de donnees).
    """
    end = date.today()
    start = end - timedelta(days=DRY_RUN_WINDOW_DAYS - 1)
    with OpenMeteoClient(settings) as client:
        records = client.fetch_regions(regions, start, end)
    payload = [record.to_row() for record in records]
    print(json.dumps(payload, indent=2, default=str))
    return 0


def print_summary(
    weather_rows: int,
    agriculture_rows: int,
    failed_chunks: Sequence[FailedChunk],
    agriculture_source: str,
    batch_id: uuid.UUID,
) -> None:
    """Affiche le recap final humain sur stdout."""
    print(
        f"{weather_rows} lignes weather upsertees / "
        f"{agriculture_rows} lignes agriculture upsertees / "
        f"{len(failed_chunks)} chunks en echec / "
        f"batch_id={batch_id} / "
        f"agriculture_source={agriculture_source}"
    )


def orchestrate(args: CliArgs, settings: Settings | None = None) -> int:
    """Enchaine run_weather / run_agriculture avec un seul batch_id partage.

    Chaque etape ouvre sa propre transaction (get_connection separe) : un
    echec sur l'agriculture ne fait pas perdre un upsert meteo deja reussi.
    Catch ValueError (loader) / SQLAlchemyError (DB) -> exit 2. Retourne 1 si
    des chunks meteo ont echoue ou si l'agriculture est en
    'mock-fallback-network-error' (degrade) ; 'mock-not-configured' reste un
    succes normal (exit 0). Sinon 0.
    """
    settings = settings or get_settings()
    if args.dry_run:
        return run_dry_run(args.regions, settings)

    batch_id = uuid.uuid4()
    logger.info("Debut ingestion batch_id=%s", batch_id)

    weather_rows = 0
    failed_chunks: list[FailedChunk] = []
    agriculture_rows = 0
    agriculture_source = "skipped"

    try:
        if not args.skip_weather:
            with OpenMeteoClient(settings) as client, get_connection(settings=settings) as conn:
                weather_rows, failed_chunks = run_weather(
                    client, args.regions, args.start, args.end, conn, batch_id
                )

        if not args.skip_agriculture:
            with get_connection(settings=settings) as conn:
                agriculture_rows, agriculture_source = run_agriculture(settings, conn, batch_id)
    except (ValueError, SQLAlchemyError) as exc:
        logger.error("Erreur fatale: %s", exc)
        return 2

    print_summary(weather_rows, agriculture_rows, failed_chunks, agriculture_source, batch_id)

    if failed_chunks or agriculture_source == DEGRADED_AGRICULTURE_SOURCE:
        return 1
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Point d'entree CLI (python -m ingestion.run)."""
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    args = parse_args(argv)
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    return orchestrate(args)


if __name__ == "__main__":
    raise SystemExit(main())
