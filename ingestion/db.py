"""Connexion Postgres et upserts idempotents vers les tables raw."""

import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import Any

import pandas as pd
from sqlalchemy import (
    TIMESTAMP,
    Column,
    Connection,
    Date,
    Engine,
    Integer,
    MetaData,
    Numeric,
    Table,
    Text,
    Uuid,
    create_engine,
    func,
)
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ingestion.config import Settings, get_settings

metadata = MetaData(schema="raw")

weather_daily = Table(
    "weather_daily",
    metadata,
    Column("region_code", Text, primary_key=True),
    Column("date", Date, primary_key=True),
    Column("temperature_2m_max", Numeric(5, 2)),
    Column("temperature_2m_min", Numeric(5, 2)),
    Column("temperature_2m_mean", Numeric(5, 2)),
    Column("precipitation_sum", Numeric(6, 2)),
    Column("et0_fao_evapotranspiration", Numeric(6, 2)),
    Column("shortwave_radiation_sum", Numeric(7, 2)),
    Column("relative_humidity_2m_mean", Numeric(5, 2)),
    Column("wind_speed_10m_max", Numeric(5, 2)),
    Column("_source", Text, nullable=False),
    Column("_source_url", Text),
    Column("_ingested_at", TIMESTAMP(timezone=True), nullable=False),
    Column("_batch_id", Uuid, nullable=False),
)

agriculture_regional = Table(
    "agriculture_regional",
    metadata,
    Column("region_code", Text, primary_key=True),
    Column("annee", Integer, primary_key=True),
    Column("production_tonnes", Numeric(12, 2)),
    Column("surface_irriguee_ha", Numeric(12, 2)),
    Column("ressource_hydrique_m3", Numeric(14, 2)),
    Column("_source", Text, nullable=False),
    Column("_source_url", Text),
    Column("_ingested_at", TIMESTAMP(timezone=True), nullable=False),
    Column("_batch_id", Uuid, nullable=False),
)


def get_engine(settings: Settings | None = None) -> Engine:
    """Construit l'Engine SQLAlchemy vers la base dwh (lazy, a reutiliser)."""
    settings = settings or get_settings()
    return create_engine(settings.dwh_dsn)


@contextmanager
def get_connection(
    engine: Engine | None = None,
    settings: Settings | None = None,
) -> Iterator[Connection]:
    """Ouvre une connexion transactionnelle vers dwh.

    Commit automatique en sortie de bloc, rollback si une exception est levee.
    Si `engine` est fourni (ex: SQLite in-memory en test), il est utilise tel
    quel et prime sur `settings` / `get_engine()`.
    """
    engine = engine or get_engine(settings)
    with engine.connect() as conn:
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()


def _upsert(
    conn: Connection,
    table: Table,
    df: pd.DataFrame,
    *,
    unique_cols: Sequence[str],
    batch_id: uuid.UUID,
    source: str,
    source_url: str | None,
) -> int:
    """Upsert generique (ON CONFLICT unique_cols DO UPDATE) partage par les 2 tables raw.

    Si `df` contient une colonne `source_url` (cas du client Open-Meteo, une URL
    exacte par chunk/ligne), elle prime ligne par ligne sur le parametre
    `source_url` : celui-ci ne sert alors que de repli si jamais cette colonne
    manquait. Sans cette colonne (cas agriculture, une seule source par appel),
    le parametre `source_url` s'applique tel quel a toutes les lignes.
    """
    if df.empty:
        return 0

    known_columns = {col.name for col in table.columns}
    data_columns = [c for c in df.columns if c in known_columns]
    per_row_source_urls = df["source_url"].tolist() if "source_url" in df.columns else None

    raw_records = df[data_columns].to_dict(orient="records")
    records: list[dict[str, Any]] = [
        {str(key): value for key, value in record.items()} for record in raw_records
    ]
    for index, record in enumerate(records):
        record["_source"] = source
        record["_source_url"] = (
            per_row_source_urls[index] if per_row_source_urls is not None else source_url
        )
        record["_batch_id"] = batch_id

    stmt = pg_insert(table).values(records)
    update_columns: dict[str, Any] = {
        col.name: stmt.excluded[col.name]
        for col in table.columns
        if col.name not in unique_cols and col.name != "_ingested_at"
    }
    update_columns["_ingested_at"] = func.now()
    stmt = stmt.on_conflict_do_update(index_elements=list(unique_cols), set_=update_columns)

    conn.execute(stmt)
    return len(records)


def upsert_weather_daily(
    conn: Connection,
    df: pd.DataFrame,
    *,
    batch_id: uuid.UUID,
    source: str = "open-meteo-archive",
    source_url: str | None = None,
) -> int:
    """Upsert idempotent (ON CONFLICT region_code,date DO UPDATE) vers raw.weather_daily.

    df doit contenir au minimum region_code, date et les variables climatiques
    definies dans le DDL. Les colonnes d'audit sont ajoutees ici, pas dans df.
    Un DataFrame vide est un no-op (retourne 0, aucune requete executee).
    """
    return _upsert(
        conn,
        weather_daily,
        df,
        unique_cols=("region_code", "date"),
        batch_id=batch_id,
        source=source,
        source_url=source_url,
    )


def upsert_agriculture_regional(
    conn: Connection,
    df: pd.DataFrame,
    *,
    batch_id: uuid.UUID,
    source: str,
    source_url: str | None = None,
) -> int:
    """Upsert idempotent (ON CONFLICT region_code,annee DO UPDATE) vers raw.agriculture_regional.

    df doit contenir au minimum region_code, annee et les colonnes metier
    definies dans le DDL. `source` est obligatoire (pas de defaut, cf. DDL).
    Un DataFrame vide est un no-op (retourne 0, aucune requete executee).
    """
    return _upsert(
        conn,
        agriculture_regional,
        df,
        unique_cols=("region_code", "annee"),
        batch_id=batch_id,
        source=source,
        source_url=source_url,
    )
