"""Lecture des marts dbt dans Postgres (schema marts) pour le dashboard."""

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from dashboard.metrics import merge_kpis
from ingestion.db import get_engine

CLIMATE_QUERY = text("SELECT * FROM marts.fct_region_climate_kpi")
SOLAR_QUERY = text("SELECT * FROM marts.fct_solar_potential")


def load_region_year_kpis(engine: Engine | None = None) -> pd.DataFrame:
    """Charge les deux marts et les fusionne (1 ligne = region x annee)."""
    engine = engine or get_engine()
    with engine.connect() as connection:
        climate = pd.read_sql(CLIMATE_QUERY, connection)
        solar = pd.read_sql(SOLAR_QUERY, connection)
    return merge_kpis(climate, solar)
