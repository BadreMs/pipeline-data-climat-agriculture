"""Configuration typee du module d'ingestion, lue depuis les variables d'environnement (.env)."""

from datetime import date
from functools import lru_cache

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Parametres de connexion et d'ingestion, avec des defauts alignes sur .env.example."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Postgres
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "pipeline"
    postgres_password: str = "changeme"
    postgres_dwh_db: str = "dwh"
    postgres_airflow_db: str = "airflow"

    # Open-Meteo
    openmeteo_base_url: str = "https://archive-api.open-meteo.com/v1/archive"
    openmeteo_start_date: date = date(2015, 1, 1)
    openmeteo_end_date: date | None = None
    openmeteo_sleep_between_requests: float = 1.0

    # data.gov.ma
    datagovma_agriculture_url: str | None = None
    datagovma_irrigation_url: str | None = None
    datagovma_barrages_url: str | None = None
    datagovma_use_mock_fallback: bool = True

    @field_validator("openmeteo_end_date", mode="before")
    @classmethod
    def _blank_end_date_to_none(cls, value: object) -> object:
        """.env peut definir OPENMETEO_END_DATE= (vide) pour designer 'aujourd'hui'."""
        if value == "":
            return None
        return value

    @property
    def dwh_dsn(self) -> str:
        """DSN SQLAlchemy vers la base dwh (schemas raw/staging/marts)."""
        return (
            f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_dwh_db}"
        )


@lru_cache
def get_settings() -> Settings:
    """Instance memoisee des settings (evite de reparser .env a chaque appel)."""
    return Settings()
