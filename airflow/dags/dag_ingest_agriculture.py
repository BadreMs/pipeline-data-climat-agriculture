"""DAG mensuel : ingestion agricole/hydrique (data.gov.ma ou mock-fallback) par region.

Pas de fenetre temporelle a calculer ici : load_agriculture_regional() ne
prend pas de plage de dates (contrairement a l'ingestion meteo). run.py
calcule quand meme --start/--end en interne mais ne les utilise jamais tant
que --skip-weather est actif.
"""

from datetime import timedelta

import pendulum
from airflow.operators.bash import BashOperator

from airflow import DAG

TIMEZONE = pendulum.timezone("Africa/Casablanca")

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_ingest_agriculture",
    description="Ingestion agricole/hydrique mensuelle (data.gov.ma ou mock-fallback).",
    schedule="0 4 1 * *",
    start_date=pendulum.datetime(2024, 1, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ingestion", "agriculture"],
) as dag:
    ingest_agriculture = BashOperator(
        task_id="ingest_agriculture",
        bash_command="bash /opt/airflow/scripts/run_ingestion.sh --skip-weather --verbose",
    )
