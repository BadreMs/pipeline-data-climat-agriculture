"""DAG quotidien : ingestion climatique Open-Meteo pour les 12 regions.

Le backfill historique complet (2015 -> aujourd'hui) se fait manuellement en
UNE commande (cf. README "Ingestion locale" / `make ingest`) : OpenMeteoClient
decoupe deja la periode en chunks de 2 ans cote client, un backfill via
`airflow dags backfill` rejouerait ce DAG une fois par jour depuis 2015 pour
un gain nul (des milliers de requetes minuscules au lieu de quelques dizaines
de chunks). Ce DAG ne gere que l'increment quotidien qui suit ce backfill.

Une seule tache couvre les 12 regions (pas de task-par-region) : le volume
quotidien est petit (fenetre de quelques jours x 12 regions, toujours 1
requete/region grace au chunking cote client), la parallelisation Airflow
n'apporterait ici que de l'overhead.
"""

from datetime import timedelta

import pendulum
from airflow.operators.bash import BashOperator

from airflow import DAG

TIMEZONE = pendulum.timezone("Africa/Casablanca")

# Fenetre glissante : aujourd'hui + les WEATHER_WINDOW_DAYS - 1 jours precedents.
# Couvre le delai de consolidation observe sur l'API archive Open-Meteo (Phase 1) :
# les 1-2 derniers jours renvoient souvent des valeurs null lors du premier passage.
WEATHER_WINDOW_DAYS = 5

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_ingest_openmeteo",
    description="Ingestion climatique quotidienne (Open-Meteo) pour les 12 regions du Maroc.",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2015, 1, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ingestion", "weather"],
) as dag:
    ingest_weather = BashOperator(
        task_id="ingest_weather",
        bash_command=(
            "bash /opt/airflow/scripts/run_ingestion.sh "
            "--start {{ macros.ds_add(ds, -4) }} "
            "--end {{ ds }} "
            "--skip-agriculture "
            "--verbose"
        ),
    )
