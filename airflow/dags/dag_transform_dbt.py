"""DAG quotidien : transformations dbt (seed -> run -> test) sur le DWH.

Planifie a 04:30 (Africa/Casablanca), apres l'ingestion meteo de 03:00 et sans
chevaucher l'ingestion agriculture mensuelle (04:00, le 1er du mois).
dbt deps est execute a chaque run (idempotent) : dbt_packages/ est ignore par git et
n'est pas dans l'image. Le DAG suppose que le projet est monte dans /opt/airflow/dbt_project.
"""

from datetime import timedelta

import pendulum
from airflow.operators.bash import BashOperator

from airflow import DAG

TIMEZONE = pendulum.timezone("Africa/Casablanca")

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_ARGS = f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_transform_dbt",
    description="Transformations dbt (seed -> run -> test) : staging, intermediate, marts.",
    schedule="30 4 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["transform", "dbt", "phase-3"],
) as dag:
    dbt_deps = BashOperator(task_id="dbt_deps", bash_command=f"dbt deps {DBT_ARGS}")
    dbt_seed = BashOperator(task_id="dbt_seed", bash_command=f"dbt seed {DBT_ARGS}")
    dbt_run = BashOperator(task_id="dbt_run", bash_command=f"dbt run {DBT_ARGS}")
    dbt_test = BashOperator(task_id="dbt_test", bash_command=f"dbt test {DBT_ARGS}")
    dbt_deps >> dbt_seed >> dbt_run >> dbt_test
