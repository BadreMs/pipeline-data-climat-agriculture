"""DAG squelette (active en Phase 3) : transformations dbt (raw -> staging -> marts).

schedule=None : jamais declenche automatiquement. dbt_project/ ne contient
encore que des .gitkeep (Phase 1) - dbt_project.yml/profiles.yml et les
modeles arrivent en Phase 3. Ce fichier fixe la structure attendue (2 taches
sequentielles dbt run -> dbt test) sans risque : sans schedule, aucune
execution automatique ne peut etre tentee contre un projet dbt inexistant.
"""

import pendulum
from airflow.operators.bash import BashOperator

from airflow import DAG

TIMEZONE = pendulum.timezone("Africa/Casablanca")

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

with DAG(
    dag_id="dag_transform_dbt",
    description="Transformations dbt (raw -> staging -> marts). Squelette Phase 3.",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    tags=["transform", "dbt", "phase-3"],
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR}",
    )
    dbt_run >> dbt_test
