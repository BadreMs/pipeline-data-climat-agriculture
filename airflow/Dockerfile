FROM apache/airflow:2.9.3-python3.11

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Le package ingestion/ est copie et installe pour etre importable depuis les DAGs.
COPY --chown=airflow:root pyproject.toml /opt/airflow/project/pyproject.toml
COPY --chown=airflow:root ingestion /opt/airflow/project/ingestion

RUN pip install --no-cache-dir "/opt/airflow/project" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
