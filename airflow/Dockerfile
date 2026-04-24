FROM apache/airflow:2.10.3-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

USER airflow
WORKDIR /opt/galactus

COPY --chown=airflow:root pyproject.toml alembic.ini ./
COPY --chown=airflow:root src ./src
COPY --chown=airflow:root noticias ./noticias
COPY --chown=airflow:root supermercados ./supermercados
COPY --chown=airflow:root migrations ./migrations

RUN pip install --no-cache-dir -e .
