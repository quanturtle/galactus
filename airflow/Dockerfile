FROM apache/airflow:3.1.8-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /home/airflow/galactus

COPY --chown=airflow:root pyproject.toml uv.lock alembic.ini ./
COPY --chown=airflow:root galactus ./galactus
COPY --chown=airflow:root sql ./sql
COPY --chown=airflow:root migrations ./migrations
COPY --chown=airflow:root configs ./configs

ENV UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/home/airflow/galactus/.venv
RUN uv venv --python 3.12 /home/airflow/galactus/.venv \
    && uv sync --frozen --no-dev \
    && ln -sf /home/airflow/galactus/.venv/bin/galactus /usr/local/bin/galactus \
    && chown -R airflow:root /home/airflow/galactus

# pre-create the logs dir owned by the airflow user (uid 50000) so a fresh
# airflow_logs named volume inherits that ownership instead of mounting as
# root:root, which would block the uid-50000 process from writing task logs
RUN mkdir -p /home/airflow/logs && chown airflow:root /home/airflow/logs

USER airflow
