FROM apache/airflow:2.10.3-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /opt/galactus

COPY --chown=airflow:root pyproject.toml requirements.txt alembic.ini ./
COPY --chown=airflow:root src ./src
COPY --chown=airflow:root noticias ./noticias
COPY --chown=airflow:root supermercados ./supermercados
COPY --chown=airflow:root migrations ./migrations

ENV UV_LINK_MODE=copy
RUN uv venv --python 3.12 /opt/galactus/.venv \
    && uv pip install --python /opt/galactus/.venv/bin/python --no-cache -r requirements.txt \
    && uv pip install --python /opt/galactus/.venv/bin/python --no-cache --no-deps -e . \
    && ln -sf /opt/galactus/.venv/bin/galactus /usr/local/bin/galactus \
    && chown -R airflow:root /opt/galactus

USER airflow
