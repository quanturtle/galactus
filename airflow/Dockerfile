FROM apache/airflow:2.10.3-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /home/airflow/galactus

COPY --chown=airflow:root pyproject.toml uv.lock ./
COPY --chown=airflow:root galactus ./galactus
COPY --chown=airflow:root configs ./configs

ENV UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/home/airflow/galactus/.venv
RUN uv venv --python 3.12 /home/airflow/galactus/.venv \
    && uv sync --frozen --no-dev \
    && ln -sf /home/airflow/galactus/.venv/bin/galactus /usr/local/bin/galactus \
    && chown -R airflow:root /home/airflow/galactus

USER airflow
