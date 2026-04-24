"""Alembic environment — sync psycopg3 driver, DATABASE_URL from env."""

import os
import sys
from logging.config import fileConfig
from pathlib import Path

from sqlalchemy import engine_from_config, pool

from alembic import context

sys.path.insert(0, str(Path(__file__).resolve().parent))

from schema import metadata  # noqa: E402

from galactus.db import DEFAULT_DATABASE_URL  # noqa: E402

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = metadata

MANAGED_SCHEMAS = {"bronze", "silver"}


def _include_name(name, type_, parent_names):
    if type_ == "schema":
        return name in MANAGED_SCHEMAS
    return True


def _include_object(object, name, type_, reflected, compare_to):
    if type_ == "table":
        return getattr(object, "schema", None) in MANAGED_SCHEMAS
    return True


_AUTOGEN_OPTS = dict(
    target_metadata=target_metadata,
    include_schemas=True,
    include_name=_include_name,
    include_object=_include_object,
    compare_type=True,
    compare_server_default=True,
)


def _database_url() -> str:
    url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    # SQLAlchemy defaults postgresql:// to psycopg2; force psycopg3 to match
    # the rest of the project.
    if url.startswith("postgresql://"):
        url = "postgresql+psycopg://" + url[len("postgresql://") :]
    return url


def run_migrations_offline() -> None:
    context.configure(
        url=_database_url(),
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        **_AUTOGEN_OPTS,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section, {})
    section["sqlalchemy.url"] = _database_url()
    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, **_AUTOGEN_OPTS)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
