import os
from logging.config import fileConfig

from alembic import context
from dotenv import load_dotenv
from sqlalchemy import engine_from_config, pool, text
from sqlalchemy.dialects import registry
from sqlmodel import SQLModel

import sql  # noqa: F401  -- registers all tables + schema DDL hooks
from sql.a_bronze.schema import SCHEMA as BRONZE_SCHEMA
from sql.b_silver.schema import SCHEMA as SILVER_SCHEMA
from sql.c_gold.schema import SCHEMA as GOLD_SCHEMA

# bare postgresql:// resolves to psycopg3, not psycopg2 (which isn't installed in this venv)
registry.register("postgresql", "sqlalchemy.dialects.postgresql.psycopg", "dialect")

load_dotenv()

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

database_url = os.environ.get("DATABASE_URL")
if not database_url:
    raise RuntimeError("DATABASE_URL env var is required for Alembic")
config.set_main_option("sqlalchemy.url", database_url)

target_metadata = SQLModel.metadata
SCHEMAS = (BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA)


def ensure_schemas(connection) -> None:
    """CREATE SCHEMA IF NOT EXISTS for every layer before migrations run."""
    for name in SCHEMAS:
        connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {name}"))


def include_name(name, type_, parent_names) -> bool:
    """Restrict autogenerate to galactus-owned schemas; ignore airflow's public."""
    if type_ == "schema":
        return name in SCHEMAS
    return True


# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        include_schemas=True,
        include_name=include_name,
        version_table="galactus_alembic_version",
        version_table_schema="public",
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_schemas=True,
            include_name=include_name,
            version_table="galactus_alembic_version",
            version_table_schema="public",
        )

        with context.begin_transaction():
            ensure_schemas(connection)
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
