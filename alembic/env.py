import os
import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

# Ensure project root is in sys.path so 'app' can be imported
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.db import DATABASE_URL  # noqa: E402
from app.models import Base  # noqa: E402

# Alembic Config object
config = context.config

# Interpret the config file for Python logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Model's MetaData object for 'autogenerate' support
target_metadata = Base.metadata


def get_sync_url() -> str:
    """Convert asyncpg DATABASE_URL to psycopg2 for synchronous Alembic migrations."""
    db_url = os.getenv("DATABASE_URL", DATABASE_URL)
    if "+asyncpg" in db_url:
        return db_url.replace("+asyncpg", "+psycopg2")
    if db_url.startswith("postgresql://") and "+psycopg2" not in db_url:
        return db_url.replace("postgresql://", "postgresql+psycopg2://", 1)
    if db_url.startswith("postgres://") and "+psycopg2" not in db_url:
        return db_url.replace("postgres://", "postgresql+psycopg2://", 1)
    return db_url


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = get_sync_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    configuration = config.get_section(config.config_ini_section, {}) or {}
    configuration["sqlalchemy.url"] = get_sync_url()

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
