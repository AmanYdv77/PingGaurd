"""
Database Configuration & Session Providers.

Provides asynchronous engine/sessions for FastAPI and synchronous engine/sessions for Celery workers.
"""

import os
from contextlib import contextmanager
from pathlib import Path
from typing import AsyncGenerator, Generator
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker

# Load environment variables from .env if present
env_file = Path(__file__).resolve().parent.parent / ".env"
if env_file.exists():
    load_dotenv(dotenv_path=env_file)

# Default development PostgreSQL URL
DEFAULT_DB_URL = "postgresql+asyncpg://postgres:password@localhost:5432/pingguard"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DB_URL)

# Ensure asyncpg driver prefix is used for SQLAlchemy async engine
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)

# Asynchronous SQLAlchemy Engine for FastAPI
engine = create_async_engine(
    DATABASE_URL,
    pool_pre_ping=True,      # Discards broken/stale pooled connections
    pool_size=10,            # Sensible development pool size
    max_overflow=20,         # Maximum overflow connections during surges
    pool_recycle=1800,       # Recycle connections every 30 minutes
    echo=False,              # Set to True for verbose SQL logging if debugging
)

# Asynchronous Session Factory for FastAPI
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency yielding an independent AsyncSession per request.
    
    Guarantees automatic rollback on unhandled exceptions and cleans up
    the connection session when the request finishes.
    """
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


# =========================================================================
# Synchronous Database Engine & Session Provider for Celery Workers
# =========================================================================

# Translate asyncpg connection string to psycopg2 for synchronous workers
SYNC_DATABASE_URL = DATABASE_URL
if "+asyncpg" in SYNC_DATABASE_URL:
    SYNC_DATABASE_URL = SYNC_DATABASE_URL.replace("+asyncpg", "+psycopg2")
elif SYNC_DATABASE_URL.startswith("postgresql://") and "+psycopg2" not in SYNC_DATABASE_URL:
    SYNC_DATABASE_URL = SYNC_DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://", 1)
elif SYNC_DATABASE_URL.startswith("postgres://") and "+psycopg2" not in SYNC_DATABASE_URL:
    SYNC_DATABASE_URL = SYNC_DATABASE_URL.replace("postgres://", "postgresql+psycopg2://", 1)

# Synchronous Engine for Celery tasks
sync_engine = create_engine(
    SYNC_DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    pool_recycle=1800,
    echo=False,
)

# Synchronous Session Factory
SyncSessionLocal = sessionmaker(
    bind=sync_engine,
    autoflush=False,
    expire_on_commit=False,
)


@contextmanager
def get_sync_db() -> Generator[Session, None, None]:
    """
    Context manager providing an isolated synchronous SQLAlchemy Session for Celery tasks.
    
    Each background task executes within a controlled transaction:
    commits automatically on clean exit, rolls back on exceptions, and closes the session.
    """
    session = SyncSessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
