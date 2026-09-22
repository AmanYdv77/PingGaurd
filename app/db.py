"""
Database Configuration & Session Providers.

Provides asynchronous engine/sessions for FastAPI and synchronous engine/sessions for Celery workers.
"""

from contextlib import contextmanager
from typing import AsyncGenerator, Generator
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker
from app.config import get_settings

# Centralised application settings
settings = get_settings()
DATABASE_URL = settings.async_database_url
SYNC_DATABASE_URL = settings.sync_database_url

# Asynchronous SQLAlchemy Engine for FastAPI
engine = create_async_engine(
    DATABASE_URL,
    pool_pre_ping=True,  # Discards broken/stale pooled connections
    pool_size=10,  # Sensible development pool size
    max_overflow=20,  # Maximum overflow connections during surges
    pool_recycle=1800,  # Recycle connections every 30 minutes
    echo=False,  # Set to True for verbose SQL logging if debugging
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
        yield session


# =========================================================================
# Synchronous Database Engine & Session Provider for Celery Workers
# =========================================================================

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
