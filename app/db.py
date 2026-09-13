"""
Database Configuration & Asynchronous Session Provider — Chapter 2: The Persistence Layer

Establishes the SQLAlchemy 2.0 asynchronous engine and session factory.
Provides the `get_db()` dependency for FastAPI route handlers.
"""

import os
from pathlib import Path
from typing import AsyncGenerator
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

# Load environment variables from .env if present
env_file = Path(__file__).resolve().parent.parent / ".env"
if env_file.exists():
    load_dotenv(dotenv_path=env_file)

# Default to development PostgreSQL URL if not set in environment
DEFAULT_DB_URL = "postgresql+asyncpg://postgres:password@localhost:5432/pingguard"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DB_URL)

# Ensure asyncpg driver prefix is used for SQLAlchemy async engine
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)

# Asynchronous SQLAlchemy Engine
engine = create_async_engine(
    DATABASE_URL,
    pool_pre_ping=True,      # Discards broken/stale pooled connections
    pool_size=10,            # Sensible development pool size
    max_overflow=20,         # Maximum overflow connections during surges
    pool_recycle=1800,       # Recycle connections every 30 minutes
    echo=False,              # Set to True for verbose SQL logging if debugging
)

# Asynchronous Session Factory
# expire_on_commit=False prevents extra SELECTs when accessing attributes after commit
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
