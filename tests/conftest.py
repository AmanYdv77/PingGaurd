"""
Pytest configuration and safety guard for PingGuard test suite.

Ensures tests run strictly against an isolated test database (name ending in '_test').
"""

import os
from pathlib import Path
import pytest
from sqlalchemy.engine import make_url
from alembic import command
from alembic.config import Config

# -----------------------------------------------------------------------------
# HARD SAFETY GUARD (Runs at import time before any `app` modules are imported)
# -----------------------------------------------------------------------------
test_db_url = os.environ.get("TEST_DATABASE_URL")
if not test_db_url:
    pytest.exit(
        "TEST_DATABASE_URL is not set. Set it before running tests, e.g.:\n"
        "$env:TEST_DATABASE_URL = 'postgresql+asyncpg://postgres:testpass@localhost:55432/pingguard_test'",
        returncode=2,
    )

try:
    parsed_url = make_url(test_db_url)
except Exception as exc:
    pytest.exit(
        f"TEST_DATABASE_URL could not be parsed as a valid database URL: {exc}",
        returncode=2,
    )

if not parsed_url.database or not parsed_url.database.endswith("_test"):
    pytest.exit(
        f"TEST_DATABASE_URL database name '{parsed_url.database}' must end with '_test' "
        "to prevent accidental data loss in development/production databases.",
        returncode=2,
    )

# Normalize localhost to 127.0.0.1 to avoid Windows Docker IPv6 (::1) connection drops
normalized_db_url = test_db_url
if "@localhost:" in normalized_db_url:
    normalized_db_url = normalized_db_url.replace("@localhost:", "@127.0.0.1:")

TEST_API_KEY = "test-static-api-key-at-least-24-chars-long"

# Only after guard passes, point DATABASE_URL to normalized TEST_DATABASE_URL
os.environ["DATABASE_URL"] = normalized_db_url
os.environ["ENVIRONMENT"] = "test"
os.environ["API_KEY"] = TEST_API_KEY


@pytest.fixture(scope="session", autouse=True)
def run_migrations():
    """Apply Alembic migrations to the isolated test database before running tests."""
    project_root = Path(__file__).resolve().parent.parent
    ini_path = project_root / "alembic.ini"
    alembic_cfg = Config(str(ini_path))
    command.upgrade(alembic_cfg, "head")


@pytest.fixture
def client():
    """Unauthenticated FastAPI TestClient."""
    from fastapi.testclient import TestClient
    import app.main
    with TestClient(app.main.app) as c:
        yield c


@pytest.fixture
def auth_client():
    """FastAPI TestClient pre-configured with the valid X-API-Key header."""
    from fastapi.testclient import TestClient
    import app.main
    with TestClient(app.main.app, headers={"X-API-Key": TEST_API_KEY}) as c:
        yield c


@pytest.fixture
def db_session():
    """Synchronous SQLAlchemy Session connected to the test database."""
    from app.db import get_sync_db
    with get_sync_db() as session:
        yield session


@pytest.fixture(autouse=True)
def cleanup_database():
    """Truncate tables before each test run against the verified test database."""
    from sqlalchemy import text
    from app.db import sync_engine
    with sync_engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE ping_results, monitors RESTART IDENTITY CASCADE;"))
    yield


@pytest.fixture(autouse=True)
def setup_db_override():
    """Ensure FastAPI uses NullPool async engine and InMemoryRateLimiter for tests."""
    from sqlalchemy.pool import NullPool
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
    from app.db import DATABASE_URL, get_db
    from app.ratelimit import InMemoryRateLimiter, get_rate_limiter
    import app.main

    test_async_engine = create_async_engine(DATABASE_URL, poolclass=NullPool)
    test_session_local = async_sessionmaker(
        bind=test_async_engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
    )

    async def override_get_db():
        async with test_session_local() as session:
            try:
                yield session
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()

    app.main.app.dependency_overrides[get_db] = override_get_db
    app.main.app.dependency_overrides[get_rate_limiter] = lambda: InMemoryRateLimiter()
    yield
    app.main.app.dependency_overrides.clear()


