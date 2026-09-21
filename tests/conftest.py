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

# Only after guard passes, point DATABASE_URL to normalized TEST_DATABASE_URL
os.environ["DATABASE_URL"] = normalized_db_url
os.environ["ENVIRONMENT"] = "test"


@pytest.fixture(scope="session", autouse=True)
def run_migrations():
    """Apply Alembic migrations to the isolated test database before running tests."""
    project_root = Path(__file__).resolve().parent.parent
    ini_path = project_root / "alembic.ini"
    alembic_cfg = Config(str(ini_path))
    command.upgrade(alembic_cfg, "head")
