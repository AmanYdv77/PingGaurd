"""
Unit tests for centralised typed Settings in app.config.
"""

import pytest
from pydantic import ValidationError


def test_import_and_defaults(monkeypatch):
    """Verify Settings loads expected defaults with required database_url provided."""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://postgres:pass@localhost:5432/pingguard")
    from app.config import Settings

    settings = Settings()
    assert settings.database_url == "postgresql+asyncpg://postgres:pass@localhost:5432/pingguard"
    assert settings.redis_broker_url == "redis://localhost:6379/0"
    assert settings.redis_result_backend_url == "redis://localhost:6379/1"
    assert settings.sweep_interval_seconds == 15.0
    assert settings.http_connect_timeout == 2.0
    assert settings.http_read_timeout == 5.0
    assert settings.http_write_timeout == 5.0
    assert settings.http_pool_timeout == 2.0
    assert settings.http_max_response_bytes == 1048576
    assert settings.http_max_redirects == 5
    assert settings.http_user_agent == "PingGuard/1.0"
    assert settings.http_keep_alive_user_agent == "PingGuard-KeepAlive/1.0"


def test_env_var_overrides(monkeypatch):
    """Verify HTTP_* and other env vars from .env / docker-compose are respected."""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@localhost:5432/db")
    monkeypatch.setenv("HTTP_CONNECT_TIMEOUT", "1.25")
    monkeypatch.setenv("HTTP_READ_TIMEOUT", "3.75")
    monkeypatch.setenv("HTTP_MAX_REDIRECTS", "10")
    monkeypatch.setenv("HTTP_USER_AGENT", "CustomProbe/2.0")
    monkeypatch.setenv("SWEEP_INTERVAL_SECONDS", "30.0")

    from app.config import Settings

    settings = Settings()
    assert settings.http_connect_timeout == 1.25
    assert settings.http_read_timeout == 3.75
    assert settings.http_max_redirects == 10
    assert settings.http_user_agent == "CustomProbe/2.0"
    assert settings.sweep_interval_seconds == 30.0


def test_http_connect_timeout_override(monkeypatch):
    """Verify with HTTP_CONNECT_TIMEOUT=1.0 set, get_settings().http_connect_timeout == 1.0."""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@localhost:5432/db")
    monkeypatch.setenv("HTTP_CONNECT_TIMEOUT", "1.0")
    from app.config import get_settings

    get_settings.cache_clear()
    try:
        assert get_settings().http_connect_timeout == 1.0
    finally:
        get_settings.cache_clear()


def test_database_url_driver_properties():
    """Verify async and sync database URL translations."""
    from app.config import Settings

    # Case 1: postgresql://
    s1 = Settings(database_url="postgresql://usr:pwd@localhost:5432/dbname")
    assert s1.async_database_url.startswith("postgresql+asyncpg://")
    assert s1.sync_database_url.startswith("postgresql+psycopg2://")

    # Case 2: postgresql+asyncpg://
    s2 = Settings(database_url="postgresql+asyncpg://usr:pwd@localhost:5432/dbname")
    assert s2.async_database_url == "postgresql+asyncpg://usr:pwd@localhost:5432/dbname"
    assert s2.sync_database_url == "postgresql+psycopg2://usr:pwd@localhost:5432/dbname"

    # Case 3: postgresql+psycopg2://
    s3 = Settings(database_url="postgresql+psycopg2://usr:pwd@localhost:5432/dbname")
    assert s3.async_database_url == "postgresql+asyncpg://usr:pwd@localhost:5432/dbname"
    assert s3.sync_database_url == "postgresql+psycopg2://usr:pwd@localhost:5432/dbname"


def test_database_url_required():
    """Verify instantiating Settings without database_url raises ValidationError."""
    from app.config import Settings

    with pytest.raises(ValidationError):
        Settings(_env_file=None, database_url=None)  # type: ignore[arg-type]


def test_positive_timeout_validation():
    """Verify negative or zero timeout values raise ValidationError."""
    from app.config import Settings

    with pytest.raises(ValidationError):
        Settings(
            database_url="postgresql+asyncpg://u:p@localhost:5432/db",
            http_connect_timeout=-1.0,
        )

    with pytest.raises(ValidationError):
        Settings(
            database_url="postgresql+asyncpg://u:p@localhost:5432/db",
            sweep_interval_seconds=0.0,
        )

    with pytest.raises(ValidationError):
        Settings(
            database_url="postgresql+asyncpg://u:p@localhost:5432/db",
            http_max_redirects=-1,
        )

    with pytest.raises(ValidationError):
        Settings(
            database_url="postgresql+asyncpg://u:p@localhost:5432/db",
            http_max_redirects=21,
        )


def test_get_settings_cached(monkeypatch):
    """Verify get_settings returns a cached singleton instance."""
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@localhost:5432/db")
    from app.config import get_settings

    s1 = get_settings()
    s2 = get_settings()
    assert s1 is s2
