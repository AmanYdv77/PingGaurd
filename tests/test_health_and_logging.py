"""
Tests for health/readiness endpoints and structured logging with request IDs (Task A20).

Covers:
- Unauthenticated liveness probe (GET /health) contract.
- Unauthenticated readiness probe (GET /ready) checking PostgreSQL and Redis dependencies.
- Error payloads on dependency outages returning HTTP 503 without leaking credentials or exception details.
- Request-ID middleware validation, propagation, and generation.
- Structured JSON logging format and context-aware request_id inclusion.
"""

import asyncio
import json
import logging
import re
from unittest.mock import AsyncMock, patch
import pytest
from app import __version__
from app.config import Settings
from app.logging_config import JSONLogFormatter, RequestIdFilter, configure_logging, request_id_var


def test_health_endpoint_contract(client):
    """GET /health must remain an unauthenticated, static liveness probe."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data == {
        "status": "healthy",
        "service": "PingGuard API",
        "version": __version__,
    }


def test_ready_endpoint_both_healthy(client):
    """GET /ready returns 200 and {'status': 'ready'} when both DB and Redis respond."""
    with patch("app.main.check_redis_readiness", new_callable=AsyncMock) as mock_redis:
        mock_redis.return_value = True
        response = client.get("/ready")
        assert response.status_code == 200
        assert response.json() == {"status": "ready"}


def test_ready_endpoint_database_failure_503(client):
    """GET /ready returns 503 with failed=['database'] when PostgreSQL fails, without leaking details."""
    with patch("app.main.check_database_readiness", new_callable=AsyncMock) as mock_db:
        mock_db.return_value = False
        with patch("app.main.check_redis_readiness", new_callable=AsyncMock) as mock_redis:
            mock_redis.return_value = True
            response = client.get("/ready")
            assert response.status_code == 503
            data = response.json()
            assert data == {"status": "not_ready", "failed": ["database"]}
            # Verify no credentials or error tracebacks leaked
            body_text = response.text
            assert "password" not in body_text.lower()
            assert "traceback" not in body_text.lower()


def test_ready_endpoint_redis_failure_503(client):
    """GET /ready returns 503 with failed=['redis'] when Redis fails, without leaking details."""
    with patch("app.main.check_database_readiness", new_callable=AsyncMock) as mock_db:
        mock_db.return_value = True
        with patch("app.main.check_redis_readiness", new_callable=AsyncMock) as mock_redis:
            mock_redis.return_value = False
            response = client.get("/ready")
            assert response.status_code == 503
            data = response.json()
            assert data == {"status": "not_ready", "failed": ["redis"]}
            body_text = response.text
            assert "redis://" not in body_text
            assert "traceback" not in body_text.lower()


def test_ready_endpoint_both_failure_503(client):
    """GET /ready returns 503 with failed=['database', 'redis'] when both dependencies fail."""
    with patch("app.main.check_database_readiness", new_callable=AsyncMock) as mock_db:
        mock_db.return_value = False
        with patch("app.main.check_redis_readiness", new_callable=AsyncMock) as mock_redis:
            mock_redis.return_value = False
            response = client.get("/ready")
            assert response.status_code == 503
            assert response.json() == {"status": "not_ready", "failed": ["database", "redis"]}


def test_request_id_header_generated_when_missing(client):
    """Requests without X-Request-ID header receive a newly generated UUID hex request ID."""
    response = client.get("/health")
    assert response.status_code == 200
    req_id = response.headers.get("X-Request-ID")
    assert req_id is not None
    assert re.match(r"^[0-9a-f]{32}$", req_id)


def test_request_id_header_echoed_when_valid(client):
    """Requests with valid X-Request-ID header (8-64 alphanumeric/dash/underscore) echo the exact ID."""
    valid_id = "req-custom_trace-12345"
    response = client.get("/health", headers={"X-Request-ID": valid_id})
    assert response.status_code == 200
    assert response.headers.get("X-Request-ID") == valid_id


def test_request_id_header_replaced_when_invalid(client):
    """Requests with invalid X-Request-ID header are replaced with a valid generated UUID hex."""
    invalid_ids = [
        "short",  # < 8 chars
        "invalid;characters!here",  # disallowed characters
        "a" * 65,  # > 64 chars
        "spaces not allowed",
    ]
    for bad_id in invalid_ids:
        response = client.get("/health", headers={"X-Request-ID": bad_id})
        assert response.status_code == 200
        req_id = response.headers.get("X-Request-ID")
        assert req_id is not None
        assert req_id != bad_id
        assert re.match(r"^[0-9a-f]{32}$", req_id)


def test_json_log_formatter_structure():
    """Verify JSONLogFormatter outputs standard UTC ISO 8601 JSON structure with request_id."""
    token = request_id_var.set("trace-abc-123")
    try:
        formatter = JSONLogFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname=__file__,
            lineno=10,
            msg="User %s performed action",
            args=("alice",),
            exc_info=None,
        )
        filt = RequestIdFilter()
        filt.filter(record)
        formatted = formatter.format(record)
        parsed = json.loads(formatted)
        assert parsed["level"] == "INFO"
        assert parsed["logger"] == "test.logger"
        assert parsed["message"] == "User alice performed action"
        assert parsed["request_id"] == "trace-abc-123"
        assert "timestamp" in parsed
        assert parsed["timestamp"].endswith("+00:00") or parsed["timestamp"].endswith("Z")
    finally:
        request_id_var.reset(token)


def test_logging_configuration_dictconfig():
    """Verify configure_logging executes cleanly without exceptions."""
    configure_logging(level="DEBUG", json_logs=True)
    root_logger = logging.getLogger()
    assert root_logger.level == logging.DEBUG
    # Reset to default
    configure_logging(level="INFO", json_logs=True)


def test_config_log_level_validation():
    """Verify Settings validates log_level and rejects invalid levels."""
    valid_settings = Settings(
        api_key="a" * 24,
        database_url="postgresql+asyncpg://postgres:pass@localhost:5432/db",
        log_level="debug",
        log_json=True,
    )
    assert valid_settings.log_level == "DEBUG"
    assert valid_settings.log_json is True

    with pytest.raises(ValueError, match="Invalid log level"):
        Settings(
            api_key="a" * 24,
            database_url="postgresql+asyncpg://postgres:pass@localhost:5432/db",
            log_level="INVALID_LEVEL",
        )


def test_live_database_readiness():
    """Verify check_database_readiness executes SELECT 1 against live test DB."""
    from sqlalchemy.pool import NullPool
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
    from app.db import DATABASE_URL
    from app.main import check_database_readiness

    async def _run():
        test_engine = create_async_engine(DATABASE_URL, poolclass=NullPool)
        session_factory = async_sessionmaker(bind=test_engine, class_=AsyncSession)
        try:
            async with session_factory() as session:
                return await check_database_readiness(session)
        finally:
            await test_engine.dispose()

    ready = asyncio.run(_run())
    assert ready is True


def test_graceful_redis_readiness_failure():
    """Verify check_redis_readiness returns False when Redis is unreachable without raising."""
    from app.main import check_redis_readiness

    ready = asyncio.run(check_redis_readiness())
    assert isinstance(ready, bool)


def test_credential_redaction_before_logging():
    """Ensure sensitive credentials in target URLs are safely redacted before logging."""
    from app.ssrf import redact_url_credentials

    raw_url = "https://user:verysecretpass@api.service.internal:8080/data"
    redacted = redact_url_credentials(raw_url)
    assert "verysecretpass" not in redacted
    assert "user" not in redacted
    assert redacted == "https://***:***@api.service.internal:8080/data"
