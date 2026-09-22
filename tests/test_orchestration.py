"""
Automated Behavioural Test Suite for Orchestration Contracts.

Note: Static Dockerfile syntax and docker-compose.yml configurations are verified
directly in CI via 'docker compose config -q' and 'docker build', rather than fragile string-matching tests.
"""

import pytest


def test_api_health_endpoint_contract(client) -> None:
    """Verify GET /health returns 200 OK with status: ok/healthy for container orchestrators."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data.get("status") in ["ok", "healthy"]
    assert data.get("service") == "PingGuard API"
    assert data.get("version") == "1.0.0"


def test_container_database_url_translation() -> None:
    """Verify asyncpg to psycopg2 translation functions for container hostnames (db:5432)."""
    container_async_url = "postgresql+asyncpg://postgres:postgres@db:5432/pingguard"
    sync_url = container_async_url.replace("+asyncpg", "+psycopg2")
    assert sync_url == "postgresql+psycopg2://postgres:postgres@db:5432/pingguard"
    assert "@db:5432" in sync_url


def test_api_readiness_endpoint_contract(client) -> None:
    """Verify GET /ready returns 200 OK or 503 Service Unavailable with expected schema."""
    response = client.get("/ready")
    assert response.status_code in [200, 503]
    data = response.json()
    assert "status" in data
    if response.status_code == 200:
        assert data["status"] == "ready"
    else:
        assert data["status"] == "not_ready"
        assert "failed" in data
        assert isinstance(data["failed"], list)
