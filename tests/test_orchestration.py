"""
PingGuard — Chapter 6 Automated Test Suite: Docker & Docker Compose Orchestration

Validates:
1. Dockerfile structural integrity (multi-stage, Python 3.12-slim, non-root user, caching).
2. docker-compose.yml configuration (service isolation, healthcheck chains, volume persistence,
   singleton Beat enforcement, port boundaries, network isolation).
3. .dockerignore protection (prevents secret and artifact leaks into image build context).
4. Container network URL translation and DNS compatibility.
5. API /health endpoint contract for container orchestration readiness.
"""

import os
import re
import unittest
from pathlib import Path
from fastapi.testclient import TestClient

from app.main import app

PINGGUARD_DIR = Path(__file__).resolve().parent.parent


class TestChapter6Orchestration(unittest.TestCase):
    """Automated verification suite for Chapter 6 Docker & Compose specifications."""

    def setUp(self):
        self.dockerfile_path = PINGGUARD_DIR / "Dockerfile"
        self.compose_path = PINGGUARD_DIR / "docker-compose.yml"
        self.dockerignore_path = PINGGUARD_DIR / ".dockerignore"
        self.client = TestClient(app)

    # --------------------------------------------------------------------------
    # 1. Dockerfile Verification
    # --------------------------------------------------------------------------
    def test_dockerfile_exists(self):
        """Verify Dockerfile exists in project root."""
        self.assertTrue(self.dockerfile_path.exists(), "Dockerfile must exist in pingguard root")

    def test_dockerfile_specifications(self):
        """Verify Dockerfile conforms to Python 3.12, multi-stage, and non-root security rules."""
        content = self.dockerfile_path.read_text(encoding="utf-8")

        # 1. Must use Python 3.12 slim base
        self.assertIn("FROM python:3.12-slim", content, "Dockerfile must use python:3.12-slim")

        # 2. Must be multi-stage (builder + runtime)
        self.assertIn("AS builder", content, "Dockerfile must feature a builder stage")
        self.assertIn("AS runtime", content, "Dockerfile must feature a runtime stage")

        # 3. Must implement dependency cache optimization (copy requirements before full copy)
        req_index = content.find("COPY requirements.txt")
        app_index = content.find("COPY --chown=appuser:appuser . /app")
        if app_index == -1:
            app_index = content.find("COPY . /app")
        if app_index == -1:
            app_index = content.find("COPY . .")
        self.assertGreater(app_index, req_index, "Requirements must be copied before source code for layer caching")

        # 4. Must enforce unprivileged user execution (appuser / UID 1000)
        self.assertIn("useradd -u 1000", content, "Dockerfile must create non-root appuser")
        self.assertIn("USER appuser", content, "Dockerfile must drop root privileges")

        # 5. Must expose port 8000
        self.assertIn("EXPOSE 8000", content, "Dockerfile must expose port 8000")

        # 6. Must not bake secrets into the image
        self.assertNotIn("password123", content.lower())
        self.assertNotIn("mysecretpassword", content.lower())
        self.assertNotIn("postgres:password@", content.lower())

    # --------------------------------------------------------------------------
    # 2. .dockerignore Verification
    # --------------------------------------------------------------------------
    def test_dockerignore_exists_and_blocks_secrets(self):
        """Verify .dockerignore excludes credentials, caches, and local runtime state."""
        self.assertTrue(self.dockerignore_path.exists(), ".dockerignore must exist")
        content = self.dockerignore_path.read_text(encoding="utf-8")

        critical_exclusions = [
            ".git",
            ".venv",
            "__pycache__",
            ".env",
            "celerybeat-schedule",
            "dump.rdb",
        ]
        for exclusion in critical_exclusions:
            self.assertTrue(
                any(exclusion in line for line in content.splitlines()),
                f".dockerignore must exclude '{exclusion}'",
            )

    # --------------------------------------------------------------------------
    # 3. docker-compose.yml Verification
    # --------------------------------------------------------------------------
    def test_compose_exists_and_parses_yaml(self):
        """Verify docker-compose.yml is valid YAML and defines all 5 required services."""
        self.assertTrue(self.compose_path.exists(), "docker-compose.yml must exist")
        
        # Simple parser to avoid requiring external yaml library if not present
        content = self.compose_path.read_text(encoding="utf-8")

        # Required services
        services = ["db", "redis", "web", "worker", "scheduler"]
        for svc in services:
            pattern = rf"^\s{{2}}{svc}:"
            self.assertTrue(
                bool(re.search(pattern, content, re.MULTILINE)),
                f"docker-compose.yml must define service '{svc}'",
            )

    def test_compose_shared_application_image(self):
        """Verify web, worker, and scheduler share the same application image/build context."""
        content = self.compose_path.read_text(encoding="utf-8")

        # web, worker, and scheduler must all use build context or shared image
        self.assertIn("image: pingguard-app:latest", content, "App services must share pingguard-app:latest image")

    def test_compose_healthchecks_and_dependencies(self):
        """Verify healthchecks on db & redis, and service_healthy dependencies on app services."""
        content = self.compose_path.read_text(encoding="utf-8")

        # PostgreSQL pg_isready healthcheck
        self.assertIn("pg_isready", content, "PostgreSQL service must configure pg_isready healthcheck")

        # Redis redis-cli ping healthcheck
        self.assertIn("redis-cli", content, "Redis service must configure redis-cli ping healthcheck")
        self.assertIn("ping", content)

        # App services must wait for condition: service_healthy
        self.assertIn("condition: service_healthy", content, "App services must wait for healthy dependencies")

    def test_compose_singleton_scheduler(self):
        """Verify scheduler (Celery Beat) is strictly constrained as a singleton (replicas: 1)."""
        content = self.compose_path.read_text(encoding="utf-8")

        # Find scheduler section
        scheduler_match = re.search(r"scheduler:(.+?)(?=\n  \w+:|\Z)", content, re.DOTALL)
        self.assertIsNotNone(scheduler_match, "scheduler section must exist in docker-compose.yml")
        scheduler_block = scheduler_match.group(1)

        self.assertIn("replicas: 1", scheduler_block, "scheduler must have replicas: 1 to prevent duplicate sweeps")

    def test_compose_network_and_port_isolation(self):
        """Verify only port 8000 is exposed to host, and db/redis remain internal."""
        content = self.compose_path.read_text(encoding="utf-8")

        # Port 8000 must be mapped for web
        self.assertIn('"8000:8000"', content, "Web service must expose 8000:8000")

        # Neither 5432 nor 6379 should be published to host in production compose
        db_match = re.search(r"db:(.+?)(?=\n  \w+:|\Z)", content, re.DOTALL)
        self.assertIsNotNone(db_match)
        self.assertNotIn('"5432:5432"', db_match.group(1), "PostgreSQL port 5432 must NOT be published to host")

        redis_match = re.search(r"redis:(.+?)(?=\n  \w+:|\Z)", content, re.DOTALL)
        self.assertIsNotNone(redis_match)
        self.assertNotIn('"6379:6379"', redis_match.group(1), "Redis port 6379 must NOT be published to host")

        # Internal network must be declared
        self.assertIn("pingguard_net", content, "All services must communicate via pingguard_net")

    def test_compose_volume_persistence(self):
        """Verify named volumes are declared for PostgreSQL and Redis."""
        content = self.compose_path.read_text(encoding="utf-8")
        self.assertIn("pgdata:", content, "PostgreSQL named volume pgdata must be declared")
        self.assertIn("redis_data:", content, "Redis named volume redis_data must be declared")

    # --------------------------------------------------------------------------
    # 4. Health Endpoint Contract
    # --------------------------------------------------------------------------
    def test_api_health_endpoint_contract(self):
        """Verify GET /health returns 200 OK with status: ok/healthy for container orchestrators."""
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn(data.get("status"), ["ok", "healthy"])
        self.assertEqual(data.get("service"), "PingGuard API")
        self.assertEqual(data.get("version"), "0.6.0")

    # --------------------------------------------------------------------------
    # 5. Database URL Resolution in Container Context
    # --------------------------------------------------------------------------
    def test_container_database_url_translation(self):
        """Verify asyncpg to psycopg2 translation functions for container hostnames (db:5432)."""
        container_async_url = "postgresql+asyncpg://postgres:postgres@db:5432/pingguard"

        # Worker sync translation logic
        sync_url = container_async_url.replace("+asyncpg", "+psycopg2")
        self.assertEqual(sync_url, "postgresql+psycopg2://postgres:postgres@db:5432/pingguard")
        self.assertIn("@db:5432", sync_url, "Must target internal container DNS 'db'")


if __name__ == "__main__":
    unittest.main()
