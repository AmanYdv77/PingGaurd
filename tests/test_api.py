"""
Comprehensive test suite for PingGuard Chapter 1 (FastAPI & Async Python).

Covers:
1. Valid monitor creation with default interval and backward-compatible payloads.
2. Input validation rejections (invalid URL, out-of-bounds intervals, invalid names).
3. Keep-alive configuration and validation:
   - TEST 1: Normal monitor creation.
   - TEST 2: Old request format without new fields (backward compatibility).
   - TEST 3: Monitor + Keep-Alive configuration works.
   - TEST 4: Keep-alive enabled without interval fails (422).
   - TEST 5: Keep-alive interval below 15 fails (422).
   - TEST 6: Keep-alive interval above 86400 fails (422).
   - TEST 7: Keep-alive path "/health" works.
   - TEST 8: Keep-alive path "https://abc.com" fails (422).
   - TEST 9: Monitor-only mode with keep_alive_enabled=false works.
   - TEST 10: GET returns all new fields.
   - TEST 11: UPDATE can modify keep-alive configuration.
   - TEST 12: GET unknown monitor still returns 404.
   - TEST 13: Invalid original fields still produce 422.
   - TEST 14: No outbound network request occurs during monitor creation.
"""

import unittest
from datetime import datetime, timezone
from unittest.mock import patch
import psycopg2
from fastapi.testclient import TestClient
from sqlalchemy import select

from sqlalchemy.pool import NullPool
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from app.db import DATABASE_URL, get_db
from app.main import app
from app.models import Monitor, PingResult
from app.schemas import MonitorMode, MonitorStatus

# Use NullPool for tests so each request in TestClient gets a connection on its current event loop
test_engine = create_async_engine(DATABASE_URL, poolclass=NullPool)
TestSessionLocal = async_sessionmaker(
    bind=test_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


async def override_get_db():
    async with TestSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


def _get_pg_conn_str(url: str) -> str:
    """Strip SQLAlchemy driver prefixes for native psycopg2 connection."""
    for prefix in ("+asyncpg", "+psycopg2"):
        url = url.replace(prefix, "")
    return url


class TestPingGuardChapter2(unittest.TestCase):
    def setUp(self) -> None:
        """Reset PostgreSQL tables and dependency overrides before every test run."""
        app.dependency_overrides[get_db] = override_get_db

        sync_url = _get_pg_conn_str(DATABASE_URL)
        conn = psycopg2.connect(sync_url)
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE ping_results, monitors RESTART IDENTITY CASCADE;")
        conn.commit()
        cur.close()
        conn.close()

        self.client = TestClient(app)

    def tearDown(self) -> None:
        app.dependency_overrides.clear()

    def test_health_check(self) -> None:
        """Verify the basic service health check endpoint."""
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "healthy")
        self.assertEqual(data["service"], "PingGuard API")

    # =========================================================================
    # TEST 1 & TEST 2: Backward Compatibility & Defaults
    # =========================================================================
    def test_create_monitor_valid_and_backward_compatible(self) -> None:
        """
        TEST 1 & 2: Old request format without new fields works with exact defaults.
        Payload: {"name": "t", "url": "https://example.com"}
        Expected: 201 Created, check_interval_seconds=60, mode='monitor',
                  keep_alive_enabled=false, keep_alive_interval_seconds=null, keep_alive_path=null.
        """
        payload = {
            "name": "t",
            "url": "https://example.com"
        }
        response = self.client.post("/monitors/", json=payload)
        self.assertEqual(response.status_code, 201)
        data = response.json()

        self.assertIn("id", data)
        self.assertEqual(data["id"], 1)
        self.assertEqual(data["name"], "t")
        self.assertEqual(data["url"], "https://example.com/")
        self.assertEqual(data["check_interval_seconds"], 60)  # Default
        self.assertEqual(data["status"], "pending")
        self.assertIsNone(data["last_checked_at"])
        self.assertIsNotNone(data["next_check_at"])

        # Verify new fields default correctly
        self.assertEqual(data["mode"], "monitor")
        self.assertFalse(data["keep_alive_enabled"])
        self.assertIsNone(data["keep_alive_interval_seconds"])
        self.assertIsNone(data["keep_alive_path"])

    def test_create_monitor_custom_interval(self) -> None:
        """Verify custom check_interval_seconds within valid range."""
        payload = {
            "name": "Production API",
            "url": "https://api.example.org/health",
            "check_interval_seconds": 30
        }
        response = self.client.post("/monitors/", json=payload)
        self.assertEqual(response.status_code, 201)
        data = response.json()
        self.assertEqual(data["check_interval_seconds"], 30)

    # =========================================================================
    # TEST 3: Monitor + Keep-Alive Configuration
    # =========================================================================
    def test_create_monitor_with_keep_alive(self) -> None:
        """
        TEST 3: Full Monitor + Keep-Alive configuration succeeds.
        """
        payload = {
            "name": "XYZ Website",
            "url": "https://xyz.com",
            "check_interval_seconds": 60,
            "mode": "monitor_and_keep_alive",
            "keep_alive_enabled": True,
            "keep_alive_interval_seconds": 600,
            "keep_alive_path": "/health"
        }
        response = self.client.post("/monitors/", json=payload)
        self.assertEqual(response.status_code, 201)
        data = response.json()

        self.assertEqual(data["mode"], "monitor_and_keep_alive")
        self.assertTrue(data["keep_alive_enabled"])
        self.assertEqual(data["keep_alive_interval_seconds"], 600)
        self.assertEqual(data["keep_alive_path"], "/health")

    def test_create_keep_alive_only_mode(self) -> None:
        """Verify mode='keep_alive' registers successfully."""
        payload = {
            "name": "Worker Node",
            "url": "https://worker.internal.net",
            "mode": "keep_alive",
            "keep_alive_enabled": True,
            "keep_alive_interval_seconds": 300,
            "keep_alive_path": "/ping"
        }
        response = self.client.post("/monitors/", json=payload)
        self.assertEqual(response.status_code, 201)
        data = response.json()
        self.assertEqual(data["mode"], "keep_alive")
        self.assertTrue(data["keep_alive_enabled"])
        self.assertEqual(data["keep_alive_interval_seconds"], 300)

    # =========================================================================
    # TEST 4, 5, 6: Keep-Alive Interval Validation
    # =========================================================================
    def test_keep_alive_enabled_without_interval_fails(self) -> None:
        """
        TEST 4: keep_alive_enabled=True without keep_alive_interval_seconds fails with 422.
        """
        payload = {
            "name": "Incomplete KeepAlive",
            "url": "https://example.com",
            "mode": "keep_alive",
            "keep_alive_enabled": True,
            "keep_alive_interval_seconds": None
        }
        response = self.client.post("/monitors/", json=payload)
        self.assertEqual(response.status_code, 422)

    def test_keep_alive_interval_below_minimum(self) -> None:
        """
        TEST 5: keep_alive_interval_seconds < 15 must return HTTP 422.
        """
        payload = {
            "name": "Too Rapid",
            "url": "https://example.com",
            "mode": "keep_alive",
            "keep_alive_enabled": True,
            "keep_alive_interval_seconds": 10
        }
        response = self.client.post("/monitors/", json=payload)
        self.assertEqual(response.status_code, 422)

    def test_keep_alive_interval_above_maximum(self) -> None:
        """
        TEST 6: keep_alive_interval_seconds > 86400 must return HTTP 422.
        """
        payload = {
            "name": "Too Slow",
            "url": "https://example.com",
            "mode": "keep_alive",
            "keep_alive_enabled": True,
            "keep_alive_interval_seconds": 90000
        }
        response = self.client.post("/monitors/", json=payload)
        self.assertEqual(response.status_code, 422)

    def test_keep_alive_disabled_with_interval_rejected(self) -> None:
        """Inconsistent configuration: keep_alive_enabled=False with interval must return 422."""
        payload = {
            "name": "Inconsistent",
            "url": "https://example.com",
            "mode": "monitor",
            "keep_alive_enabled": False,
            "keep_alive_interval_seconds": 600
        }
        response = self.client.post("/monitors/", json=payload)
        self.assertEqual(response.status_code, 422)

    # =========================================================================
    # TEST 7 & 8: Keep-Alive Path Validation
    # =========================================================================
    def test_keep_alive_path_valid(self) -> None:
        """
        TEST 7: Keep-alive path starting with '/' works.
        """
        valid_paths = ["/health", "/", "/api/v1/ping", "/healthcheck"]
        for p in valid_paths:
            with self.subTest(path=p):
                payload = {
                    "name": "Path Test",
                    "url": "https://example.com",
                    "mode": "keep_alive",
                    "keep_alive_enabled": True,
                    "keep_alive_interval_seconds": 60,
                    "keep_alive_path": p
                }
                response = self.client.post("/monitors/", json=payload)
                self.assertEqual(response.status_code, 201)
                self.assertEqual(response.json()["keep_alive_path"], p)

    def test_keep_alive_path_absolute_url_fails(self) -> None:
        """
        TEST 8: Keep-alive path as absolute URL or missing leading slash fails with 422.
        """
        invalid_paths = [
            "https://abc.com",
            "http://other-site.com/health",
            "//malicious-redirect.com",
            "health",
            "just-path"
        ]
        for p in invalid_paths:
            with self.subTest(path=p):
                payload = {
                    "name": "Invalid Path Test",
                    "url": "https://example.com",
                    "mode": "keep_alive",
                    "keep_alive_enabled": True,
                    "keep_alive_interval_seconds": 60,
                    "keep_alive_path": p
                }
                response = self.client.post("/monitors/", json=payload)
                self.assertEqual(response.status_code, 422)

    # =========================================================================
    # TEST 9: Monitor-Only Mode with keep_alive_enabled=False
    # =========================================================================
    def test_monitor_only_explicit_false(self) -> None:
        """
        TEST 9: Monitor-only mode with keep_alive_enabled=false explicitly works.
        """
        payload = {
            "name": "Explicit Monitor Only",
            "url": "https://example.com",
            "mode": "monitor",
            "keep_alive_enabled": False,
            "keep_alive_interval_seconds": None,
            "keep_alive_path": None
        }
        response = self.client.post("/monitors/", json=payload)
        self.assertEqual(response.status_code, 201)
        data = response.json()
        self.assertEqual(data["mode"], "monitor")
        self.assertFalse(data["keep_alive_enabled"])

    def test_monitor_mode_with_keep_alive_enabled_true_fails(self) -> None:
        """mode='monitor' with keep_alive_enabled=True is inconsistent and must return 422."""
        payload = {
            "name": "Conflict Test",
            "url": "https://example.com",
            "mode": "monitor",
            "keep_alive_enabled": True,
            "keep_alive_interval_seconds": 60
        }
        response = self.client.post("/monitors/", json=payload)
        self.assertEqual(response.status_code, 422)

    # =========================================================================
    # TEST 10: GET Returns All New Fields
    # =========================================================================
    def test_get_returns_all_new_fields(self) -> None:
        """
        TEST 10: GET /monitors/{id} returns all keep-alive and mode fields.
        """
        create_resp = self.client.post(
            "/monitors/",
            json={
                "name": "Inspectable Monitor",
                "url": "https://example.com",
                "mode": "monitor_and_keep_alive",
                "keep_alive_enabled": True,
                "keep_alive_interval_seconds": 120,
                "keep_alive_path": "/healthz"
            }
        )
        created_id = create_resp.json()["id"]

        get_resp = self.client.get(f"/monitors/{created_id}")
        self.assertEqual(get_resp.status_code, 200)
        data = get_resp.json()
        self.assertEqual(data["mode"], "monitor_and_keep_alive")
        self.assertTrue(data["keep_alive_enabled"])
        self.assertEqual(data["keep_alive_interval_seconds"], 120)
        self.assertEqual(data["keep_alive_path"], "/healthz")

    # =========================================================================
    # TEST 11: UPDATE Can Modify Keep-Alive Configuration
    # =========================================================================
    def test_update_modify_keep_alive_configuration(self) -> None:
        """
        TEST 11: PATCH /monitors/{id} can enable, modify, or disable keep-alive settings.
        """
        # 1. Create standard monitor
        create_resp = self.client.post(
            "/monitors/",
            json={"name": "Updatable", "url": "https://example.com"}
        )
        mid = create_resp.json()["id"]

        # 2. Update to enable keep-alive
        patch_resp = self.client.patch(
            f"/monitors/{mid}",
            json={
                "mode": "monitor_and_keep_alive",
                "keep_alive_enabled": True,
                "keep_alive_interval_seconds": 180,
                "keep_alive_path": "/ping"
            }
        )
        self.assertEqual(patch_resp.status_code, 200)
        data = patch_resp.json()
        self.assertEqual(data["mode"], "monitor_and_keep_alive")
        self.assertTrue(data["keep_alive_enabled"])
        self.assertEqual(data["keep_alive_interval_seconds"], 180)
        self.assertEqual(data["keep_alive_path"], "/ping")

        # 3. Incomplete update fails (keep_alive_enabled=True without interval)
        bad_patch = self.client.patch(
            f"/monitors/{mid}",
            json={"keep_alive_enabled": True, "keep_alive_interval_seconds": None}
        )
        self.assertEqual(bad_patch.status_code, 422)

        # 4. Disable keep-alive
        disable_resp = self.client.patch(
            f"/monitors/{mid}",
            json={"keep_alive_enabled": False}
        )
        self.assertEqual(disable_resp.status_code, 200)
        dis_data = disable_resp.json()
        self.assertFalse(dis_data["keep_alive_enabled"])
        self.assertIsNone(dis_data["keep_alive_interval_seconds"])
        self.assertEqual(dis_data["mode"], "monitor")

    # =========================================================================
    # TEST 12: GET Unknown Monitor Returns 404
    # =========================================================================
    def test_get_unknown_monitor_returns_404(self) -> None:
        """
        TEST 12: GET /monitors/99999 returns HTTP 404 'Monitor not found'.
        """
        response = self.client.get("/monitors/99999")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "Monitor not found"})

    # =========================================================================
    # TEST 13: Invalid Original Fields Still Produce 422
    # =========================================================================
    def test_invalid_original_fields_produce_422(self) -> None:
        """
        TEST 13: Invalid URL, empty name, name too long, interval bounds still return 422.
        """
        # Invalid URL
        self.assertEqual(self.client.post("/monitors/", json={"name": "A", "url": "not-url"}).status_code, 422)
        # Empty name
        self.assertEqual(self.client.post("/monitors/", json={"name": "", "url": "https://example.com"}).status_code, 422)
        # Name too long
        self.assertEqual(self.client.post("/monitors/", json={"name": "A" * 121, "url": "https://example.com"}).status_code, 422)
        # Check interval < 15
        self.assertEqual(self.client.post("/monitors/", json={"name": "A", "url": "https://example.com", "check_interval_seconds": 10}).status_code, 422)
        # Check interval > 86400
        self.assertEqual(self.client.post("/monitors/", json={"name": "A", "url": "https://example.com", "check_interval_seconds": 90000}).status_code, 422)

    # =========================================================================
    # TEST 14: No Outbound Network Request Occurs
    # =========================================================================
    def test_no_outbound_network_request_during_creation(self) -> None:
        """
        TEST 14: CRITICAL ARCHITECTURAL BOUNDARY:
        Verify that registering a monitor (with or without keep-alive) makes ZERO
        outbound HTTP requests to the target destination.
        """
        import http.client
        import urllib.request

        with patch("urllib.request.urlopen") as mock_urlopen, \
             patch("http.client.HTTPSConnection.request") as mock_https_request, \
             patch("http.client.HTTPConnection.request") as mock_http_request:
            response = self.client.post(
                "/monitors/",
                json={
                    "name": "Air-Gapped Isolation Test",
                    "url": "https://nonexistent-fake-target-domain-999.xyz",
                    "mode": "monitor_and_keep_alive",
                    "keep_alive_enabled": True,
                    "keep_alive_interval_seconds": 300,
                    "keep_alive_path": "/health"
                }
            )
            self.assertEqual(response.status_code, 201)
            mock_urlopen.assert_not_called()
            mock_https_request.assert_not_called()
            mock_http_request.assert_not_called()

    # =========================================================================
    # Additional Listing & OpenAPI Tests
    # =========================================================================
    def test_list_monitors_includes_keep_alive(self) -> None:
        """Verify list endpoint includes keep-alive attributes."""
        self.client.post("/monitors/", json={"name": "M1", "url": "https://m1.com"})
        self.client.post(
            "/monitors/",
            json={
                "name": "M2",
                "url": "https://m2.com",
                "mode": "keep_alive",
                "keep_alive_enabled": True,
                "keep_alive_interval_seconds": 60
            }
        )
        response = self.client.get("/monitors/")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(len(data), 2)
        self.assertFalse(data[0]["keep_alive_enabled"])
        self.assertTrue(data[1]["keep_alive_enabled"])

    def test_openapi_schema_exposes_new_models(self) -> None:
        """Verify /openapi.json exposes MonitorMode enum and new fields."""
        response = self.client.get("/openapi.json")
        self.assertEqual(response.status_code, 200)
        schema = response.json()

        schemas = schema.get("components", {}).get("schemas", {})
        self.assertIn("MonitorMode", schemas)
        self.assertEqual(
            schemas["MonitorMode"]["enum"],
            ["monitor", "keep_alive", "monitor_and_keep_alive"]
        )

        create_props = schemas["MonitorCreate"]["properties"]
        self.assertIn("mode", create_props)
        self.assertIn("keep_alive_enabled", create_props)
        self.assertIn("keep_alive_interval_seconds", create_props)
        self.assertIn("keep_alive_path", create_props)

    def test_update_monitor_put_keep_alive(self) -> None:
        """Verify PUT endpoint works symmetrically with keep-alive configurations."""
        create_resp = self.client.post(
            "/monitors/",
            json={"name": "Initial", "url": "https://example.com"}
        )
        mid = create_resp.json()["id"]

        put_resp = self.client.put(
            f"/monitors/{mid}",
            json={
                "name": "PUT Updated",
                "mode": "keep_alive",
                "keep_alive_enabled": True,
                "keep_alive_interval_seconds": 120,
                "keep_alive_path": "/status"
            }
        )
        self.assertEqual(put_resp.status_code, 200)
        data = put_resp.json()
        self.assertEqual(data["name"], "PUT Updated")
        self.assertEqual(data["mode"], "keep_alive")
        self.assertTrue(data["keep_alive_enabled"])
        self.assertEqual(data["keep_alive_interval_seconds"], 120)
        self.assertEqual(data["keep_alive_path"], "/status")

    def test_list_monitors_pagination(self) -> None:
        """Verify pagination with skip and limit parameters."""
        for i in range(1, 6):
            self.client.post("/monitors/", json={"name": f"M{i}", "url": f"https://m{i}.com"})
        resp = self.client.get("/monitors/?skip=1&limit=2")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["name"], "M2")
        self.assertEqual(data[1]["name"], "M3")

    def test_swagger_ui(self) -> None:
        """Verify Swagger UI (/docs) returns HTTP 200 with interactive HTML."""
        response = self.client.get("/docs")
        self.assertEqual(response.status_code, 200)
        self.assertIn("swagger-ui", response.text.lower())

    # =========================================================================
    # CHAPTER 2 SPECIFIC TESTS: Persistence, Restart, Relationships, Cascade
    # =========================================================================
    def test_keep_alive_fields_persisted_in_postgresql(self) -> None:
        """
        Chapter 2 - Req 36: Verify Monitor with Keep-Alive configuration is persisted
        durably in PostgreSQL with exact column values and next_keep_alive_at timestamp.
        """
        payload = {
            "name": "XYZ Website",
            "url": "https://xyz.com",
            "check_interval_seconds": 60,
            "mode": "monitor_and_keep_alive",
            "keep_alive_enabled": True,
            "keep_alive_interval_seconds": 600,
            "keep_alive_path": "/health"
        }
        create_resp = self.client.post("/monitors/", json=payload)
        self.assertEqual(create_resp.status_code, 201)
        created_id = create_resp.json()["id"]

        # Verify directly in PostgreSQL via synchronous connection
        sync_url = _get_pg_conn_str(DATABASE_URL)
        conn = psycopg2.connect(sync_url)
        cur = conn.cursor()
        cur.execute(
            "SELECT name, url, check_interval_seconds, mode, keep_alive_enabled, "
            "keep_alive_interval_seconds, keep_alive_path, next_keep_alive_at "
            "FROM monitors WHERE id = %s;",
            (created_id,)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()

        self.assertIsNotNone(row)
        self.assertEqual(row[0], "XYZ Website")
        self.assertEqual(row[1], "https://xyz.com/")
        self.assertEqual(row[2], 60)
        self.assertEqual(row[3], "monitor_and_keep_alive")
        self.assertTrue(row[4])
        self.assertEqual(row[5], 600)
        self.assertEqual(row[6], "/health")
        self.assertIsNotNone(row[7])  # next_keep_alive_at is set for future scheduler

    def test_persistence_across_app_restart(self) -> None:
        """
        Chapter 2 - Req 35 & 37: Verify monitor and Keep-Alive data survive
        a complete FastAPI application teardown and restart.
        """
        import sys

        # 1. Create monitor with Keep-Alive in session 1
        payload = {
            "name": "Durable Monitor",
            "url": "https://durable.example.com",
            "check_interval_seconds": 90,
            "mode": "monitor_and_keep_alive",
            "keep_alive_enabled": True,
            "keep_alive_interval_seconds": 450,
            "keep_alive_path": "/alive"
        }
        res1 = self.client.post("/monitors/", json=payload)
        self.assertEqual(res1.status_code, 201)
        mid = res1.json()["id"]

        # 2. Simulate FastAPI shutdown and restart
        del self.client
        for mod in list(sys.modules.keys()):
            if mod.startswith("app.main"):
                del sys.modules[mod]

        from app.main import app as restarted_app
        restarted_app.dependency_overrides[get_db] = override_get_db
        restarted_client = TestClient(restarted_app)

        # 3. Retrieve from new application instance
        res2 = restarted_client.get(f"/monitors/{mid}")
        self.assertEqual(res2.status_code, 200)
        data = res2.json()

        self.assertEqual(data["id"], mid)
        self.assertEqual(data["name"], "Durable Monitor")
        self.assertEqual(data["url"], "https://durable.example.com/")
        self.assertEqual(data["check_interval_seconds"], 90)
        self.assertEqual(data["mode"], "monitor_and_keep_alive")
        self.assertTrue(data["keep_alive_enabled"])
        self.assertEqual(data["keep_alive_interval_seconds"], 450)
        self.assertEqual(data["keep_alive_path"], "/alive")

    def test_ping_result_orm_relationship_both_check_types(self) -> None:
        """
        Chapter 2 - Req 38: Verify PingResult ORM model and relationship to Monitor.
        Confirms both 'monitor' and 'keep_alive' check types can belong to the same Monitor.
        """
        import asyncio

        async def _test():
            async with TestSessionLocal() as session:
                # Create Monitor
                monitor = Monitor(
                    name="Relationship Test Monitor",
                    url="https://rel.example.com",
                    check_interval_seconds=60,
                    mode="monitor_and_keep_alive",
                    keep_alive_enabled=True,
                    keep_alive_interval_seconds=300,
                )
                session.add(monitor)
                await session.commit()
                await session.refresh(monitor)
                mid = monitor.id

                # Create two PingResults: one 'monitor', one 'keep_alive'
                now = datetime.now(timezone.utc)
                pr_monitor = PingResult(
                    monitor_id=mid,
                    check_type="monitor",
                    status_code=200,
                    latency_ms=120.0,
                    error=None,
                    checked_at=now,
                )
                pr_keep_alive = PingResult(
                    monitor_id=mid,
                    check_type="keep_alive",
                    status_code=200,
                    latency_ms=85.5,
                    error=None,
                    checked_at=now,
                )
                session.add_all([pr_monitor, pr_keep_alive])
                await session.commit()

                # Verify relationship
                stmt = select(PingResult).where(PingResult.monitor_id == mid).order_by(PingResult.id.asc())
                res = await session.execute(stmt)
                results = res.scalars().all()
                self.assertEqual(len(results), 2)
                self.assertEqual(results[0].check_type, "monitor")
                self.assertEqual(results[0].latency_ms, 120.0)
                self.assertEqual(results[1].check_type, "keep_alive")
                self.assertEqual(results[1].latency_ms, 85.5)

        asyncio.run(_test())

    def test_cascade_delete_monitor_and_ping_results(self) -> None:
        """
        Chapter 2 - Req 39: Verify cascade delete.
        Deleting a Monitor must cascade delete all associated PingResult rows.
        """
        import asyncio

        async def _test():
            async with TestSessionLocal() as session:
                # Create Monitor
                monitor = Monitor(
                    name="Cascade Target",
                    url="https://cascade.example.com",
                    check_interval_seconds=60,
                )
                session.add(monitor)
                await session.commit()
                await session.refresh(monitor)
                mid = monitor.id

                # Add PingResult
                pr = PingResult(
                    monitor_id=mid,
                    check_type="monitor",
                    status_code=200,
                    latency_ms=99.0,
                    checked_at=datetime.now(timezone.utc),
                )
                session.add(pr)
                await session.commit()

                # Confirm PingResult exists
                stmt = select(PingResult).where(PingResult.monitor_id == mid)
                res = await session.execute(stmt)
                self.assertEqual(len(res.scalars().all()), 1)

                # Delete monitor
                await session.delete(monitor)
                await session.commit()

                # Confirm PingResults were cascade deleted
                res_after = await session.execute(stmt)
                self.assertEqual(len(res_after.scalars().all()), 0)

        asyncio.run(_test())

    def test_health_endpoint(self):
        """Verify GET /health returns 200 OK with status ok or healthy."""
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn(data.get("status"), ["ok", "healthy"])
        self.assertEqual(data.get("service"), "PingGuard API")
        self.assertEqual(data.get("version"), "0.6.0")

    def test_delete_monitor_api(self):
        """Verify DELETE /monitors/{id} deletes the monitor and returns 204."""
        create_resp = self.client.post("/monitors/", json={
            "name": "Delete Me Test",
            "url": "https://example.com/delete-test",
            "check_interval_seconds": 60,
        })
        self.assertEqual(create_resp.status_code, 201)
        mid = create_resp.json()["id"]

        del_resp = self.client.delete(f"/monitors/{mid}")
        self.assertEqual(del_resp.status_code, 204)

        get_resp = self.client.get(f"/monitors/{mid}")
        self.assertEqual(get_resp.status_code, 404)

    def test_get_monitor_results_api(self):
        """Verify GET /monitors/{id}/results returns historical telemetry."""
        create_resp = self.client.post("/monitors/", json={
            "name": "Results Test",
            "url": "https://example.com/results-test",
            "check_interval_seconds": 60,
        })
        self.assertEqual(create_resp.status_code, 201)
        mid = create_resp.json()["id"]

        # Directly query empty results
        res_resp = self.client.get(f"/monitors/{mid}/results")
        self.assertEqual(res_resp.status_code, 200)
        self.assertEqual(res_resp.json(), [])

        # Non-existent monitor 404
        bad_resp = self.client.get("/monitors/999999/results")
        self.assertEqual(bad_resp.status_code, 404)


if __name__ == "__main__":
    unittest.main()
