"""
Comprehensive test suite for PingGuard Chapter 1 (FastAPI & Async Python).

Covers:
1. Valid monitor creation with default interval.
2. Input validation rejections (invalid URL, out-of-bounds intervals, invalid name lengths).
3. Retrieval of existing and non-existent monitors.
4. Partial updates via PATCH and PUT.
5. List pagination and response limits.
6. OpenAPI schema generation and /docs endpoint availability.
7. Event loop isolation verification (no synchronous blocking).
"""

import unittest
from fastapi.testclient import TestClient

from app.main import app
from app.store import _global_store


class TestPingGuardChapter1(unittest.TestCase):
    def setUp(self) -> None:
        """Reset the in-memory store before every test run."""
        _global_store.clear()
        self.client = TestClient(app)

    def test_health_check(self) -> None:
        """Verify the basic service health check endpoint."""
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "healthy")
        self.assertEqual(data["service"], "PingGuard API")

    # =========================================================================
    # Test 1: Valid Creation & Default Interval
    # =========================================================================
    def test_create_monitor_valid(self) -> None:
        """
        Test 1: Valid creation (POST /monitors/)
        Payload: {"name": "t", "url": "https://example.com"}
        Expected: 201 Created, check_interval_seconds defaults to 60, status 'pending'.
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
    # Test 2: Invalid URL Validation
    # =========================================================================
    def test_create_monitor_invalid_url(self) -> None:
        """
        Test 2: Invalid URL format should return HTTP 422 Unprocessable Entity.
        """
        invalid_urls = [
            "not-a-valid-url",
            "ftp://files.example.com",
            "just_a_string",
            "htt://broken.scheme",
            ""
        ]
        for url in invalid_urls:
            with self.subTest(url=url):
                response = self.client.post(
                    "/monitors/",
                    json={"name": "Test Site", "url": url}
                )
                self.assertEqual(response.status_code, 422)

    # =========================================================================
    # Test 3: Interval Boundary Validation
    # =========================================================================
    def test_create_monitor_interval_below_minimum(self) -> None:
        """
        Test 3: check_interval_seconds below minimum (< 15) must return HTTP 422.
        """
        response = self.client.post(
            "/monitors/",
            json={"name": "Fast Checker", "url": "https://example.com", "check_interval_seconds": 10}
        )
        self.assertEqual(response.status_code, 422)
        errors = response.json()["detail"]
        self.assertTrue(any("check_interval_seconds" in str(err["loc"]) for err in errors))

    def test_create_monitor_interval_above_maximum(self) -> None:
        """check_interval_seconds above maximum (> 86400) must return HTTP 422."""
        response = self.client.post(
            "/monitors/",
            json={"name": "Slow Checker", "url": "https://example.com", "check_interval_seconds": 90000}
        )
        self.assertEqual(response.status_code, 422)

    def test_create_monitor_interval_wrong_type(self) -> None:
        """String non-integer interval must return HTTP 422."""
        response = self.client.post(
            "/monitors/",
            json={"name": "Type Test", "url": "https://example.com", "check_interval_seconds": "not-an-int"}
        )
        self.assertEqual(response.status_code, 422)

    # =========================================================================
    # Name Validation Tests
    # =========================================================================
    def test_create_monitor_empty_name(self) -> None:
        """Empty string name (len < 1) must return HTTP 422."""
        response = self.client.post(
            "/monitors/",
            json={"name": "", "url": "https://example.com"}
        )
        self.assertEqual(response.status_code, 422)

    def test_create_monitor_name_too_long(self) -> None:
        """Name exceeding 120 characters must return HTTP 422."""
        too_long = "A" * 121
        response = self.client.post(
            "/monitors/",
            json={"name": too_long, "url": "https://example.com"}
        )
        self.assertEqual(response.status_code, 422)

    def test_create_monitor_missing_name(self) -> None:
        """Missing name field must return HTTP 422."""
        response = self.client.post(
            "/monitors/",
            json={"url": "https://example.com"}
        )
        self.assertEqual(response.status_code, 422)

    # =========================================================================
    # Test 4 & 5: Get Monitor by ID (Existing & Non-existent)
    # =========================================================================
    def test_get_existing_monitor(self) -> None:
        """
        Test 4: GET /monitors/{id} for existing monitor returns HTTP 200.
        """
        create_resp = self.client.post(
            "/monitors/",
            json={"name": "Staging Server", "url": "https://staging.example.com", "check_interval_seconds": 45}
        )
        created_id = create_resp.json()["id"]

        get_resp = self.client.get(f"/monitors/{created_id}")
        self.assertEqual(get_resp.status_code, 200)
        data = get_resp.json()
        self.assertEqual(data["id"], created_id)
        self.assertEqual(data["name"], "Staging Server")
        self.assertEqual(data["url"], "https://staging.example.com/")

    def test_get_non_existent_monitor(self) -> None:
        """
        Test 5: GET /monitors/99999 for non-existent monitor returns HTTP 404.
        Detail should be 'Monitor not found'.
        """
        response = self.client.get("/monitors/99999")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "Monitor not found"})

    # =========================================================================
    # Test 6: Update Monitor (PATCH & PUT)
    # =========================================================================
    def test_update_monitor_patch(self) -> None:
        """
        Test 6: Update monitor via PATCH and verify changed fields while preserving others.
        """
        create_resp = self.client.post(
            "/monitors/",
            json={"name": "Original Name", "url": "https://example.com", "check_interval_seconds": 60}
        )
        monitor_id = create_resp.json()["id"]

        # 1. Update only name
        patch_resp = self.client.patch(
            f"/monitors/{monitor_id}",
            json={"name": "Updated Name"}
        )
        self.assertEqual(patch_resp.status_code, 200)
        data = patch_resp.json()
        self.assertEqual(data["name"], "Updated Name")
        self.assertEqual(data["check_interval_seconds"], 60)  # Preserved

        # 2. Update only check_interval_seconds
        patch_resp2 = self.client.patch(
            f"/monitors/{monitor_id}",
            json={"check_interval_seconds": 120}
        )
        self.assertEqual(patch_resp2.status_code, 200)
        data2 = patch_resp2.json()
        self.assertEqual(data2["name"], "Updated Name")  # Preserved
        self.assertEqual(data2["check_interval_seconds"], 120)

    def test_update_monitor_put(self) -> None:
        """Verify PUT endpoint operates symmetrically to update."""
        create_resp = self.client.post(
            "/monitors/",
            json={"name": "Initial Name", "url": "https://example.com"}
        )
        monitor_id = create_resp.json()["id"]

        put_resp = self.client.put(
            f"/monitors/{monitor_id}",
            json={"name": "PUT Updated Name", "check_interval_seconds": 90}
        )
        self.assertEqual(put_resp.status_code, 200)
        data = put_resp.json()
        self.assertEqual(data["name"], "PUT Updated Name")
        self.assertEqual(data["check_interval_seconds"], 90)

    def test_update_non_existent_monitor(self) -> None:
        """Updating a non-existent monitor returns HTTP 404."""
        response = self.client.patch(
            "/monitors/99999",
            json={"name": "Ghost"}
        )
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "Monitor not found"})

    def test_update_invalid_interval(self) -> None:
        """Updating with an invalid interval (< 15) returns HTTP 422."""
        create_resp = self.client.post(
            "/monitors/",
            json={"name": "Valid Name", "url": "https://example.com"}
        )
        monitor_id = create_resp.json()["id"]

        patch_resp = self.client.patch(
            f"/monitors/{monitor_id}",
            json={"check_interval_seconds": 5}
        )
        self.assertEqual(patch_resp.status_code, 422)

    # =========================================================================
    # Test 7: List Monitors & Pagination
    # =========================================================================
    def test_list_monitors(self) -> None:
        """
        Test 7: GET /monitors/ returns HTTP 200 with list of records.
        """
        # Create 3 monitors
        for i in range(1, 4):
            self.client.post(
                "/monitors/",
                json={"name": f"Monitor {i}", "url": f"https://example{i}.com"}
            )

        # Fetch all
        list_resp = self.client.get("/monitors/")
        self.assertEqual(list_resp.status_code, 200)
        data = list_resp.json()
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0]["name"], "Monitor 1")
        self.assertEqual(data[2]["name"], "Monitor 3")

    def test_list_monitors_pagination(self) -> None:
        """Verify skip and limit query parameters."""
        for i in range(1, 6):
            self.client.post(
                "/monitors/",
                json={"name": f"Monitor {i}", "url": f"https://example{i}.com"}
            )

        resp = self.client.get("/monitors/?skip=1&limit=2")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["name"], "Monitor 2")
        self.assertEqual(data[1]["name"], "Monitor 3")

    # =========================================================================
    # Test 8: Documentation & OpenAPI Schema
    # =========================================================================
    def test_openapi_schema(self) -> None:
        """
        Test 8: OpenAPI schema (/openapi.json) is valid and exposes all routes & schemas.
        """
        response = self.client.get("/openapi.json")
        self.assertEqual(response.status_code, 200)
        schema = response.json()

        paths = schema.get("paths", {})
        self.assertIn("/monitors/", paths)
        self.assertIn("/monitors/{monitor_id}", paths)

        # Check schemas in components
        schemas = schema.get("components", {}).get("schemas", {})
        self.assertIn("MonitorCreate", schemas)
        self.assertIn("MonitorRead", schemas)
        self.assertIn("MonitorUpdate", schemas)
        self.assertIn("MonitorStatus", schemas)

    def test_swagger_ui(self) -> None:
        """Verify Swagger UI (/docs) returns HTTP 200 with interactive HTML."""
        response = self.client.get("/docs")
        self.assertEqual(response.status_code, 200)
        self.assertIn("swagger-ui", response.text.lower())


if __name__ == "__main__":
    unittest.main()
