"""
Automated Test Suite for Chapter 3 — Celery & Redis (Distributed Task Execution)

Validates:
1. Safe URL joining helper for Keep-Alive destinations.
2. `execute_ping` successful execution, latency recording, and status mapping.
3. `execute_ping` non-existent monitor handling (clean exit without crash).
4. `execute_keep_alive` successful execution and telemetry recording.
5. `execute_keep_alive` gatekeeper: skips cleanly when keep_alive_enabled=False.
6. `execute_keep_alive` does not alter primary monitor uptime status.
7. Transient error handling and timeout classification.
8. Non-retry of standard HTTP 4xx/5xx responses.
9. Task idempotency across repeat executions.
10. Celery `.delay()` asynchronous invocation flow.
"""

import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
import psycopg2
import requests
from requests.exceptions import ConnectionError as ReqConnectionError, Timeout

from app.db import DATABASE_URL, get_sync_db
from app.models import Monitor, PingResult
from app.schemas import MonitorMode, MonitorStatus
from app.tasks import execute_keep_alive, execute_ping, safe_join_url
from app.worker import celery_app


def _get_pg_conn_str(url: str) -> str:
    """Strip SQLAlchemy driver prefixes for native psycopg2 connection."""
    for prefix in ("+asyncpg", "+psycopg2"):
        url = url.replace(prefix, "")
    return url


class TestChapter3Tasks(unittest.TestCase):
    def setUp(self) -> None:
        """Reset PostgreSQL tables before each test run."""
        sync_url = _get_pg_conn_str(DATABASE_URL)
        conn = psycopg2.connect(sync_url)
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE ping_results, monitors RESTART IDENTITY CASCADE;")
        conn.commit()
        cur.close()
        conn.close()

    def _create_test_monitor(
        self,
        name: str = "Test Target",
        url: str = "https://example.com",
        mode: str = "monitor",
        keep_alive_enabled: bool = False,
        keep_alive_path: str | None = None,
    ) -> int:
        """Helper to synchronously seed a Monitor into PostgreSQL."""
        with get_sync_db() as session:
            monitor = Monitor(
                name=name,
                url=url,
                check_interval_seconds=60,
                status=MonitorStatus.PENDING.value,
                mode=mode,
                keep_alive_enabled=keep_alive_enabled,
                keep_alive_interval_seconds=300 if keep_alive_enabled else None,
                keep_alive_path=keep_alive_path,
                next_check_at=datetime.now(timezone.utc),
            )
            session.add(monitor)
            session.flush()
            session.refresh(monitor)
            return monitor.id

    # =========================================================================
    # URL Combination Helper Tests
    # =========================================================================
    def test_safe_join_url_variations(self) -> None:
        """Verify safe URL joining avoids double slashes across all path patterns."""
        self.assertEqual(safe_join_url("https://xyz.com", "/health"), "https://xyz.com/health")
        self.assertEqual(safe_join_url("https://xyz.com/", "/health"), "https://xyz.com/health")
        self.assertEqual(safe_join_url("https://xyz.com", "health"), "https://xyz.com/health")
        self.assertEqual(safe_join_url("https://xyz.com/api/", "/v1/ping"), "https://xyz.com/api/v1/ping")
        self.assertEqual(safe_join_url("https://xyz.com", None), "https://xyz.com")
        self.assertEqual(safe_join_url("https://xyz.com", "  "), "https://xyz.com")

    # =========================================================================
    # execute_ping Tests
    # =========================================================================
    @patch("requests.get")
    def test_execute_ping_success(self, mock_get: MagicMock) -> None:
        """
        Verify execute_ping performs HTTP probe, records PingResult with check_type='monitor',
        updates monitor status to 'up', and records last_checked_at.
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_get.return_value = mock_resp

        mid = self._create_test_monitor(url="https://healthy-target.com")

        result = execute_ping(mid)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["monitor_id"], mid)
        self.assertEqual(result["check_type"], "monitor")
        self.assertEqual(result["status_code"], 200)
        self.assertIsNone(result["error"])
        self.assertGreater(result["latency_ms"], 0)

        # Verify DB state
        with get_sync_db() as session:
            mon = session.get(Monitor, mid)
            self.assertIsNotNone(mon)
            self.assertEqual(mon.status, MonitorStatus.UP.value)
            self.assertIsNotNone(mon.last_checked_at)

            pr = session.query(PingResult).filter_by(monitor_id=mid).first()
            self.assertIsNotNone(pr)
            self.assertEqual(pr.check_type, "monitor")
            self.assertEqual(pr.status_code, 200)
            self.assertIsNone(pr.error)

    @patch("requests.get")
    def test_execute_ping_status_mappings(self, mock_get: MagicMock) -> None:
        """Verify HTTP 404 maps to DEGRADED and HTTP 500 maps to DOWN."""
        mid = self._create_test_monitor(url="https://status-target.com")

        # 404 -> DEGRADED
        mock_resp_404 = MagicMock(status_code=404)
        mock_get.return_value = mock_resp_404
        execute_ping(mid)
        with get_sync_db() as session:
            self.assertEqual(session.get(Monitor, mid).status, MonitorStatus.DEGRADED.value)

        # 500 -> DOWN
        mock_resp_500 = MagicMock(status_code=500)
        mock_get.return_value = mock_resp_500
        execute_ping(mid)
        with get_sync_db() as session:
            self.assertEqual(session.get(Monitor, mid).status, MonitorStatus.DOWN.value)

    def test_execute_ping_nonexistent_monitor(self) -> None:
        """Verify execute_ping handles non-existent monitor cleanly without raising exception."""
        result = execute_ping(99999)
        self.assertEqual(result["status"], "not_found")
        self.assertEqual(result["monitor_id"], 99999)

    # =========================================================================
    # execute_keep_alive Tests
    # =========================================================================
    @patch("requests.get")
    def test_execute_keep_alive_success(self, mock_get: MagicMock) -> None:
        """
        Verify execute_keep_alive contacts `url + keep_alive_path`, writes PingResult(check_type='keep_alive'),
        and leaves Monitor.status untouched.
        """
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_get.return_value = mock_resp

        mid = self._create_test_monitor(
            url="https://service-target.org",
            mode="monitor_and_keep_alive",
            keep_alive_enabled=True,
            keep_alive_path="/healthcheck",
        )

        result = execute_keep_alive(mid)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["check_type"], "keep_alive")
        self.assertEqual(result["status_code"], 200)

        # Verify requests.get was called with the joined path
        mock_get.assert_called_once()
        called_url = mock_get.call_args[0][0]
        self.assertEqual(called_url, "https://service-target.org/healthcheck")

        # Verify PingResult recorded with check_type='keep_alive'
        with get_sync_db() as session:
            mon = session.get(Monitor, mid)
            self.assertEqual(mon.status, MonitorStatus.PENDING.value)  # Untouched by keep-alive

            pr = session.query(PingResult).filter_by(monitor_id=mid, check_type="keep_alive").first()
            self.assertIsNotNone(pr)
            self.assertEqual(pr.status_code, 200)

    @patch("requests.get")
    def test_execute_keep_alive_skipped_when_disabled(self, mock_get: MagicMock) -> None:
        """
        Critical Rule: If keep_alive_enabled=False, execute_keep_alive MUST NOT make any HTTP
        request and MUST NOT persist a success PingResult.
        """
        mid = self._create_test_monitor(
            url="https://disabled-keepalive.com",
            mode="monitor",
            keep_alive_enabled=False,
        )

        result = execute_keep_alive(mid)
        self.assertEqual(result["status"], "skipped")
        self.assertEqual(result["reason"], "keep_alive_disabled")

        # Verify zero HTTP requests were made
        mock_get.assert_not_called()

        # Verify no PingResult was created
        with get_sync_db() as session:
            pr_count = session.query(PingResult).filter_by(monitor_id=mid).count()
            self.assertEqual(pr_count, 0)

    def test_execute_keep_alive_nonexistent_monitor(self) -> None:
        """Verify execute_keep_alive handles non-existent monitor cleanly."""
        result = execute_keep_alive(99999)
        self.assertEqual(result["status"], "not_found")
        self.assertEqual(result["monitor_id"], 99999)

    # =========================================================================
    # Failure & Transient Error Handling
    # =========================================================================
    @patch("requests.get", side_effect=Timeout("Connection timed out after 5000ms"))
    def test_execute_ping_timeout_permanent_failure(self, mock_get: MagicMock) -> None:
        """Verify exhausted retries persist error='timeout' and set Monitor.status='down'."""
        mid = self._create_test_monitor(url="https://timeout-target.com")

        # Set task.request.retries = 3 via Celery push_request
        execute_ping.push_request(retries=3)
        try:
            result = execute_ping(mid)
        finally:
            execute_ping.pop_request()

        self.assertEqual(result["status"], "completed")
        self.assertIn("timeout", result["error"])
        self.assertIsNone(result["status_code"])

        with get_sync_db() as session:
            mon = session.get(Monitor, mid)
            self.assertEqual(mon.status, MonitorStatus.DOWN.value)

            pr = session.query(PingResult).filter_by(monitor_id=mid).first()
            self.assertIsNotNone(pr)
            self.assertIn("timeout", pr.error)

    # =========================================================================
    # Idempotency & Repeat Execution Test
    # =========================================================================
    @patch("requests.get")
    def test_task_idempotency_multiple_executions(self, mock_get: MagicMock) -> None:
        """
        Verify that running tasks repeatedly does not corrupt monitor state,
        properly records successive telemetry in PingResult, and updates last_checked_at.
        """
        mock_get.return_value = MagicMock(status_code=200)

        mid = self._create_test_monitor(
            url="https://idempotent-target.com",
            mode="monitor_and_keep_alive",
            keep_alive_enabled=True,
            keep_alive_path="/ping",
        )

        # Run 2 pings and 2 keep-alives
        execute_ping(mid)
        execute_keep_alive(mid)
        execute_ping(mid)
        execute_keep_alive(mid)

        with get_sync_db() as session:
            # Monitor record remains unique and valid
            mon = session.get(Monitor, mid)
            self.assertEqual(mon.status, MonitorStatus.UP.value)

            # Exactly 4 distinct telemetry rows exist
            results = session.query(PingResult).filter_by(monitor_id=mid).all()
            self.assertEqual(len(results), 4)
            monitor_results = [r for r in results if r.check_type == "monitor"]
            keep_alive_results = [r for r in results if r.check_type == "keep_alive"]
            self.assertEqual(len(monitor_results), 2)
            self.assertEqual(len(keep_alive_results), 2)

    # =========================================================================
    # Celery .delay() Asynchronous Invocation Test
    # =========================================================================
    @patch("requests.get")
    def test_celery_delay_eager_execution(self, mock_get: MagicMock) -> None:
        """
        Verify tasks can be queued via Celery's .delay() method.
        Uses Celery eager mode to test queueing mechanics within the test process.
        """
        mock_get.return_value = MagicMock(status_code=200)

        mid = self._create_test_monitor(
            url="https://celery-eager.com",
            mode="monitor_and_keep_alive",
            keep_alive_enabled=True,
            keep_alive_path="/health",
        )

        # Temporarily enable task_always_eager for in-process queue simulation
        celery_app.conf.task_always_eager = True
        try:
            async_ping = execute_ping.delay(mid)
            self.assertTrue(async_ping.ready())
            ping_res = async_ping.get()
            self.assertEqual(ping_res["status"], "completed")
            self.assertEqual(ping_res["check_type"], "monitor")

            async_ka = execute_keep_alive.delay(mid)
            self.assertTrue(async_ka.ready())
            ka_res = async_ka.get()
            self.assertEqual(ka_res["status"], "completed")
            self.assertEqual(ka_res["check_type"], "keep_alive")
        finally:
            celery_app.conf.task_always_eager = False


if __name__ == "__main__":
    unittest.main()
