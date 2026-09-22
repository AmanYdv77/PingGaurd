"""
Automated Test Suite for Worker Tasks and Network Probing Integration.
"""

import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
import psycopg2

from app.db import DATABASE_URL, get_sync_db
from app.models import Monitor, PingResult
from app.enums import MonitorMode, MonitorStatus, PingOutcome
from app.net import PingResultDTO
from app.tasks import execute_keep_alive, execute_ping, safe_join_url
from app.worker import celery_app


def _get_pg_conn_str(url: str) -> str:
    """Strip SQLAlchemy driver prefixes for native psycopg2 connection."""
    for prefix in ("+asyncpg", "+psycopg2"):
        url = url.replace(prefix, "")
    return url


class TestWorkerTasks(unittest.TestCase):
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
                next_keep_alive_at=datetime.now(timezone.utc) if keep_alive_enabled else None,
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
    @patch("app.tasks.robust_ping")
    def test_execute_ping_success(self, mock_ping: MagicMock) -> None:
        """
        Verify execute_ping performs probe via robust_ping, records PingResult with check_type='monitor',
        updates monitor status to 'up', and records last_checked_at.
        """
        mock_ping.return_value = PingResultDTO(
            outcome=PingOutcome.UP,
            status_code=200,
            latency_ms=25.5,
            error_detail=None,
            original_url="https://healthy-target.com",
            final_url="https://healthy-target.com",
        )

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

    @patch("app.tasks.robust_ping")
    def test_execute_ping_status_mappings(self, mock_ping: MagicMock) -> None:
        """Verify HTTP 404 maps to DEGRADED and HTTP 500 maps to DOWN."""
        mid = self._create_test_monitor(url="https://status-target.com")

        # 404 -> DEGRADED
        mock_ping.return_value = PingResultDTO(
            outcome=PingOutcome.DEGRADED,
            status_code=404,
            latency_ms=18.0,
            error_detail=None,
            original_url="https://status-target.com",
            final_url="https://status-target.com",
        )
        execute_ping(mid)
        with get_sync_db() as session:
            self.assertEqual(session.get(Monitor, mid).status, MonitorStatus.DEGRADED.value)

        # 500 -> DOWN
        mock_ping.return_value = PingResultDTO(
            outcome=PingOutcome.DOWN,
            status_code=500,
            latency_ms=22.0,
            error_detail=None,
            original_url="https://status-target.com",
            final_url="https://status-target.com",
        )
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
    @patch("app.tasks.robust_keep_alive")
    def test_execute_keep_alive_success(self, mock_ka: MagicMock) -> None:
        """
        Verify execute_keep_alive contacts `url + keep_alive_path`, writes PingResult(check_type='keep_alive'),
        and leaves Monitor.status untouched.
        """
        mock_ka.return_value = PingResultDTO(
            outcome=PingOutcome.UP,
            status_code=200,
            latency_ms=14.2,
            error_detail=None,
            original_url="https://service-target.org/healthcheck",
            final_url="https://service-target.org/healthcheck",
        )

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

        mock_ka.assert_called_once_with("https://service-target.org", "/healthcheck")

        # Verify PingResult recorded with check_type='keep_alive'
        with get_sync_db() as session:
            mon = session.get(Monitor, mid)
            self.assertEqual(mon.status, MonitorStatus.PENDING.value)  # Untouched by keep-alive

            pr = session.query(PingResult).filter_by(monitor_id=mid, check_type="keep_alive").first()
            self.assertIsNotNone(pr)
            self.assertEqual(pr.status_code, 200)

    @patch("app.tasks.robust_keep_alive")
    def test_execute_keep_alive_skipped_when_disabled(self, mock_ka: MagicMock) -> None:
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

        mock_ka.assert_not_called()

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
    @patch("app.tasks.robust_ping")
    def test_execute_ping_timeout_permanent_failure(self, mock_ping: MagicMock) -> None:
        """Verify exhausted retries persist error='read_timeout' and set Monitor.status='down'."""
        mock_ping.return_value = PingResultDTO(
            outcome=PingOutcome.UNREACHABLE,
            status_code=None,
            latency_ms=5000.0,
            error_detail="read_timeout",
            original_url="https://timeout-target.com",
            final_url=None,
        )

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
    @patch("app.tasks.robust_keep_alive")
    @patch("app.tasks.robust_ping")
    def test_task_idempotency_multiple_executions(self, mock_ping: MagicMock, mock_ka: MagicMock) -> None:
        """
        Verify that running tasks repeatedly does not corrupt monitor state,
        properly records successive telemetry in PingResult, and updates last_checked_at.
        """
        mock_ping.return_value = PingResultDTO(
            outcome=PingOutcome.UP,
            status_code=200,
            latency_ms=15.0,
            error_detail=None,
            original_url="https://idempotent-target.com",
            final_url="https://idempotent-target.com",
        )
        mock_ka.return_value = PingResultDTO(
            outcome=PingOutcome.UP,
            status_code=200,
            latency_ms=12.0,
            error_detail=None,
            original_url="https://idempotent-target.com/ping",
            final_url="https://idempotent-target.com/ping",
        )

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
            mon = session.get(Monitor, mid)
            self.assertEqual(mon.status, MonitorStatus.UP.value)

            results = session.query(PingResult).filter_by(monitor_id=mid).all()
            self.assertEqual(len(results), 4)
            monitor_results = [r for r in results if r.check_type == "monitor"]
            keep_alive_results = [r for r in results if r.check_type == "keep_alive"]
            self.assertEqual(len(monitor_results), 2)
            self.assertEqual(len(keep_alive_results), 2)

    # =========================================================================
    # Celery .delay() Asynchronous Invocation Test
    # =========================================================================
    @patch("app.tasks.robust_keep_alive")
    @patch("app.tasks.robust_ping")
    def test_celery_delay_eager_execution(self, mock_ping: MagicMock, mock_ka: MagicMock) -> None:
        """
        Verify tasks can be queued via Celery's .delay() method.
        Uses Celery eager mode to test queueing mechanics within the test process.
        """
        mock_ping.return_value = PingResultDTO(
            outcome=PingOutcome.UP,
            status_code=200,
            latency_ms=20.0,
            error_detail=None,
            original_url="https://celery-eager.com",
            final_url="https://celery-eager.com",
        )
        mock_ka.return_value = PingResultDTO(
            outcome=PingOutcome.UP,
            status_code=200,
            latency_ms=18.0,
            error_detail=None,
            original_url="https://celery-eager.com/health",
            final_url="https://celery-eager.com/health",
        )

        mid = self._create_test_monitor(
            url="https://celery-eager.com",
            mode="monitor_and_keep_alive",
            keep_alive_enabled=True,
            keep_alive_path="/health",
        )

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

    # =========================================================================
    # Task A8: Soft Time Limit Tests
    # =========================================================================
    @patch("app.tasks.robust_ping")
    def test_execute_ping_soft_time_limit_exceeded(self, mock_ping: MagicMock) -> None:
        """Verify SoftTimeLimitExceeded writes PingResult(outcome=DOWN, error='task_soft_time_limit') and updates status='down'."""
        from celery.exceptions import SoftTimeLimitExceeded

        mock_ping.side_effect = SoftTimeLimitExceeded("Task soft time limit exceeded")
        mid = self._create_test_monitor(url="https://soft-limit-ping.com")

        result = execute_ping(mid)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["outcome"], PingOutcome.DOWN.value)
        self.assertEqual(result["error"], "task_soft_time_limit")

        with get_sync_db() as session:
            mon = session.get(Monitor, mid)
            self.assertEqual(mon.status, MonitorStatus.DOWN.value)
            self.assertIsNotNone(mon.last_checked_at)

            pr = session.query(PingResult).filter_by(monitor_id=mid).first()
            self.assertIsNotNone(pr)
            self.assertEqual(pr.check_type, "monitor")
            self.assertEqual(pr.error, "task_soft_time_limit")

    @patch("app.tasks.robust_keep_alive")
    def test_execute_keep_alive_soft_time_limit_exceeded(self, mock_ka: MagicMock) -> None:
        """Verify SoftTimeLimitExceeded in keep_alive writes PingResult and sets status='down'."""
        from celery.exceptions import SoftTimeLimitExceeded

        mock_ka.side_effect = SoftTimeLimitExceeded("Task soft time limit exceeded")
        mid = self._create_test_monitor(
            url="https://soft-limit-ka.com",
            mode="monitor_and_keep_alive",
            keep_alive_enabled=True,
            keep_alive_path="/health",
        )

        result = execute_keep_alive(mid)
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["outcome"], PingOutcome.DOWN.value)
        self.assertEqual(result["error"], "task_soft_time_limit")

        with get_sync_db() as session:
            mon = session.get(Monitor, mid)
            self.assertEqual(mon.status, MonitorStatus.DOWN.value)

            pr = session.query(PingResult).filter_by(monitor_id=mid, check_type="keep_alive").first()
            self.assertIsNotNone(pr)
            self.assertEqual(pr.error, "task_soft_time_limit")


if __name__ == "__main__":
    unittest.main()

