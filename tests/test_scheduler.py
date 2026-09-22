"""
Automated Test Suite for Periodic Scheduler Sweep.
"""

import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch
import psycopg2
import redis

from app.db import DATABASE_URL, SyncSessionLocal, get_sync_db
from app.models import Monitor, PingResult
from app.enums import MonitorMode, MonitorStatus
from app.tasks import execute_keep_alive, execute_ping, sweep_due_monitors
from app.worker import celery_app


def _get_pg_conn_str(url: str) -> str:
    """Strip SQLAlchemy driver prefixes for native psycopg2 connection."""
    for prefix in ("+asyncpg", "+psycopg2"):
        url = url.replace(prefix, "")
    return url


class TestSchedulerSweep(unittest.TestCase):
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
        name: str = "Test Scheduler Monitor",
        url: str = "https://example.com",
        check_interval_seconds: int = 60,
        mode: str = "monitor",
        next_check_at: datetime | None = None,
        keep_alive_enabled: bool = False,
        keep_alive_interval_seconds: int | None = None,
        keep_alive_path: str | None = None,
        next_keep_alive_at: datetime | None = None,
    ) -> int:
        """Helper to synchronously seed a Monitor into PostgreSQL."""
        with get_sync_db() as session:
            monitor = Monitor(
                name=name,
                url=url,
                check_interval_seconds=check_interval_seconds,
                status=MonitorStatus.PENDING.value,
                mode=mode,
                next_check_at=next_check_at,
                keep_alive_enabled=keep_alive_enabled,
                keep_alive_interval_seconds=keep_alive_interval_seconds,
                keep_alive_path=keep_alive_path,
                next_keep_alive_at=next_keep_alive_at,
            )
            session.add(monitor)
            session.flush()
            mid = monitor.id
        return mid

    def test_beat_schedule_configuration(self) -> None:
        """Verify Celery Beat static periodic task entries and absence of dynamic per-monitor entries."""
        beat_sched = celery_app.conf.beat_schedule
        self.assertIn("sweep-due-monitors", beat_sched)
        self.assertEqual(beat_sched["sweep-due-monitors"]["task"], "app.tasks.sweep_due_monitors")
        self.assertGreater(beat_sched["sweep-due-monitors"]["schedule"], 0)
        self.assertIn("prune-ping-results", beat_sched)
        self.assertEqual(beat_sched["prune-ping-results"]["task"], "app.tasks.prune_ping_results")
        # Ensure only the static system tasks exist (no dynamic per-monitor entries created)
        self.assertEqual(len(beat_sched), 2)


    @patch("app.tasks.execute_ping.delay")
    @patch("app.tasks.execute_keep_alive.delay")
    def test_sweep_monitoring_due(self, mock_ka_delay: MagicMock, mock_ping_delay: MagicMock) -> None:
        """Verify overdue monitor triggers execute_ping and advances next_check_at."""
        now = datetime.now(timezone.utc)
        past_time = now - timedelta(seconds=10)
        mid = self._create_test_monitor(
            check_interval_seconds=60,
            mode=MonitorMode.MONITOR.value,
            next_check_at=past_time,
            keep_alive_enabled=False,
        )

        res = sweep_due_monitors()
        self.assertEqual(res["monitors_enqueued"], 1)
        self.assertEqual(res["keep_alives_enqueued"], 0)

        # Verify Celery delay was called with monitor_id
        mock_ping_delay.assert_called_once_with(mid)
        mock_ka_delay.assert_not_called()

        # Verify next_check_at advanced into the future (~ now + 60s)
        with get_sync_db() as session:
            mon = session.get(Monitor, mid)
            self.assertIsNotNone(mon.next_check_at)
            self.assertGreater(mon.next_check_at, now)
            diff = (mon.next_check_at - now).total_seconds()
            self.assertAlmostEqual(diff, 60.0, delta=5.0)

    @patch("app.tasks.execute_ping.delay")
    @patch("app.tasks.execute_keep_alive.delay")
    def test_sweep_keep_alive_due(self, mock_ka_delay: MagicMock, mock_ping_delay: MagicMock) -> None:
        """Verify overdue keep-alive triggers execute_keep_alive and advances next_keep_alive_at."""
        now = datetime.now(timezone.utc)
        past_time = now - timedelta(seconds=15)
        mid = self._create_test_monitor(
            mode=MonitorMode.KEEP_ALIVE.value,
            keep_alive_enabled=True,
            keep_alive_interval_seconds=300,
            keep_alive_path="/ping",
            next_keep_alive_at=past_time,
        )

        res = sweep_due_monitors()
        self.assertEqual(res["monitors_enqueued"], 0)
        self.assertEqual(res["keep_alives_enqueued"], 1)

        mock_ka_delay.assert_called_once_with(mid)
        mock_ping_delay.assert_not_called()

        with get_sync_db() as session:
            mon = session.get(Monitor, mid)
            self.assertIsNotNone(mon.next_keep_alive_at)
            self.assertGreater(mon.next_keep_alive_at, now)
            diff = (mon.next_keep_alive_at - now).total_seconds()
            self.assertAlmostEqual(diff, 300.0, delta=5.0)

    @patch("app.tasks.execute_ping.delay")
    @patch("app.tasks.execute_keep_alive.delay")
    def test_sweep_dual_mode_both_due(self, mock_ka_delay: MagicMock, mock_ping_delay: MagicMock) -> None:
        """Verify monitor_and_keep_alive mode independently triggers both tasks when both are due."""
        now = datetime.now(timezone.utc)
        past_time = now - timedelta(seconds=5)
        mid = self._create_test_monitor(
            check_interval_seconds=45,
            mode=MonitorMode.MONITOR_AND_KEEP_ALIVE.value,
            next_check_at=past_time,
            keep_alive_enabled=True,
            keep_alive_interval_seconds=180,
            keep_alive_path="/health",
            next_keep_alive_at=past_time,
        )

        res = sweep_due_monitors()
        self.assertEqual(res["monitors_enqueued"], 1)
        self.assertEqual(res["keep_alives_enqueued"], 1)

        mock_ping_delay.assert_called_once_with(mid)
        mock_ka_delay.assert_called_once_with(mid)

        with get_sync_db() as session:
            mon = session.get(Monitor, mid)
            self.assertAlmostEqual((mon.next_check_at - now).total_seconds(), 45.0, delta=5.0)
            self.assertAlmostEqual((mon.next_keep_alive_at - now).total_seconds(), 180.0, delta=5.0)

    @patch("app.tasks.execute_ping.delay")
    @patch("app.tasks.execute_keep_alive.delay")
    def test_sweep_dual_mode_independent_timing(self, mock_ka_delay: MagicMock, mock_ping_delay: MagicMock) -> None:
        """Verify only the overdue schedule triggers when intervals differ."""
        now = datetime.now(timezone.utc)
        past_time = now - timedelta(seconds=10)
        future_time = now + timedelta(seconds=200)

        mid = self._create_test_monitor(
            check_interval_seconds=60,
            mode=MonitorMode.MONITOR_AND_KEEP_ALIVE.value,
            next_check_at=past_time,  # DUE
            keep_alive_enabled=True,
            keep_alive_interval_seconds=300,
            keep_alive_path="/health",
            next_keep_alive_at=future_time,  # NOT DUE
        )

        res = sweep_due_monitors()
        self.assertEqual(res["monitors_enqueued"], 1)
        self.assertEqual(res["keep_alives_enqueued"], 0)

        mock_ping_delay.assert_called_once_with(mid)
        mock_ka_delay.assert_not_called()

        with get_sync_db() as session:
            mon = session.get(Monitor, mid)
            # next_check_at advanced
            self.assertAlmostEqual((mon.next_check_at - now).total_seconds(), 60.0, delta=5.0)
            # next_keep_alive_at was untouched
            self.assertEqual(mon.next_keep_alive_at, future_time)

    @patch("app.tasks.execute_ping.delay")
    @patch("app.tasks.execute_keep_alive.delay")
    def test_sweep_keep_alive_disabled_never_enqueued(self, mock_ka_delay: MagicMock, mock_ping_delay: MagicMock) -> None:
        """Gatekeeper: If keep_alive_enabled=False, never enqueue keep-alive even if timestamp is past."""
        now = datetime.now(timezone.utc)
        past_time = now - timedelta(seconds=50)

        mid = self._create_test_monitor(
            mode=MonitorMode.MONITOR.value,
            next_check_at=now + timedelta(seconds=600),  # Not due
            keep_alive_enabled=False,
            next_keep_alive_at=past_time,  # Past timestamp
        )

        res = sweep_due_monitors()
        self.assertEqual(res["monitors_enqueued"], 0)
        self.assertEqual(res["keep_alives_enqueued"], 0)
        mock_ka_delay.assert_not_called()
        mock_ping_delay.assert_not_called()

    def test_sweep_skip_locked_concurrency(self) -> None:
        """Verify that SELECT FOR UPDATE SKIP LOCKED avoids claiming rows locked by another transaction."""
        now = datetime.now(timezone.utc)
        past_time = now - timedelta(seconds=20)
        mid = self._create_test_monitor(
            mode=MonitorMode.MONITOR.value,
            next_check_at=past_time,
        )

        session_lock = SyncSessionLocal()
        try:
            # Transaction 1 acquires row lock
            locked_monitors = (
                session_lock.query(Monitor)
                .filter(Monitor.id == mid)
                .with_for_update(skip_locked=True)
                .all()
            )
            self.assertEqual(len(locked_monitors), 1)

            # Transaction 2 (sweep_due_monitors) runs concurrently: must skip locked row
            with patch("app.tasks.execute_ping.delay") as mock_delay:
                res = sweep_due_monitors()
                self.assertEqual(res["monitors_enqueued"], 0)
                mock_delay.assert_not_called()

            # Transaction 1 releases lock
            session_lock.commit()
        finally:
            session_lock.close()

        # Now that lock is released, subsequent sweep must claim it
        with patch("app.tasks.execute_ping.delay") as mock_delay:
            res = sweep_due_monitors()
            self.assertEqual(res["monitors_enqueued"], 1)
            mock_delay.assert_called_once_with(mid)

    @patch("app.tasks.execute_ping.delay")
    def test_sweep_missed_schedules_no_catchup_storm(self, mock_delay: MagicMock) -> None:
        """Anti-storm rule: A monitor 3 hours overdue enqueues ONE check and advances from current time."""
        now = datetime.now(timezone.utc)
        three_hours_ago = now - timedelta(hours=3)
        mid = self._create_test_monitor(
            check_interval_seconds=60,
            mode=MonitorMode.MONITOR.value,
            next_check_at=three_hours_ago,
        )

        res = sweep_due_monitors()
        self.assertEqual(res["monitors_enqueued"], 1)
        mock_delay.assert_called_once_with(mid)

        with get_sync_db() as session:
            mon = session.get(Monitor, mid)
            # Advanced from 'now', NOT from three_hours_ago + 60s!
            self.assertGreater(mon.next_check_at, now)
            diff = (mon.next_check_at - now).total_seconds()
            self.assertAlmostEqual(diff, 60.0, delta=5.0)

    @patch("app.tasks.execute_ping.delay", side_effect=redis.exceptions.ConnectionError("Redis connection lost"))
    def test_sweep_redis_failure_rolls_back_db(self, mock_delay: MagicMock) -> None:
        """Resilience: If task enqueueing fails, DB transaction rolls back and schedule is not advanced."""
        now = datetime.now(timezone.utc)
        past_time = now - timedelta(seconds=30)
        mid = self._create_test_monitor(
            mode=MonitorMode.MONITOR.value,
            next_check_at=past_time,
        )

        with self.assertRaises(redis.exceptions.ConnectionError):
            sweep_due_monitors()

        with get_sync_db() as session:
            mon = session.get(Monitor, mid)
            # next_check_at must NOT have advanced!
            self.assertEqual(mon.next_check_at, past_time)

    def test_sweep_idle_no_monitors_due(self) -> None:
        """Verify clean idle run when no monitors are due."""
        now = datetime.now(timezone.utc)
        self._create_test_monitor(
            mode=MonitorMode.MONITOR.value,
            next_check_at=now + timedelta(hours=1),
        )

        res = sweep_due_monitors()
        self.assertEqual(res["monitors_enqueued"], 0)
        self.assertEqual(res["keep_alives_enqueued"], 0)


if __name__ == "__main__":
    unittest.main()
