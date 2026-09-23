"""
Automated Test Suite for Periodic Scheduler Sweep.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import redis
from app.db import SyncSessionLocal, get_sync_db
from app.enums import MonitorMode, MonitorStatus
from app.models import Monitor
from app.tasks import sweep_due_monitors
from app.worker import celery_app


def _create_test_monitor(
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


def test_beat_schedule_configuration() -> None:
    """
    Verify Celery Beat static periodic task entries and absence of dynamic per-monitor entries.
    """
    beat_sched = celery_app.conf.beat_schedule
    assert "sweep-due-monitors" in beat_sched
    assert beat_sched["sweep-due-monitors"]["task"] == "app.tasks.sweep_due_monitors"
    assert beat_sched["sweep-due-monitors"]["schedule"] > 0
    assert "prune-ping-results" in beat_sched
    assert beat_sched["prune-ping-results"]["task"] == "app.tasks.prune_ping_results"
    assert len(beat_sched) == 2


@patch("app.tasks.execute_ping.apply_async")
@patch("app.tasks.execute_keep_alive.apply_async")
def test_sweep_monitoring_due(mock_ka_async: MagicMock, mock_ping_async: MagicMock) -> None:
    """Verify overdue monitor triggers execute_ping and advances next_check_at."""
    now = datetime.now(UTC)
    past_time = now - timedelta(seconds=10)
    mid = _create_test_monitor(
        check_interval_seconds=60,
        mode=MonitorMode.MONITOR.value,
        next_check_at=past_time,
        keep_alive_enabled=False,
    )

    res = sweep_due_monitors()
    assert res["monitors_enqueued"] == 1
    assert res["keep_alives_enqueued"] == 0

    mock_ping_async.assert_called_once_with(args=[mid], expires=60)
    mock_ka_async.assert_not_called()

    with get_sync_db() as session:
        mon = session.get(Monitor, mid)
        assert mon.next_check_at is not None
        assert mon.next_check_at > now
        diff = (mon.next_check_at - now).total_seconds()
        assert abs(diff - 60.0) <= 5.0


@patch("app.tasks.execute_ping.apply_async")
@patch("app.tasks.execute_keep_alive.apply_async")
def test_sweep_enqueues_with_task_expiration(
    mock_ka_async: MagicMock, mock_ping_async: MagicMock
) -> None:
    """Verify Celery Beat dispatches with apply_async and passes expires=check_interval_seconds."""
    now = datetime.now(UTC)
    past_time = now - timedelta(seconds=10)
    mid = _create_test_monitor(
        check_interval_seconds=60,
        mode=MonitorMode.MONITOR.value,
        next_check_at=past_time,
        keep_alive_enabled=False,
    )

    res = sweep_due_monitors()
    assert res["monitors_enqueued"] == 1
    mock_ping_async.assert_called_once_with(args=[mid], expires=60)
    mock_ka_async.assert_not_called()


@patch("app.tasks.execute_ping.apply_async")
@patch("app.tasks.execute_keep_alive.apply_async")
def test_sweep_keep_alive_due(mock_ka_async: MagicMock, mock_ping_async: MagicMock) -> None:
    """Verify overdue keep-alive triggers execute_keep_alive and advances next_keep_alive_at."""
    now = datetime.now(UTC)
    past_time = now - timedelta(seconds=15)
    mid = _create_test_monitor(
        mode=MonitorMode.KEEP_ALIVE.value,
        keep_alive_enabled=True,
        keep_alive_interval_seconds=300,
        keep_alive_path="/ping",
        next_keep_alive_at=past_time,
    )

    res = sweep_due_monitors()
    assert res["monitors_enqueued"] == 0
    assert res["keep_alives_enqueued"] == 1

    mock_ka_async.assert_called_once_with(args=[mid], expires=300)
    mock_ping_async.assert_not_called()

    with get_sync_db() as session:
        mon = session.get(Monitor, mid)
        assert mon.next_keep_alive_at is not None
        assert mon.next_keep_alive_at > now
        diff = (mon.next_keep_alive_at - now).total_seconds()
        assert abs(diff - 300.0) <= 5.0


@patch("app.tasks.execute_ping.apply_async")
@patch("app.tasks.execute_keep_alive.apply_async")
def test_sweep_dual_mode_both_due(mock_ka_async: MagicMock, mock_ping_async: MagicMock) -> None:
    """Verify monitor_and_keep_alive mode independently triggers both tasks when both are due."""
    now = datetime.now(UTC)
    past_time = now - timedelta(seconds=5)
    mid = _create_test_monitor(
        check_interval_seconds=45,
        mode=MonitorMode.MONITOR_AND_KEEP_ALIVE.value,
        next_check_at=past_time,
        keep_alive_enabled=True,
        keep_alive_interval_seconds=180,
        keep_alive_path="/health",
        next_keep_alive_at=past_time,
    )

    res = sweep_due_monitors()
    assert res["monitors_enqueued"] == 1
    assert res["keep_alives_enqueued"] == 1

    mock_ping_async.assert_called_once_with(args=[mid], expires=45)
    mock_ka_async.assert_called_once_with(args=[mid], expires=180)

    with get_sync_db() as session:
        mon = session.get(Monitor, mid)
        assert abs((mon.next_check_at - now).total_seconds() - 45.0) <= 5.0
        assert abs((mon.next_keep_alive_at - now).total_seconds() - 180.0) <= 5.0


@patch("app.tasks.execute_ping.apply_async")
@patch("app.tasks.execute_keep_alive.apply_async")
def test_sweep_dual_mode_independent_timing(
    mock_ka_async: MagicMock, mock_ping_async: MagicMock
) -> None:
    """Verify only the overdue schedule triggers when intervals differ."""
    now = datetime.now(UTC)
    past_time = now - timedelta(seconds=10)
    future_time = now + timedelta(seconds=200)

    mid = _create_test_monitor(
        check_interval_seconds=60,
        mode=MonitorMode.MONITOR_AND_KEEP_ALIVE.value,
        next_check_at=past_time,  # DUE
        keep_alive_enabled=True,
        keep_alive_interval_seconds=300,
        keep_alive_path="/health",
        next_keep_alive_at=future_time,  # NOT DUE
    )

    res = sweep_due_monitors()
    assert res["monitors_enqueued"] == 1
    assert res["keep_alives_enqueued"] == 0

    mock_ping_async.assert_called_once_with(args=[mid], expires=60)
    mock_ka_async.assert_not_called()

    with get_sync_db() as session:
        mon = session.get(Monitor, mid)
        assert abs((mon.next_check_at - now).total_seconds() - 60.0) <= 5.0
        assert mon.next_keep_alive_at == future_time


@patch("app.tasks.execute_ping.apply_async")
@patch("app.tasks.execute_keep_alive.apply_async")
def test_sweep_keep_alive_disabled_never_enqueued(
    mock_ka_async: MagicMock, mock_ping_async: MagicMock
) -> None:
    """
    Gatekeeper: If keep_alive_enabled=False, never enqueue keep-alive even if timestamp is past.
    """
    now = datetime.now(UTC)
    past_time = now - timedelta(seconds=50)

    _create_test_monitor(
        mode=MonitorMode.MONITOR.value,
        next_check_at=now + timedelta(seconds=600),  # Not due
        keep_alive_enabled=False,
        next_keep_alive_at=past_time,  # Past timestamp
    )

    res = sweep_due_monitors()
    assert res["monitors_enqueued"] == 0
    assert res["keep_alives_enqueued"] == 0
    mock_ka_async.assert_not_called()
    mock_ping_async.assert_not_called()


def test_sweep_skip_locked_concurrency() -> None:
    """
    Verify SELECT FOR UPDATE SKIP LOCKED avoids claiming rows locked by another transaction.
    """
    now = datetime.now(UTC)
    past_time = now - timedelta(seconds=20)
    mid = _create_test_monitor(
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
        assert len(locked_monitors) == 1

        # Transaction 2 (sweep_due_monitors) runs concurrently: must skip locked row
        with patch("app.tasks.execute_ping.apply_async") as mock_async:
            res = sweep_due_monitors()
            assert res["monitors_enqueued"] == 0
            mock_async.assert_not_called()

        # Transaction 1 releases lock
        session_lock.commit()
    finally:
        session_lock.close()

    # Now that lock is released, subsequent sweep must claim it
    with patch("app.tasks.execute_ping.apply_async") as mock_async:
        res = sweep_due_monitors()
        assert res["monitors_enqueued"] == 1
        mock_async.assert_called_once_with(args=[mid], expires=60)


@patch("app.tasks.execute_ping.apply_async")
def test_sweep_missed_schedules_no_catchup_storm(mock_async: MagicMock) -> None:
    """Anti-storm: A monitor 3 hours overdue enqueues ONE check and advances from current time."""
    now = datetime.now(UTC)
    three_hours_ago = now - timedelta(hours=3)
    mid = _create_test_monitor(
        check_interval_seconds=60,
        mode=MonitorMode.MONITOR.value,
        next_check_at=three_hours_ago,
    )

    res = sweep_due_monitors()
    assert res["monitors_enqueued"] == 1
    mock_async.assert_called_once_with(args=[mid], expires=60)

    with get_sync_db() as session:
        mon = session.get(Monitor, mid)
        # Advanced from 'now', NOT from three_hours_ago + 60s!
        assert mon.next_check_at > now
        diff = (mon.next_check_at - now).total_seconds()
        assert abs(diff - 60.0) <= 5.0


@patch(
    "app.tasks.execute_ping.apply_async",
    side_effect=redis.exceptions.ConnectionError("Redis connection lost"),
)
def test_sweep_redis_failure_recovers_schedule_to_now(mock_async: MagicMock) -> None:
    """
    Resilience (New Semantics):
    When task enqueueing fails (broker down), the error is logged and in a new short transaction
    next_check_at is reset to 'now' so the monitor is immediately retried on the next sweep
    instead of staying pushed into the future.
    """
    now = datetime.now(UTC)
    past_time = now - timedelta(seconds=30)
    mid = _create_test_monitor(
        check_interval_seconds=60,
        mode=MonitorMode.MONITOR.value,
        next_check_at=past_time,
    )

    # Sweep catches broker exception, logs error, and runs recovery transaction
    res = sweep_due_monitors()
    assert res["monitors_enqueued"] == 0

    with get_sync_db() as session:
        mon = session.get(Monitor, mid)
        diff = (mon.next_check_at - now).total_seconds()
        assert abs(diff - 0.0) <= 3.0


@patch("app.tasks.execute_ping.apply_async")
def test_sweep_batching_respects_batch_size(mock_async: MagicMock) -> None:
    """Batching: 5 due monitors with batch_size=2 are processed across multiple batches."""
    now = datetime.now(UTC)
    past_time = now - timedelta(seconds=10)
    for i in range(5):
        _create_test_monitor(name=f"Monitor {i}", next_check_at=past_time)

    res = sweep_due_monitors(batch_size=2)
    assert res["monitors_enqueued"] == 5
    assert mock_async.call_count == 5


@patch("app.tasks.execute_ping.apply_async")
def test_sweep_max_batches_bounds_sweep_runtime(mock_async: MagicMock) -> None:
    """
    Batch bounding: 6 due monitors with batch_size=2 and max_batches=2 processes only 4 monitors.
    """
    now = datetime.now(UTC)
    past_time = now - timedelta(seconds=10)
    for i in range(6):
        _create_test_monitor(name=f"Monitor {i}", next_check_at=past_time)

    # max_batches=2 with batch_size=2 -> claims at most 4 monitors
    res = sweep_due_monitors(batch_size=2, max_batches=2)
    assert res["monitors_enqueued"] == 4
    assert mock_async.call_count == 4

    # The remaining 2 monitors must still be overdue and ready for next sweep
    with get_sync_db() as session:
        remaining_due = session.query(Monitor).filter(Monitor.next_check_at <= now).count()
        assert remaining_due == 2


def test_sweep_idle_no_monitors_due() -> None:
    """Verify clean idle run when no monitors are due."""
    now = datetime.now(UTC)
    _create_test_monitor(
        mode=MonitorMode.MONITOR.value,
        next_check_at=now + timedelta(hours=1),
    )

    res = sweep_due_monitors()
    assert res["monitors_enqueued"] == 0
    assert res["keep_alives_enqueued"] == 0
