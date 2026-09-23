"""
Automated Test Suite for Worker Tasks and Network Probing Integration.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from app.db import get_sync_db
from app.enums import MonitorStatus, PingOutcome
from app.models import Monitor, PingResult
from app.net import PingResultDTO
from app.tasks import execute_keep_alive, execute_ping
from app.urls import safe_join_url
from app.worker import celery_app


def _create_test_monitor(
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
            next_check_at=datetime.now(UTC),
            next_keep_alive_at=datetime.now(UTC) if keep_alive_enabled else None,
        )
        session.add(monitor)
        session.flush()
        session.refresh(monitor)
        return monitor.id


def test_safe_join_url_variations() -> None:
    """Verify safe URL joining avoids double slashes across all path patterns."""
    assert safe_join_url("https://xyz.com", "/health") == "https://xyz.com/health"
    assert safe_join_url("https://xyz.com/", "/health") == "https://xyz.com/health"
    assert safe_join_url("https://xyz.com", "health") == "https://xyz.com/health"
    assert safe_join_url("https://xyz.com/api/", "/v1/ping") == "https://xyz.com/api/v1/ping"
    assert safe_join_url("https://xyz.com", None) == "https://xyz.com"
    assert safe_join_url("https://xyz.com", "  ") == "https://xyz.com"


@patch("app.tasks.robust_ping")
def test_execute_ping_success(mock_ping: MagicMock) -> None:
    """
    Verify execute_ping performs probe via robust_ping,
    records PingResult with check_type='monitor',
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

    mid = _create_test_monitor(url="https://healthy-target.com")

    result = execute_ping(mid)
    assert result["status"] == "completed"
    assert result["monitor_id"] == mid
    assert result["check_type"] == "monitor"
    assert result["status_code"] == 200
    assert result["error"] is None
    assert result["latency_ms"] > 0

    # Verify DB state
    with get_sync_db() as session:
        mon = session.get(Monitor, mid)
        assert mon is not None
        assert mon.status == MonitorStatus.UP.value
        assert mon.last_checked_at is not None

        pr = session.query(PingResult).filter_by(monitor_id=mid).first()
        assert pr is not None
        assert pr.check_type == "monitor"
        assert pr.status_code == 200
        assert pr.error is None


@patch("app.tasks.robust_ping")
def test_execute_ping_status_mappings(mock_ping: MagicMock) -> None:
    """Verify HTTP 404 maps to DEGRADED and HTTP 500 maps to DOWN."""
    mid = _create_test_monitor(url="https://status-target.com")

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
        assert session.get(Monitor, mid).status == MonitorStatus.DEGRADED.value

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
        assert session.get(Monitor, mid).status == MonitorStatus.DOWN.value


def test_execute_ping_nonexistent_monitor() -> None:
    """Verify execute_ping handles non-existent monitor cleanly without raising exception."""
    result = execute_ping(99999)
    assert result["status"] == "not_found"
    assert result["monitor_id"] == 99999


@patch("app.tasks.robust_keep_alive")
def test_execute_keep_alive_success(mock_ka: MagicMock) -> None:
    """
    Verify execute_keep_alive contacts `url + keep_alive_path`,
    writes PingResult(check_type='keep_alive'),
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

    mid = _create_test_monitor(
        url="https://service-target.org",
        mode="monitor_and_keep_alive",
        keep_alive_enabled=True,
        keep_alive_path="/healthcheck",
    )

    result = execute_keep_alive(mid)
    assert result["status"] == "completed"
    assert result["check_type"] == "keep_alive"
    assert result["status_code"] == 200

    mock_ka.assert_called_once_with("https://service-target.org", "/healthcheck")

    # Verify PingResult recorded with check_type='keep_alive'
    with get_sync_db() as session:
        mon = session.get(Monitor, mid)
        assert mon.status == MonitorStatus.PENDING.value  # Untouched by keep-alive

        pr = session.query(PingResult).filter_by(monitor_id=mid, check_type="keep_alive").first()
        assert pr is not None
        assert pr.status_code == 200


@patch("app.tasks.robust_keep_alive")
def test_execute_keep_alive_skipped_when_disabled(mock_ka: MagicMock) -> None:
    """
    Critical Rule: If keep_alive_enabled=False, execute_keep_alive MUST NOT make any HTTP
    request and MUST NOT persist a success PingResult.
    """
    mid = _create_test_monitor(
        url="https://disabled-keepalive.com",
        mode="monitor",
        keep_alive_enabled=False,
    )

    result = execute_keep_alive(mid)
    assert result["status"] == "skipped"
    assert result["reason"] == "keep_alive_disabled"

    mock_ka.assert_not_called()

    with get_sync_db() as session:
        pr_count = session.query(PingResult).filter_by(monitor_id=mid).count()
        assert pr_count == 0


def test_execute_keep_alive_nonexistent_monitor() -> None:
    """Verify execute_keep_alive handles non-existent monitor cleanly."""
    result = execute_keep_alive(99999)
    assert result["status"] == "not_found"
    assert result["monitor_id"] == 99999


@patch("app.tasks.robust_ping")
def test_execute_ping_timeout_permanent_failure(mock_ping: MagicMock) -> None:
    """Verify exhausted retries persist error='read_timeout' and set Monitor.status='down'."""
    mock_ping.return_value = PingResultDTO(
        outcome=PingOutcome.UNREACHABLE,
        status_code=None,
        latency_ms=5000.0,
        error_detail="read_timeout",
        original_url="https://timeout-target.com",
        final_url=None,
    )

    mid = _create_test_monitor(url="https://timeout-target.com")

    # Set task.request.retries = 3 via Celery push_request
    execute_ping.push_request(retries=3)
    try:
        result = execute_ping(mid)
    finally:
        execute_ping.pop_request()

    assert result["status"] == "completed"
    assert "timeout" in result["error"]
    assert result["status_code"] is None

    with get_sync_db() as session:
        mon = session.get(Monitor, mid)
        assert mon.status == MonitorStatus.DOWN.value

        pr = session.query(PingResult).filter_by(monitor_id=mid).first()
        assert pr is not None
        assert "timeout" in pr.error


@patch("app.tasks.robust_keep_alive")
@patch("app.tasks.robust_ping")
def test_task_idempotency_multiple_executions(mock_ping: MagicMock, mock_ka: MagicMock) -> None:
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

    mid = _create_test_monitor(
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
        assert mon.status == MonitorStatus.UP.value

        results = session.query(PingResult).filter_by(monitor_id=mid).all()
        assert len(results) == 4
        monitor_results = [r for r in results if r.check_type == "monitor"]
        keep_alive_results = [r for r in results if r.check_type == "keep_alive"]
        assert len(monitor_results) == 2
        assert len(keep_alive_results) == 2


@patch("app.tasks.robust_keep_alive")
@patch("app.tasks.robust_ping")
def test_celery_delay_eager_execution(mock_ping: MagicMock, mock_ka: MagicMock) -> None:
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

    mid = _create_test_monitor(
        url="https://celery-eager.com",
        mode="monitor_and_keep_alive",
        keep_alive_enabled=True,
        keep_alive_path="/health",
    )

    celery_app.conf.task_always_eager = True
    try:
        async_ping = execute_ping.delay(mid)
        assert async_ping.ready()
        ping_res = async_ping.get()
        assert ping_res["status"] == "completed"
        assert ping_res["check_type"] == "monitor"

        async_ka = execute_keep_alive.delay(mid)
        assert async_ka.ready()
        ka_res = async_ka.get()
        assert ka_res["status"] == "completed"
        assert ka_res["check_type"] == "keep_alive"
    finally:
        celery_app.conf.task_always_eager = False


@patch("app.tasks.robust_ping")
def test_execute_ping_soft_time_limit_exceeded(mock_ping: MagicMock) -> None:
    """
    Verify SoftTimeLimitExceeded writes PingResult(outcome=DOWN, error='task_soft_time_limit')
    and updates status='down'.
    """
    from celery.exceptions import SoftTimeLimitExceeded

    mock_ping.side_effect = SoftTimeLimitExceeded("Task soft time limit exceeded")
    mid = _create_test_monitor(url="https://soft-limit-ping.com")

    result = execute_ping(mid)
    assert result["status"] == "completed"
    assert result["outcome"] == PingOutcome.DOWN.value
    assert result["error"] == "task_soft_time_limit"

    with get_sync_db() as session:
        mon = session.get(Monitor, mid)
        assert mon.status == MonitorStatus.DOWN.value
        assert mon.last_checked_at is not None

        pr = session.query(PingResult).filter_by(monitor_id=mid).first()
        assert pr is not None
        assert pr.check_type == "monitor"
        assert pr.error == "task_soft_time_limit"


@patch("app.tasks.robust_keep_alive")
def test_execute_keep_alive_soft_time_limit_exceeded(mock_ka: MagicMock) -> None:
    """Verify SoftTimeLimitExceeded in keep_alive writes PingResult and sets status='down'."""
    from celery.exceptions import SoftTimeLimitExceeded

    mock_ka.side_effect = SoftTimeLimitExceeded("Task soft time limit exceeded")
    mid = _create_test_monitor(
        url="https://soft-limit-ka.com",
        mode="monitor_and_keep_alive",
        keep_alive_enabled=True,
        keep_alive_path="/health",
    )

    result = execute_keep_alive(mid)
    assert result["status"] == "completed"
    assert result["outcome"] == PingOutcome.DOWN.value
    assert result["error"] == "task_soft_time_limit"

    with get_sync_db() as session:
        mon = session.get(Monitor, mid)
        assert mon.status == MonitorStatus.DOWN.value

        pr = session.query(PingResult).filter_by(monitor_id=mid, check_type="keep_alive").first()
        assert pr is not None
        assert pr.error == "task_soft_time_limit"
