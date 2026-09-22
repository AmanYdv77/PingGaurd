"""
Distributed Task Definitions.

Defines Celery background tasks:
- `execute_ping`: HTTP health probe, latency recording, and monitor status updates.
- `execute_keep_alive`: Lightweight activity request intended to wake/touch idle services.
- `sweep_due_monitors`: Periodic scheduler sweep dispatching due health and keep-alive checks.
"""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.db import get_sync_db

from app.models import Monitor, PingResult
from app.net import (
    PingOutcome,
    PingResultDTO,
    robust_keep_alive,
    robust_ping,
)
from celery.exceptions import SoftTimeLimitExceeded
from app.config import get_settings
from app.schemas import MonitorMode, MonitorStatus
from app.status import outcome_to_status
from app.urls import safe_join_url
from app.worker import celery_app

logger = logging.getLogger(__name__)



def save_ping_result(
    session: Session,
    monitor_id: int,
    check_type: str,
    status_code: int | None,
    latency_ms: float | None,
    error: str | None,
    checked_at: datetime,
) -> PingResult:
    """Reusable persistence helper to create a PingResult row."""
    result = PingResult(
        monitor_id=monitor_id,
        check_type=check_type,
        status_code=status_code,
        latency_ms=latency_ms,
        error=error,
        checked_at=checked_at,
    )
    session.add(result)
    return result


@celery_app.task(
    bind=True,
    name="app.tasks.execute_ping",
    max_retries=3,
    default_retry_delay=2,
)
def execute_ping(self, monitor_id: int) -> dict[str, Any]:
    """
    Executes an uptime health probe for the specified monitor.
    
    - Uses shared `robust_ping` powered by httpx.AsyncClient.
    - Enforces SSRF defense, DNS rebinding checks, and redirect interception.
    - Classifies outcomes into UP, DEGRADED, DOWN, UNREACHABLE.
    - Retries only transient network timeouts/errors with exponential backoff.
    - Never retries SSRF-blocked destinations or TLS configuration errors.
    - Persists historical telemetry to 'ping_results' with check_type='monitor'.
    - Updates Monitor.status ('up', 'degraded', 'down') and last_checked_at.
    """
    logger.info("Starting ping monitor_id=%s", monitor_id)

    try:
        with get_sync_db() as session:
            monitor = session.get(Monitor, monitor_id)
            if monitor is None:
                logger.warning("Monitor not found for execute_ping monitor_id=%s", monitor_id)
                return {"status": "not_found", "monitor_id": monitor_id}

            now = datetime.now(timezone.utc)

            # Execute network probe via shared network engine
            dto = robust_ping(monitor.url)

            # Map PingOutcome to Monitor.status via centralised mapping
            monitor.status = outcome_to_status(dto.outcome).value

            # Update monitor telemetry timestamp
            monitor.last_checked_at = now

            # Persist PingResult record
            save_ping_result(
                session=session,
                monitor_id=monitor_id,
                check_type="monitor",
                status_code=dto.status_code,
                latency_ms=dto.latency_ms,
                error=dto.error_detail,
                checked_at=now,
            )

            logger.info(
                "Ping completed monitor_id=%s outcome=%s status_code=%s latency_ms=%s error=%s",
                monitor_id,
                dto.outcome.value,
                dto.status_code,
                dto.latency_ms,
                dto.error_detail,
            )

            # Retry transient network failures only (timeouts, connection drops)
            # Strictly DO NOT retry ssrf_blocked, redirect_error, or tls_error
            TRANSIENT_ERRORS = (
                "connect_timeout",
                "read_timeout",
                "write_timeout",
                "pool_timeout",
                "total_timeout",
                "connect_error",
            )
            if dto.error_detail in TRANSIENT_ERRORS and self.request.retries < self.max_retries:
                countdown = 2 ** self.request.retries
                logger.warning(
                    "Transient network error on monitor_id=%s (%s). Retrying in %ss (attempt %s/%s)...",
                    monitor_id,
                    dto.error_detail,
                    countdown,
                    self.request.retries + 1,
                    self.max_retries,
                )
                raise self.retry(exc=Exception(dto.error_detail), countdown=countdown)

            return {
                "status": "completed",
                "monitor_id": monitor_id,
                "check_type": "monitor",
                "outcome": dto.outcome.value,
                "status_code": dto.status_code,
                "latency_ms": dto.latency_ms,
                "error": dto.error_detail,
                "final_url": dto.final_url,
            }

    except SoftTimeLimitExceeded:
        logger.error("Soft time limit exceeded in execute_ping for monitor_id=%s", monitor_id)
        now = datetime.now(timezone.utc)
        with get_sync_db() as recovery_session:
            mon = recovery_session.get(Monitor, monitor_id)
            if mon:
                mon.status = outcome_to_status(PingOutcome.DOWN).value
                mon.last_checked_at = now
            save_ping_result(
                session=recovery_session,
                monitor_id=monitor_id,
                check_type="monitor",
                status_code=None,
                latency_ms=None,
                error="task_soft_time_limit",
                checked_at=now,
            )
        return {
            "status": "completed",
            "monitor_id": monitor_id,
            "check_type": "monitor",
            "outcome": PingOutcome.DOWN.value,
            "status_code": None,
            "latency_ms": None,
            "error": "task_soft_time_limit",
            "final_url": None,
        }


@celery_app.task(
    bind=True,
    name="app.tasks.execute_keep_alive",
    max_retries=3,
    default_retry_delay=2,
)
def execute_keep_alive(self, monitor_id: int) -> dict[str, Any]:
    """
    Executes an optional lightweight Keep-Alive activity ping for the specified monitor.
    
    - Uses shared `robust_keep_alive` powered by httpx.AsyncClient.
    - Sends activity request to `url + keep_alive_path`.
    - Persists result to 'ping_results' with check_type='keep_alive'.
    - Strictly preserves Monitor.status unchanged (health status belongs only to execute_ping).
    - Retries only transient network errors; never retries SSRF rejections.
    """
    logger.info("Starting keep_alive monitor_id=%s", monitor_id)

    try:
        with get_sync_db() as session:
            monitor = session.get(Monitor, monitor_id)
            if monitor is None:
                logger.warning("Monitor not found for execute_keep_alive monitor_id=%s", monitor_id)
                return {"status": "not_found", "monitor_id": monitor_id}

            # Gatekeeper: ensure keep-alive is currently enabled
            if not monitor.keep_alive_enabled:
                logger.info("Keep-alive skipped monitor_id=%s reason=disabled", monitor_id)
                return {"status": "skipped", "reason": "keep_alive_disabled", "monitor_id": monitor_id}

            # Validate URL presence
            if not monitor.url:
                logger.error("Keep-alive failed monitor_id=%s reason=missing_url", monitor_id)
                return {"status": "error", "reason": "missing_url", "monitor_id": monitor_id}

            now = datetime.now(timezone.utc)

            # Execute Keep-Alive activity probe via shared network engine
            dto = robust_keep_alive(monitor.url, monitor.keep_alive_path)

            # Persist Keep-Alive telemetry record
            save_ping_result(
                session=session,
                monitor_id=monitor_id,
                check_type="keep_alive",
                status_code=dto.status_code,
                latency_ms=dto.latency_ms,
                error=dto.error_detail,
                checked_at=now,
            )

            logger.info(
                "Keep-alive completed monitor_id=%s outcome=%s status_code=%s latency_ms=%s error=%s",
                monitor_id,
                dto.outcome.value,
                dto.status_code,
                dto.latency_ms,
                dto.error_detail,
            )

            # Retry transient network failures only
            TRANSIENT_ERRORS = (
                "connect_timeout",
                "read_timeout",
                "write_timeout",
                "pool_timeout",
                "total_timeout",
                "connect_error",
            )
            if dto.error_detail in TRANSIENT_ERRORS and self.request.retries < self.max_retries:
                countdown = 2 ** self.request.retries
                logger.warning(
                    "Transient network error in keep-alive for monitor_id=%s (%s). Retrying in %ss (attempt %s/%s)...",
                    monitor_id,
                    dto.error_detail,
                    countdown,
                    self.request.retries + 1,
                    self.max_retries,
                )
                raise self.retry(exc=Exception(dto.error_detail), countdown=countdown)

            return {
                "status": "completed",
                "monitor_id": monitor_id,
                "check_type": "keep_alive",
                "outcome": dto.outcome.value,
                "status_code": dto.status_code,
                "latency_ms": dto.latency_ms,
                "error": dto.error_detail,
                "final_url": dto.final_url,
            }

    except SoftTimeLimitExceeded:
        logger.error("Soft time limit exceeded in execute_keep_alive for monitor_id=%s", monitor_id)
        now = datetime.now(timezone.utc)
        with get_sync_db() as recovery_session:
            mon = recovery_session.get(Monitor, monitor_id)
            if mon:
                mon.status = outcome_to_status(PingOutcome.DOWN).value
                mon.last_checked_at = now
            save_ping_result(
                session=recovery_session,
                monitor_id=monitor_id,
                check_type="keep_alive",
                status_code=None,
                latency_ms=None,
                error="task_soft_time_limit",
                checked_at=now,
            )
        return {
            "status": "completed",
            "monitor_id": monitor_id,
            "check_type": "keep_alive",
            "outcome": PingOutcome.DOWN.value,
            "status_code": None,
            "latency_ms": None,
            "error": "task_soft_time_limit",
            "final_url": None,
        }


# =========================================================================
# Celery Beat Periodic Scheduling Heartbeat
# =========================================================================

@celery_app.task(
    bind=True,
    name="app.tasks.sweep_due_monitors",
    ignore_result=True,
)
def sweep_due_monitors(self) -> dict[str, int]:
    """
    Celery Beat Scheduling Heartbeat.
    
    Decides WHEN health probes and keep-alive activities are due and enqueues them.
    Adheres strictly to the PingGuard architectural separation:
    - Beat/Sweep decides WHEN to check.
    - Celery Workers decide HOW to check.
    - Zero outbound HTTP requests are performed within this task.
    - Concurrency-safe claiming via SELECT ... FOR UPDATE SKIP LOCKED.
    - Missed schedules advance from `now` to prevent catch-up storms.
    - Two independent schedules: monitoring (`next_check_at`) and keep-alive (`next_keep_alive_at`).
    """
    now = datetime.now(timezone.utc)
    logger.info("Scheduler sweep started at %s", now.isoformat())

    monitors_enqueued = 0
    keep_alives_enqueued = 0

    with get_sync_db() as session:
        # ---------------------------------------------------------------------
        # Health Monitoring Due Sweep
        # ---------------------------------------------------------------------
        monitoring_modes = [MonitorMode.MONITOR.value, MonitorMode.MONITOR_AND_KEEP_ALIVE.value]
        due_monitors = (
            session.query(Monitor)
            .filter(
                Monitor.mode.in_(monitoring_modes),
                Monitor.next_check_at <= now,
            )
            .with_for_update(skip_locked=True)
            .all()
        )

        for monitor in due_monitors:
            logger.info("Monitor %s due for health check (next_check_at=%s)", monitor.id, monitor.next_check_at)
            try:
                execute_ping.delay(monitor.id)
                monitors_enqueued += 1
                monitor.next_check_at = now + timedelta(seconds=monitor.check_interval_seconds)
                logger.info(
                    "Enqueued execute_ping monitor_id=%s, advanced next_check_at to %s",
                    monitor.id,
                    monitor.next_check_at,
                )
            except Exception as exc:
                logger.error(
                    "Failed to enqueue execute_ping monitor_id=%s: %s. Rolling back transaction.",
                    monitor.id,
                    exc,
                )
                raise

        # ---------------------------------------------------------------------
        # Keep-Alive Activity Due Sweep
        # ---------------------------------------------------------------------
        keep_alive_modes = [MonitorMode.KEEP_ALIVE.value, MonitorMode.MONITOR_AND_KEEP_ALIVE.value]
        due_keep_alives = (
            session.query(Monitor)
            .filter(
                Monitor.keep_alive_enabled == True,
                Monitor.mode.in_(keep_alive_modes),
                Monitor.next_keep_alive_at <= now,
            )
            .with_for_update(skip_locked=True)
            .all()
        )

        for monitor in due_keep_alives:
            if not monitor.keep_alive_enabled or not monitor.keep_alive_interval_seconds:
                logger.warning(
                    "Keep-alive skipped monitor_id=%s reason=invalid_or_disabled_configuration",
                    monitor.id,
                )
                continue

            logger.info("Monitor %s due for keep_alive (next_keep_alive_at=%s)", monitor.id, monitor.next_keep_alive_at)
            try:
                execute_keep_alive.delay(monitor.id)
                keep_alives_enqueued += 1
                monitor.next_keep_alive_at = now + timedelta(seconds=monitor.keep_alive_interval_seconds)
                logger.info(
                    "Enqueued execute_keep_alive monitor_id=%s, advanced next_keep_alive_at to %s",
                    monitor.id,
                    monitor.next_keep_alive_at,
                )
            except Exception as exc:
                logger.error(
                    "Failed to enqueue execute_keep_alive monitor_id=%s: %s. Rolling back transaction.",
                    monitor.id,
                    exc,
                )
                raise

    logger.info(
        "Scheduler sweep completed. Enqueued %d health pings, %d keep-alive requests.",
        monitors_enqueued,
        keep_alives_enqueued,
    )
    return {
        "monitors_enqueued": monitors_enqueued,
        "keep_alives_enqueued": keep_alives_enqueued,
    }


@celery_app.task(name="app.tasks.prune_ping_results", ignore_result=True, acks_late=True)
def prune_ping_results(batch_size: int | None = None) -> int:
    """
    Prunes ping_results older than ping_results_retention_days in small batches.
    Prevents unbounded table growth while avoiding long database table locks.
    """
    settings = get_settings()
    retention_days = settings.ping_results_retention_days

    batch_limit = batch_size if batch_size is not None else settings.retention_batch_size

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    total_deleted = 0

    with get_sync_db() as session:
        while True:
            subq = (
                select(PingResult.id)
                .where(PingResult.checked_at < cutoff)
                .order_by(PingResult.id)
                .limit(batch_limit)
                .scalar_subquery()
            )
            stmt = delete(PingResult).where(PingResult.id.in_(subq))
            result = session.execute(stmt)
            session.commit()

            deleted = result.rowcount
            total_deleted += deleted
            if deleted == 0:
                break

    logger.info("Pruned %d expired ping_result rows older than %s", total_deleted, cutoff.isoformat())
    return total_deleted

