"""
Distributed Task Definitions — Chapter 3: Distributed Task Execution

Defines Celery background tasks:
- `execute_ping`: Synchronous HTTP health probe, latency recording, and monitor status updates.
- `execute_keep_alive`: Synchronous lightweight activity request intended to wake/touch idle services.

Architectural Boundaries:
-------------------------
1. Decoupled Execution: These tasks execute in independent Celery worker processes,
   completely separated from the FastAPI event loop.
2. Synchronous Database Access: Workers use isolated `get_sync_db()` sessions,
   never sharing or reusing FastAPI's `AsyncSession`.
3. Keep-Alive Reality: Keep-Alive attempts record transmission outcomes and response codes,
   but do NOT guarantee permanent provider-level uptime.
4. Security Note (Chapter 5 Handoff): Full SSRF mitigation (private IP filtering,
   cloud metadata protections, DNS rebinding guards) is scheduled for Chapter 5.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any
import requests
from requests.exceptions import ConnectionError as ReqConnectionError
from requests.exceptions import RequestException, Timeout
from sqlalchemy.orm import Session

from app.db import get_sync_db
from app.models import Monitor, PingResult
from app.schemas import MonitorStatus
from app.worker import celery_app

logger = logging.getLogger(__name__)


def safe_join_url(base_url: str, path: str | None) -> str:
    """
    Safely joins a base URL with an optional sub-path, avoiding duplicate slashes.
    
    Examples:
        safe_join_url('https://xyz.com/', '/health') -> 'https://xyz.com/health'
        safe_join_url('https://xyz.com', 'health')   -> 'https://xyz.com/health'
        safe_join_url('https://xyz.com/api', None)   -> 'https://xyz.com/api'
    """
    if not path or not path.strip():
        return base_url
    base_clean = base_url.rstrip("/")
    path_clean = "/" + path.strip().lstrip("/")
    return f"{base_clean}{path_clean}"


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
    
    Workflow:
    1. Loads Monitor from PostgreSQL by ID using synchronous session.
    2. Exits cleanly if monitor has been deleted.
    3. Issues HTTP GET with a bounded timeout (5.0s).
    4. Computes round-trip latency in milliseconds.
    5. Retries on transient network errors (Timeout, ConnectionError) with exponential backoff.
    6. Persists historical telemetry to 'ping_results' with check_type='monitor'.
    7. Updates Monitor.status ('up', 'degraded', 'down') and last_checked_at.
    """
    logger.info("Starting ping monitor_id=%s", monitor_id)

    with get_sync_db() as session:
        monitor = session.get(Monitor, monitor_id)
        if monitor is None:
            logger.warning("Monitor not found for execute_ping monitor_id=%s", monitor_id)
            return {"status": "not_found", "monitor_id": monitor_id}

        target_url = monitor.url
        now = datetime.now(timezone.utc)
        status_code: int | None = None
        latency_ms: float | None = None
        error_msg: str | None = None

        # Network Probe Execution (Bounded with explicit timeout)
        start_time = time.perf_counter()
        try:
            response = requests.get(
                target_url,
                timeout=5.0,
                headers={"User-Agent": "PingGuard/1.0", "Accept": "*/*"},
                allow_redirects=True,
            )
            elapsed = time.perf_counter() - start_time
            latency_ms = round(elapsed * 1000, 2)
            status_code = response.status_code

            # Map status code to simple health classification
            if 200 <= status_code < 400:
                monitor.status = MonitorStatus.UP.value
            elif 400 <= status_code < 500:
                monitor.status = MonitorStatus.DEGRADED.value
            else:
                monitor.status = MonitorStatus.DOWN.value

            logger.info(
                "Ping succeeded monitor_id=%s status=%s latency_ms=%s",
                monitor_id,
                status_code,
                latency_ms,
            )

        except (Timeout, ReqConnectionError) as exc:
            elapsed = time.perf_counter() - start_time
            latency_ms = round(elapsed * 1000, 2)
            error_type = "timeout" if isinstance(exc, Timeout) else "connection_error"
            error_msg = f"{error_type}: {str(exc)[:200]}"
            monitor.status = MonitorStatus.DOWN.value

            # Bounded retry with exponential backoff for transient failures
            if self.request.retries < self.max_retries:
                countdown = 2 ** self.request.retries
                logger.warning(
                    "Transient error on monitor_id=%s (%s). Retrying in %ss (attempt %s/%s)...",
                    monitor_id,
                    error_type,
                    countdown,
                    self.request.retries + 1,
                    self.max_retries,
                )
                raise self.retry(exc=exc, countdown=countdown)

            logger.error("Ping failed permanently monitor_id=%s error=%s", monitor_id, error_type)

        except RequestException as exc:
            elapsed = time.perf_counter() - start_time
            latency_ms = round(elapsed * 1000, 2)
            error_msg = f"request_error: {str(exc)[:200]}"
            monitor.status = MonitorStatus.DOWN.value
            logger.error("Ping failed monitor_id=%s error=%s", monitor_id, error_msg)

        # Update monitor telemetry timestamp
        monitor.last_checked_at = now

        # Persist PingResult
        save_ping_result(
            session=session,
            monitor_id=monitor_id,
            check_type="monitor",
            status_code=status_code,
            latency_ms=latency_ms,
            error=error_msg,
            checked_at=now,
        )

        return {
            "status": "completed",
            "monitor_id": monitor_id,
            "check_type": "monitor",
            "status_code": status_code,
            "latency_ms": latency_ms,
            "error": error_msg,
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
    
    Important Semantic Note:
    -----------------------
    Keep-Alive is an activity attempt to touch services prone to spinning down on idle.
    Success indicates the HTTP request was transmitted and received by the target endpoint.
    It does NOT provide a guarantee of permanent uptime from the target hosting platform.
    
    Workflow:
    1. Loads Monitor from PostgreSQL.
    2. Exits cleanly if monitor does not exist.
    3. Gatekeeper: If keep_alive_enabled is False, cleanly skips without sending requests.
    4. Validates target endpoint path configuration.
    5. Sends lightweight HTTP GET request to `url + keep_alive_path` with 5.0s timeout.
    6. Persists result to 'ping_results' with check_type='keep_alive'.
    7. Does NOT alter Monitor.status (uptime status belongs strictly to health checks).
    """
    logger.info("Starting keep_alive monitor_id=%s", monitor_id)

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

        # Construct safe destination endpoint
        target_url = safe_join_url(monitor.url, monitor.keep_alive_path)
        now = datetime.now(timezone.utc)
        status_code: int | None = None
        latency_ms: float | None = None
        error_msg: str | None = None

        start_time = time.perf_counter()
        try:
            response = requests.get(
                target_url,
                timeout=5.0,
                headers={"User-Agent": "PingGuard-KeepAlive/1.0", "Accept": "*/*"},
                allow_redirects=True,
            )
            elapsed = time.perf_counter() - start_time
            latency_ms = round(elapsed * 1000, 2)
            status_code = response.status_code
            logger.info(
                "Keep-alive completed monitor_id=%s status=%s latency_ms=%s",
                monitor_id,
                status_code,
                latency_ms,
            )

        except (Timeout, ReqConnectionError) as exc:
            elapsed = time.perf_counter() - start_time
            latency_ms = round(elapsed * 1000, 2)
            error_type = "timeout" if isinstance(exc, Timeout) else "connection_error"
            error_msg = f"{error_type}: {str(exc)[:200]}"

            if self.request.retries < self.max_retries:
                countdown = 2 ** self.request.retries
                logger.warning(
                    "Transient error in keep-alive for monitor_id=%s. Retrying in %ss...",
                    monitor_id,
                    countdown,
                )
                raise self.retry(exc=exc, countdown=countdown)

            logger.error("Keep-alive failed permanently monitor_id=%s error=%s", monitor_id, error_type)

        except RequestException as exc:
            elapsed = time.perf_counter() - start_time
            latency_ms = round(elapsed * 1000, 2)
            error_msg = f"request_error: {str(exc)[:200]}"
            logger.error("Keep-alive failed monitor_id=%s error=%s", monitor_id, error_msg)

        # Persist Keep-Alive telemetry record
        save_ping_result(
            session=session,
            monitor_id=monitor_id,
            check_type="keep_alive",
            status_code=status_code,
            latency_ms=latency_ms,
            error=error_msg,
            checked_at=now,
        )

        return {
            "status": "completed",
            "monitor_id": monitor_id,
            "check_type": "keep_alive",
            "status_code": status_code,
            "latency_ms": latency_ms,
            "error": error_msg,
        }
