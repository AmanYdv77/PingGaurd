"""
Celery Application & Worker Configuration.

Configures the Celery distributed task queue backed by Redis:
- JSON-only serialization for safe message passing.
- Late acknowledgements (task_acks_late = True) to ensure tasks are not lost if workers crash.
- Prefetch multiplier = 1 to prevent head-of-line blocking on heterogeneous network latencies.
- Bounded soft and hard task time limits.
"""

from celery import Celery
from celery.schedules import crontab
from celery.signals import setup_logging
from app.config import get_settings
from app.logging_config import configure_logging

# Centralised application settings
settings = get_settings()


@setup_logging.connect
def on_setup_logging(**kwargs):
    """Align Celery worker logging with PingGuard structured JSON logging format."""
    configure_logging(level=settings.log_level, json_logs=settings.log_json)
REDIS_BROKER_URL = settings.redis_broker_url
REDIS_RESULT_BACKEND_URL = settings.redis_result_backend_url

# Instantiate Celery application
celery_app = Celery(
    "pingguard",
    broker=REDIS_BROKER_URL,
    backend=REDIS_RESULT_BACKEND_URL,
    include=["app.tasks"],
)

# Apply architectural configuration defaults
celery_app.conf.update(
    # Strict JSON serialization (prevents unsafe pickle exploitation)
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    
    # Timezone alignment
    timezone="UTC",
    enable_utc=True,
    
    # Task reliability: acknowledge only after execution finishes
    task_acks_late=True,
    
    # Prevent worker head-of-line blocking: reserve exactly 1 task at a time
    worker_prefetch_multiplier=1,
    
    # Task execution timeouts (seconds)
    task_soft_time_limit=settings.celery_soft_time_limit,
    task_time_limit=settings.celery_hard_time_limit,
    
    # Prevent Redis result backend unbounded memory accumulation
    result_expires=3600,
    
    # Explicit task tracking
    task_track_started=True,

    # Celery Beat Periodic Scheduling Heartbeat
    # Sweep task queries PostgreSQL for due work.
    # Prune task cleans up expired ping_result rows daily at 03:00 UTC.
    beat_schedule={
        "sweep-due-monitors": {
            "task": "app.tasks.sweep_due_monitors",
            "schedule": settings.sweep_interval_seconds,
        },
        "prune-ping-results": {
            "task": "app.tasks.prune_ping_results",
            "schedule": crontab(hour=3, minute=0),
        },
    },
    beat_schedule_filename=settings.celerybeat_schedule_filename,
)
