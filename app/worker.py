"""
Celery Application & Worker Configuration — Chapter 3: Distributed Task Execution

Configures the Celery distributed task queue backed by Redis:
- JSON-only serialization for safe message passing.
- Late acknowledgements (task_acks_late = True) to ensure tasks are not lost if workers crash.
- Prefetch multiplier = 1 to prevent head-of-line blocking on heterogeneous network latencies.
- Bounded soft and hard task time limits.
"""

import os
from pathlib import Path
from celery import Celery
from dotenv import load_dotenv

# Load environment variables from .env if present
env_file = Path(__file__).resolve().parent.parent / ".env"
if env_file.exists():
    load_dotenv(dotenv_path=env_file)

# Configurable Redis endpoints
DEFAULT_BROKER = "redis://localhost:6379/0"
DEFAULT_BACKEND = "redis://localhost:6379/1"

REDIS_BROKER_URL = os.getenv("REDIS_BROKER_URL", DEFAULT_BROKER)
REDIS_RESULT_BACKEND_URL = os.getenv("REDIS_RESULT_BACKEND_URL", DEFAULT_BACKEND)

# Instantiate Celery application
celery_app = Celery(
    "pingguard",
    broker=REDIS_BROKER_URL,
    backend=REDIS_RESULT_BACKEND_URL,
    include=["app.tasks"],
)

# Apply production-grade architectural defaults from blueprint
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
    task_soft_time_limit=10,
    task_time_limit=15,
    
    # Prevent Redis result backend unbounded memory accumulation
    result_expires=3600,
    
    # Explicit task tracking
    task_track_started=True,
)

# Eagerly import task definitions to ensure immediate registration
import app.tasks  # noqa: F401, E402
