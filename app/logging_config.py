"""
Structured logging configuration with correlated request IDs for PingGuard.

Provides JSON and standard logging formatters using standard-library modules only,
with asynchronous request ID propagation via contextvars.
"""

import contextvars
import json
import logging
import logging.config
import re
from datetime import UTC, datetime
from typing import Any

# Asynchronous request context variable for log correlation
request_id_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "request_id", default=None
)

# Valid Request ID regex: 8 to 64 alphanumeric, dash, or underscore characters
REQUEST_ID_REGEX = re.compile(r"^[A-Za-z0-9_-]{8,64}$")


class RequestIdFilter(logging.Filter):
    """Logging filter that injects the current request_id into every LogRecord."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get()
        return True


class JSONLogFormatter(logging.Formatter):
    """
    Standard-library JSON log formatter emitting structured log events.

    Outputs keys: timestamp (UTC ISO 8601), level, logger, message, request_id,
    and optional exception tracebacks when present.
    """

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "request_id": getattr(record, "request_id", None),
        }

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


def configure_logging(
    level: str = "INFO",
    json_logs: bool = True,
    *,
    json: bool | None = None,
) -> None:
    """
    Configure application logging using logging.config.dictConfig.

    Parameters:
    - level: Logging severity level (e.g. 'DEBUG', 'INFO', 'WARNING', 'ERROR').
    - json_logs: If True, format output as structured JSON. If False, use plain text.
    - json: Keyword alias for json_logs.
    """
    if json is not None:
        json_logs = json

    target_level = level.upper()

    config = {
        "version": 1,
        "disable_existing_loggers": False,
        "filters": {
            "request_id_filter": {
                "()": "app.logging_config.RequestIdFilter",
            },
        },
        "formatters": {
            "json": {
                "()": "app.logging_config.JSONLogFormatter",
            },
            "standard": {
                "format": "%(asctime)s [%(levelname)s] [%(request_id)s] %(name)s: %(message)s",
            },
        },
        "handlers": {
            "default": {
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "formatter": "json" if json_logs else "standard",
                "filters": ["request_id_filter"],
            },
        },
        "root": {
            "level": target_level,
            "handlers": ["default"],
        },
        "loggers": {
            "uvicorn": {
                "handlers": ["default"],
                "level": target_level,
                "propagate": False,
            },
            "uvicorn.error": {
                "handlers": ["default"],
                "level": target_level,
                "propagate": False,
            },
            "uvicorn.access": {
                "handlers": ["default"],
                "level": target_level,
                "propagate": False,
            },
        },
    }

    logging.config.dictConfig(config)
