"""
PingGuard Schemas — Chapter 1: The Request Layer

Defines the core Pydantic v2 data contracts for incoming requests and outgoing responses.
Adheres strictly to the PingGuard Technical Architecture Blueprint specification.
"""

from datetime import datetime
from enum import Enum
from pydantic import BaseModel, ConfigDict, Field, HttpUrl


class MonitorStatus(str, Enum):
    """
    Lifecycle status of a target monitor.
    
    - PENDING: Monitor is newly registered and awaiting its first scheduled probe.
    - UP: Endpoint responded successfully within latency and status criteria.
    - DEGRADED: Endpoint responded with client/edge error (e.g. 4xx) or high latency.
    - DOWN: Endpoint returned server error (5xx) or probe failed across retries.
    """
    UP = "up"
    DEGRADED = "degraded"
    DOWN = "down"
    PENDING = "pending"


class MonitorCreate(BaseModel):
    """
    Request contract for creating a new monitored endpoint.
    
    Validated strictly at the API perimeter before any persistence or scheduling.
    """
    name: str = Field(
        ...,
        min_length=1,
        max_length=120,
        description="Human-readable label or service name for the monitor.",
        examples=["Customer Portal API", "Primary Marketing Website"]
    )
    url: HttpUrl = Field(
        ...,
        description=(
            "Target HTTP or HTTPS URL to monitor. Validated structurally via HttpUrl. "
            "NOTE: Strict network/SSRF egress validation (private/link-local IP blocking) "
            "belongs to the probe execution engine in Chapter 5 and is not run during Chapter 1."
        ),
        examples=["https://status.github.com", "https://api.stripe.com/health"]
    )
    check_interval_seconds: int = Field(
        default=60,
        ge=15,
        le=86400,
        description="Evaluation interval in seconds. Minimum 15s, maximum 86400s (24h).",
        examples=[30, 60, 300]
    )


class MonitorRead(BaseModel):
    """
    Response contract representing a registered monitor.
    
    Configured with from_attributes=True so that Chapter 2 can serialize
    SQLAlchemy ORM model instances directly without manual dictionary mapping.
    """
    model_config = ConfigDict(from_attributes=True)

    id: int = Field(
        ...,
        description="Unique sequential identifier for the monitor."
    )
    name: str = Field(
        ...,
        description="Human-readable monitor name."
    )
    url: HttpUrl = Field(
        ...,
        description="Target URL being monitored."
    )
    check_interval_seconds: int = Field(
        ...,
        description="Scheduled probe frequency in seconds."
    )
    status: MonitorStatus = Field(
        ...,
        description="Current health status of the endpoint."
    )
    last_checked_at: datetime | None = Field(
        default=None,
        description="UTC timestamp of the most recent probe execution (None if pending initial check)."
    )
    next_check_at: datetime | None = Field(
        default=None,
        description="UTC timestamp when the monitor is next eligible for scheduler sweep."
    )


class MonitorUpdate(BaseModel):
    """
    Request contract for updating an existing monitor's configuration.
    
    Only fields explicitly provided are updated. Target URL modifications are
    deliberately excluded to maintain historical telemetry integrity.
    """
    name: str | None = Field(
        default=None,
        min_length=1,
        max_length=120,
        description="Updated display name. Omit or pass null to leave unchanged."
    )
    check_interval_seconds: int | None = Field(
        default=None,
        ge=15,
        le=86400,
        description="Updated check interval in seconds. Omit or pass null to leave unchanged."
    )
