"""
PingGuard Schemas.

Defines the core Pydantic v2 data contracts for incoming requests and outgoing responses,
including the Optional Keep-Alive configuration.
"""

from datetime import datetime
from enum import Enum
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator


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


class MonitorMode(str, Enum):
    """
    Operational mode of the registered monitor.
    
    - MONITOR: Standard uptime and health checking only.
    - KEEP_ALIVE: Periodic lightweight activity intended to wake/keep an idle-prone service active.
    - MONITOR_AND_KEEP_ALIVE: Combines health monitoring and periodic keep-alive activity.
    
    NOTE: Keep-alive is an optional capability and acts as an activity/wake-up attempt;
    it is not a guarantee against provider-enforced idle termination.
    """
    MONITOR = "monitor"
    KEEP_ALIVE = "keep_alive"
    MONITOR_AND_KEEP_ALIVE = "monitor_and_keep_alive"


def validate_keep_alive_rules(
    mode: MonitorMode | None,
    keep_alive_enabled: bool | None,
    keep_alive_interval_seconds: int | None,
    keep_alive_path: str | None,
) -> None:
    """
    Central validation helper for consistency between mode, keep_alive_enabled,
    keep_alive_interval_seconds, and keep_alive_path.
    """
    if mode == MonitorMode.MONITOR:
        if keep_alive_enabled is True:
            raise ValueError("keep_alive_enabled must be False when mode is 'monitor'")
    elif mode in (MonitorMode.KEEP_ALIVE, MonitorMode.MONITOR_AND_KEEP_ALIVE):
        if keep_alive_enabled is False:
            raise ValueError(f"keep_alive_enabled must be True when mode is '{mode.value}'")

    if keep_alive_enabled is True:
        if keep_alive_interval_seconds is None:
            raise ValueError("keep_alive_interval_seconds is required when keep_alive_enabled is True")
    elif keep_alive_enabled is False:
        if keep_alive_interval_seconds is not None:
            raise ValueError("keep_alive_interval_seconds must be None when keep_alive_enabled is False")
        if keep_alive_path is not None:
            raise ValueError("keep_alive_path must be None when keep_alive_enabled is False")


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
            "is enforced by the probe execution engine."
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
    mode: MonitorMode = Field(
        default=MonitorMode.MONITOR,
        description="Determines whether the monitor performs health monitoring, keep-alive activity, or both."
    )
    keep_alive_enabled: bool = Field(
        default=False,
        description=(
            "Enables periodic lightweight requests intended to provide activity to idle-prone services. "
            "Behavior depends on the hosting provider and is NOT a guarantee of permanent uptime."
        )
    )
    keep_alive_interval_seconds: int | None = Field(
        default=None,
        ge=15,
        le=86400,
        description="Interval between keep-alive attempts in seconds (minimum 15s, maximum 86400s)."
    )
    keep_alive_path: str | None = Field(
        default=None,
        max_length=255,
        description="Relative endpoint path used for keep-alive requests, for example /health."
    )

    @field_validator("url")
    @classmethod
    def validate_url_scheme_and_safety(cls, v: HttpUrl) -> HttpUrl:
        scheme = v.scheme.lower()
        if scheme not in ("http", "https"):
            raise ValueError(f"URL scheme '{scheme}' is forbidden. Only 'http' and 'https' are allowed.")
        if v.username or v.password:
            raise ValueError("Credential-bearing URLs (user:password@) are forbidden for security reasons.")
        host = (v.host or "").strip().lower()
        if host in ("localhost", "localhost.localdomain"):
            raise ValueError(f"SSRF security violation: '{host}' is a forbidden loopback destination.")
        try:
            import ipaddress
            from app.net import is_ip_blocked
            ip_obj = ipaddress.ip_address(host)
            if is_ip_blocked(ip_obj):
                raise ValueError(f"SSRF security violation: IP literal '{host}' is private or restricted.")
        except ValueError as err:
            if "SSRF security violation" in str(err):
                raise
        return v

    @field_validator("keep_alive_path")
    @classmethod
    def validate_keep_alive_path(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if not v.startswith("/"):
            raise ValueError("keep_alive_path must begin with '/' and be a relative path")
        if v.startswith("//") or "://" in v:
            raise ValueError("keep_alive_path must be a relative path, not an absolute URL")
        return v

    @model_validator(mode="after")
    def validate_consistency(self) -> "MonitorCreate":
        validate_keep_alive_rules(
            self.mode,
            self.keep_alive_enabled,
            self.keep_alive_interval_seconds,
            self.keep_alive_path
        )
        return self


class MonitorRead(BaseModel):
    """
    Response contract representing a registered monitor.
    
    Configured with from_attributes=True to serialize SQLAlchemy ORM model instances directly.
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
    mode: MonitorMode = Field(
        default=MonitorMode.MONITOR,
        description="Configured monitor mode (monitor, keep_alive, monitor_and_keep_alive)."
    )
    keep_alive_enabled: bool = Field(
        default=False,
        description="Indicates whether periodic keep-alive requests are configured."
    )
    keep_alive_interval_seconds: int | None = Field(
        default=None,
        description="Interval between keep-alive attempts in seconds."
    )
    keep_alive_path: str | None = Field(
        default=None,
        description="Relative endpoint path used for keep-alive requests."
    )


class PingResultRead(BaseModel):
    """
    Response contract representing an individual probe execution or keep-alive check.
    """
    model_config = ConfigDict(from_attributes=True)

    id: int = Field(..., description="Unique sequential identifier for the result.")
    monitor_id: int = Field(..., description="Foreign key ID of the parent monitor.")
    check_type: str = Field(..., description="'monitor' for health checks, 'keep_alive' for wake-up activity.")
    status_code: int | None = Field(default=None, description="HTTP status code returned by target, or None if connection failed.")
    latency_ms: float | None = Field(default=None, description="Round-trip latency in milliseconds.")
    error: str | None = Field(default=None, description="Error classification string if the probe encountered an issue.")
    checked_at: datetime = Field(..., description="UTC timestamp of the probe execution.")


class MonitorCheckResponse(BaseModel):
    """
    Response model for an asynchronous monitor check request.
    """
    status: str = Field(default="queued", description="Dispatch status of the probe task.")
    monitor_id: int = Field(..., description="Unique integer ID of the monitor queued for checking.")


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
    mode: MonitorMode | None = Field(
        default=None,
        description="Updated monitor mode. Omit or pass null to leave unchanged."
    )
    keep_alive_enabled: bool | None = Field(
        default=None,
        description="Updated keep-alive enable flag. Omit or pass null to leave unchanged."
    )
    keep_alive_interval_seconds: int | None = Field(
        default=None,
        ge=15,
        le=86400,
        description="Updated keep-alive interval in seconds. Omit or pass null to leave unchanged."
    )
    keep_alive_path: str | None = Field(
        default=None,
        max_length=255,
        description="Updated keep-alive relative endpoint path. Omit or pass null to leave unchanged."
    )

    @field_validator("keep_alive_path")
    @classmethod
    def validate_keep_alive_path(cls, v: str | None) -> str | None:
        if v is None:
            return None
        if not v.startswith("/"):
            raise ValueError("keep_alive_path must begin with '/' and be a relative path")
        if v.startswith("//") or "://" in v:
            raise ValueError("keep_alive_path must be a relative path, not an absolute URL")
        return v

    @model_validator(mode="after")
    def validate_update_consistency(self) -> "MonitorUpdate":
        if self.keep_alive_enabled is True and self.keep_alive_interval_seconds is None:
            raise ValueError("keep_alive_interval_seconds is required when keep_alive_enabled is True")
        if self.keep_alive_enabled is False and self.keep_alive_interval_seconds is not None:
            raise ValueError("keep_alive_interval_seconds must be None when keep_alive_enabled is False")
        if self.mode == MonitorMode.MONITOR and self.keep_alive_enabled is True:
            raise ValueError("keep_alive_enabled must be False when mode is 'monitor'")
        if self.mode in (MonitorMode.KEEP_ALIVE, MonitorMode.MONITOR_AND_KEEP_ALIVE) and self.keep_alive_enabled is False:
            raise ValueError(f"keep_alive_enabled must be True when mode is '{self.mode.value}'")
        return self
