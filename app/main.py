"""
PingGuard API — Chapter 2: The Persistence Layer

FastAPI application entry point.
Implements asynchronous REST endpoints for monitor registration, retrieval,
update, and listing, backed durably by PostgreSQL via SQLAlchemy 2.0 AsyncSession.

Architectural Rule (Chapter 2 Boundary):
----------------------------------------
The database is the single durable source of truth.
Outbound network pings (requests.get, httpx probes) MUST NEVER be executed
inside these routes. Probing execution is strictly isolated to background workers
in Chapter 3.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Annotated
from fastapi import Depends, FastAPI, HTTPException, Path, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models import Monitor
from app.schemas import (
    MonitorCreate,
    MonitorMode,
    MonitorRead,
    MonitorStatus,
    MonitorUpdate,
    validate_keep_alive_rules,
)

# Application metadata for OpenAPI /docs and /redoc
app = FastAPI(
    title="PingGuard API",
    description=(
        "Distributed Uptime Monitoring System — Chapter 2: The Persistence Layer.\n\n"
        "Provides non-blocking endpoints backed durably by PostgreSQL via SQLAlchemy 2.0 Async.\n"
        "Manages monitor definitions, scheduling states, and optional keep-alive parameters.\n\n"
        "**Note on Keep-Alive:** Keep-alive activity sends periodic lightweight requests "
        "to reduce idle sleeping on platforms that spin down. It is an optional activity attempt, "
        "not a provider-level guarantee of permanent uptime."
    ),
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)


@app.get(
    "/health",
    tags=["System"],
    summary="Health check",
    description="Returns the operational status of the PingGuard Chapter 2 API service."
)
async def health_check() -> dict[str, str]:
    """Basic service health check endpoint."""
    return {
        "status": "healthy",
        "service": "PingGuard API",
        "milestone": "Chapter 2 — The Persistence Layer (SQLAlchemy 2.0 Async + PostgreSQL)",
    }


@app.post(
    "/monitors/",
    response_model=MonitorRead,
    status_code=status.HTTP_201_CREATED,
    tags=["Monitors"],
    summary="Create a new monitor",
    description=(
        "Validates and persists a new monitor endpoint in PostgreSQL with optional keep-alive settings. "
        "Initial status is set to PENDING. No outbound network probe is performed in this request."
    ),
)
async def create_monitor(
    payload: MonitorCreate,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> MonitorRead:
    """
    Asynchronously registers a target URL for monitoring and/or keep-alive in PostgreSQL.
    
    - Validates payload via MonitorCreate schema.
    - Enforces keep-alive configuration consistency.
    - Persists durable row in the 'monitors' table with auto-generated ID.
    - Initializes status to PENDING and marks next_check_at as due immediately.
    - Returns serialized MonitorRead.
    """
    now = datetime.now(timezone.utc)
    monitor = Monitor(
        name=payload.name,
        url=str(payload.url),
        check_interval_seconds=payload.check_interval_seconds,
        status=MonitorStatus.PENDING.value,
        last_checked_at=None,
        next_check_at=now,
        mode=payload.mode.value,
        keep_alive_enabled=payload.keep_alive_enabled,
        keep_alive_interval_seconds=payload.keep_alive_interval_seconds,
        keep_alive_path=payload.keep_alive_path,
        next_keep_alive_at=now if payload.keep_alive_enabled else None,
    )
    db.add(monitor)
    await db.commit()
    await db.refresh(monitor)
    return monitor


@app.get(
    "/monitors/{monitor_id}",
    response_model=MonitorRead,
    tags=["Monitors"],
    summary="Get monitor by ID",
    description="Retrieves configuration, status, and keep-alive settings for a single monitor from PostgreSQL.",
)
async def get_monitor(
    monitor_id: Annotated[int, Path(..., description="The unique integer ID of the monitor", ge=1)],
    db: Annotated[AsyncSession, Depends(get_db)],
) -> MonitorRead:
    """
    Asynchronously fetches a monitor from PostgreSQL by its unique ID.
    
    Raises HTTP 404 if the monitor does not exist.
    """
    monitor = await db.get(Monitor, monitor_id)
    if monitor is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Monitor not found",
        )
    return monitor


@app.patch(
    "/monitors/{monitor_id}",
    response_model=MonitorRead,
    tags=["Monitors"],
    summary="Update monitor (Partial)",
    description="Partially updates an existing monitor's name, check interval, or keep-alive configuration in PostgreSQL.",
)
@app.put(
    "/monitors/{monitor_id}",
    response_model=MonitorRead,
    tags=["Monitors"],
    summary="Update monitor",
    description="Updates an existing monitor's configuration, including keep-alive parameters in PostgreSQL.",
)
async def update_monitor(
    monitor_id: Annotated[int, Path(..., description="The unique integer ID of the monitor", ge=1)],
    payload: MonitorUpdate,
    db: Annotated[AsyncSession, Depends(get_db)],
) -> MonitorRead:
    """
    Asynchronously updates an existing monitor in PostgreSQL.
    
    Supports both PATCH and PUT semantics. Fields omitted or set to None are preserved.
    Enforces configuration consistency across the merged monitor state.
    Raises HTTP 404 if the monitor does not exist, or HTTP 422 if the update creates an invalid state.
    """
    monitor = await db.get(Monitor, monitor_id)
    if monitor is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Monitor not found",
        )

    update_data = payload.model_dump(exclude_unset=True)

    # Handle keep_alive_enabled=False reset logic
    if "keep_alive_enabled" in update_data and update_data["keep_alive_enabled"] is False:
        if "keep_alive_interval_seconds" not in update_data:
            update_data["keep_alive_interval_seconds"] = None
        if "keep_alive_path" not in update_data:
            update_data["keep_alive_path"] = None
        if "mode" not in update_data and monitor.mode != MonitorMode.MONITOR.value:
            update_data["mode"] = MonitorMode.MONITOR.value

    # Compute merged state for consistency validation
    target_mode = MonitorMode(update_data.get("mode", monitor.mode))
    target_keep_alive = update_data.get("keep_alive_enabled", monitor.keep_alive_enabled)
    target_interval = update_data.get("keep_alive_interval_seconds", monitor.keep_alive_interval_seconds)
    target_path = update_data.get("keep_alive_path", monitor.keep_alive_path)

    try:
        validate_keep_alive_rules(
            mode=target_mode,
            keep_alive_enabled=target_keep_alive,
            keep_alive_interval_seconds=target_interval,
            keep_alive_path=target_path,
        )
    except ValueError as err:
        raise HTTPException(status_code=422, detail=str(err))

    # Apply updates
    for key, value in update_data.items():
        if hasattr(monitor, key):
            val_to_set = value.value if isinstance(value, Enum) else value
            setattr(monitor, key, val_to_set)

    # Maintain next_keep_alive_at synchronization
    if monitor.keep_alive_enabled and monitor.next_keep_alive_at is None:
        monitor.next_keep_alive_at = datetime.now(timezone.utc)
    elif not monitor.keep_alive_enabled:
        monitor.next_keep_alive_at = None

    await db.commit()
    await db.refresh(monitor)
    return monitor


@app.get(
    "/monitors/",
    response_model=list[MonitorRead],
    tags=["Monitors"],
    summary="List all monitors",
    description="Retrieves a paginated list of registered monitors from PostgreSQL.",
)
async def list_monitors(
    db: Annotated[AsyncSession, Depends(get_db)],
    skip: Annotated[int, Query(description="Number of records to skip (offset)", ge=0)] = 0,
    limit: Annotated[int, Query(description="Maximum records to return", ge=1, le=1000)] = 100,
) -> list[MonitorRead]:
    """
    Asynchronously retrieves a paginated slice of monitors from PostgreSQL.
    
    Guarded with default limit=100 to prevent unbounded database query memory allocation.
    """
    stmt = select(Monitor).order_by(Monitor.id.asc()).offset(skip).limit(limit)
    result = await db.execute(stmt)
    return list(result.scalars().all())
