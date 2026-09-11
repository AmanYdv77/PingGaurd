"""
In-Memory Store — Chapter 1 Persistence Layer.

Provides an isolated in-memory storage abstraction for PingGuard monitors.
Decoupled from route handlers via FastAPI dependency injection so Chapter 2
can replace this store with an async SQLAlchemy session provider with zero
changes to the API contract.
"""

from datetime import datetime, timezone
from typing import Any
from fastapi import HTTPException
from app.schemas import (
    MonitorCreate,
    MonitorMode,
    MonitorStatus,
    MonitorUpdate,
    validate_keep_alive_rules,
)


class InMemoryMonitorStore:
    """
    Thread-safe (asyncio single-process) in-memory repository for Chapter 1.
    
    Acts as a stand-in for the future PostgreSQL 'monitors' table.
    """

    def __init__(self) -> None:
        self._monitors: dict[int, dict[str, Any]] = {}
        self._next_id: int = 1

    async def create(self, data: MonitorCreate) -> dict[str, Any]:
        """
        Creates and persists a new monitor record in memory.
        Initializes status to PENDING and sets next_check_at to now UTC.
        Stores optional keep-alive configuration without executing network tasks.
        """
        monitor_id = self._next_id
        self._next_id += 1

        now = datetime.now(timezone.utc)
        record = {
            "id": monitor_id,
            "name": data.name,
            "url": str(data.url),
            "check_interval_seconds": data.check_interval_seconds,
            "status": MonitorStatus.PENDING,
            "last_checked_at": None,
            "next_check_at": now,
            "mode": data.mode,
            "keep_alive_enabled": data.keep_alive_enabled,
            "keep_alive_interval_seconds": data.keep_alive_interval_seconds,
            "keep_alive_path": data.keep_alive_path,
        }
        self._monitors[monitor_id] = record
        return dict(record)

    async def get_by_id(self, monitor_id: int) -> dict[str, Any] | None:
        """Retrieves a single monitor record by its integer ID."""
        record = self._monitors.get(monitor_id)
        return dict(record) if record else None

    async def update(self, monitor_id: int, data: MonitorUpdate) -> dict[str, Any] | None:
        """
        Updates an existing monitor's mutable fields.
        Validates consistency across the merged state before persisting.
        Returns None if monitor does not exist.
        """
        record = self._monitors.get(monitor_id)
        if not record:
            return None

        merged = dict(record)
        if data.name is not None:
            merged["name"] = data.name
        if data.check_interval_seconds is not None:
            merged["check_interval_seconds"] = data.check_interval_seconds
        if data.mode is not None:
            merged["mode"] = data.mode
        if data.keep_alive_enabled is not None:
            merged["keep_alive_enabled"] = data.keep_alive_enabled
        if data.keep_alive_interval_seconds is not None:
            merged["keep_alive_interval_seconds"] = data.keep_alive_interval_seconds
        if data.keep_alive_path is not None:
            merged["keep_alive_path"] = data.keep_alive_path

        # If keep-alive is explicitly disabled, reset interval and path unless explicitly specified
        if data.keep_alive_enabled is False:
            if data.keep_alive_interval_seconds is None:
                merged["keep_alive_interval_seconds"] = None
            if data.keep_alive_path is None:
                merged["keep_alive_path"] = None
            if data.mode is None and merged["mode"] != MonitorMode.MONITOR:
                merged["mode"] = MonitorMode.MONITOR

        # Verify merged state consistency
        try:
            validate_keep_alive_rules(
                mode=merged["mode"],
                keep_alive_enabled=merged["keep_alive_enabled"],
                keep_alive_interval_seconds=merged["keep_alive_interval_seconds"],
                keep_alive_path=merged["keep_alive_path"],
            )
        except ValueError as err:
            raise HTTPException(status_code=422, detail=str(err))

        self._monitors[monitor_id] = merged
        return dict(merged)

    async def list_monitors(self, skip: int = 0, limit: int = 100) -> list[dict[str, Any]]:
        """
        Lists stored monitors with basic offset/limit pagination.
        Guards against unbounded response sizes.
        """
        all_records = list(self._monitors.values())
        return [dict(r) for r in all_records[skip : skip + limit]]

    async def delete(self, monitor_id: int) -> bool:
        """Deletes a monitor by ID. Returns True if deleted, False if not found."""
        if monitor_id in self._monitors:
            del self._monitors[monitor_id]
            return True
        return False

    def clear(self) -> None:
        """Resets storage. Used exclusively for isolating automated test runs."""
        self._monitors.clear()
        self._next_id = 1


# Global singleton instance for the Chapter 1 runtime
_global_store = InMemoryMonitorStore()


async def get_store() -> InMemoryMonitorStore:
    """
    FastAPI dependency that provides access to the monitor store.
    
    Chapter 1 -> Chapter 2 Handoff:
    In Chapter 2, this dependency will be swapped for an AsyncSession generator:
        async def get_db() -> AsyncGenerator[AsyncSession, None]: ...
    allowing the route handlers to remain clean and decoupled.
    """
    return _global_store
