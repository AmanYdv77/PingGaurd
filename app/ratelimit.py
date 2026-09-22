"""
Rate limiting facilities for PingGuard write operations.

Provides fixed-window rate limiting backed by Redis with in-memory fallback for testing,
enforcing a fail-open policy so that rate limiter outages never disrupt service uptime monitoring.
"""

import hashlib
import logging
import time
from typing import Annotated, Any, Callable, Protocol
from fastapi import Depends, HTTPException, Request, status

from app.config import get_settings

logger = logging.getLogger("app.ratelimit")
logger.disabled = False



class RateLimiter(Protocol):
    """Protocol defining the rate limiter interface."""

    async def hit(self, key: str, limit: int, window_seconds: int) -> tuple[bool, int]:
        """
        Record an access hit for a key.
        Returns a tuple of (allowed: bool, retry_after_seconds: int).
        """
        ...


class RedisRateLimiter:
    """Fixed-window rate limiter backed by Redis using INCR and EXPIRE pipeline."""

    def __init__(self, redis_url: str | None = None, client: Any = None) -> None:
        self._redis_url = redis_url
        self._client = client

    async def _get_client(self) -> Any:
        if self._client is None:
            import redis.asyncio as aioredis
            self._client = aioredis.from_url(self._redis_url, decode_responses=True)
        return self._client

    async def hit(self, key: str, limit: int, window_seconds: int) -> tuple[bool, int]:
        redis_key = f"rl:{key}"
        try:
            client = await self._get_client()
            async with client.pipeline(transaction=True) as pipe:
                pipe.incr(redis_key)
                pipe.ttl(redis_key)
                count, ttl = await pipe.execute()

            # Set expiration if key was newly created or has no TTL
            if ttl < 0:
                await client.expire(redis_key, window_seconds)
                ttl = window_seconds

            if count <= limit:
                return True, 0

            retry_after = max(1, int(ttl))
            return False, retry_after
        except Exception as exc:
            logger.warning("Redis rate limiter unavailable (%s); failing open.", exc)
            return True, 0


class InMemoryRateLimiter:
    """In-memory fixed-window rate limiter with injectable clock for deterministic testing."""

    def __init__(self, clock: Callable[[], float] | None = None) -> None:
        self._clock = clock or time.monotonic
        self._data: dict[str, tuple[int, float]] = {}

    async def hit(self, key: str, limit: int, window_seconds: int) -> tuple[bool, int]:
        now = self._clock()
        if key not in self._data:
            self._data[key] = (1, now + window_seconds)
            return True, 0

        count, reset_at = self._data[key]
        if now >= reset_at:
            self._data[key] = (1, now + window_seconds)
            return True, 0

        if count < limit:
            self._data[key] = (count + 1, reset_at)
            return True, 0

        retry_after = max(1, int(reset_at - now))
        return False, retry_after


_limiter_instance: RedisRateLimiter | None = None


def get_rate_limiter() -> RateLimiter:
    """Dependency provider returning the active rate limiter."""
    global _limiter_instance
    if _limiter_instance is None:
        settings = get_settings()
        _limiter_instance = RedisRateLimiter(settings.redis_broker_url)
    return _limiter_instance


async def rate_limit_write(
    request: Request,
    limiter: Annotated[RateLimiter, Depends(get_rate_limiter)],
) -> None:
    """
    FastAPI dependency applying per-API-key (or IP fallback) write rate limiting.
    Raises HTTP 429 Too Many Requests if rate limit is exceeded.
    """
    settings = get_settings()
    api_key_val = request.headers.get("X-API-Key")
    if api_key_val:
        key = hashlib.sha256(api_key_val.encode("utf-8")).hexdigest()[:16]
    elif request.client and request.client.host:
        key = f"ip:{request.client.host}"
    else:
        key = "unknown_client"

    try:
        allowed, retry_after = await limiter.hit(
            key=key,
            limit=settings.rate_limit_writes_per_minute,
            window_seconds=60,
        )
    except Exception as exc:
        logger.disabled = False
        logger.warning("Redis rate limiter check failed (%s); failing open.", exc)
        return


    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="rate limit exceeded",
            headers={"Retry-After": str(retry_after)},
        )
