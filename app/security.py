"""
Security and authentication dependencies for PingGuard.

Provides static API-key authentication for protected endpoints.
Uses constant-time comparison to prevent timing side-channel attacks.
"""

import secrets
from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

from app.config import get_settings

# Security scheme definition for OpenAPI documentation
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(
    api_key: str | None = Security(api_key_header),
) -> str:
    """
    FastAPI dependency enforcing static API-key authentication.
    
    Validates that the incoming request contains an 'X-API-Key' header that
    matches the configured API_KEY using secrets.compare_digest.
    
    Raises:
        HTTPException: 401 Unauthorized if the header is missing or incorrect.
                       The key value is NEVER included in error text or logs.
    """
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid or missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    settings = get_settings()
    configured_key = settings.api_key.get_secret_value()

    if not secrets.compare_digest(api_key.encode("utf-8"), configured_key.encode("utf-8")):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid or missing API key",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    return api_key
