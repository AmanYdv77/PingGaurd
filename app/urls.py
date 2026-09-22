"""
Pure URL manipulation utilities for PingGuard.
Contains no application dependencies to prevent import cycles.
"""


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
