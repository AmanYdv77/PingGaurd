"""
Network Resilience & Probing Engine.

Provides an observable network execution layer with timeouts, SSRF defense,
streaming response bounds, monotonic latency tracking, and structured outcome classification.
"""

from dataclasses import dataclass
from enum import Enum
import ipaddress
import logging
import socket
import ssl
import time
import urllib.parse
from typing import Any, Callable

import httpx
from app.config import get_settings

logger = logging.getLogger(__name__)

# =============================================================================
# 1. Result DTOs & Outcomes
# =============================================================================

class PingOutcome(str, Enum):
    """Classification of the network probe outcome."""
    UP = "up"
    DEGRADED = "degraded"
    DOWN = "down"
    UNREACHABLE = "unreachable"


@dataclass(frozen=True)
class PingResultDTO:
    """
    Structured outcome returned by the network prober.
    
    Attributes:
        outcome: Tagged classification (UP, DEGRADED, DOWN, UNREACHABLE).
        status_code: HTTP response code if target responded, else None.
        latency_ms: Round-trip duration in milliseconds (monotonic).
        error_detail: Concise, stable error identifier (e.g. 'connect_timeout', 'ssrf_blocked').
        original_url: Target URL provided for evaluation.
        final_url: Effective destination URL after following redirects.
    """
    outcome: PingOutcome
    status_code: int | None
    latency_ms: float | None
    error_detail: str | None
    original_url: str
    final_url: str | None


# =============================================================================
# 2. Configuration & Default Settings
# =============================================================================




# =============================================================================
# 3. SSRF Defense & Blocklists
# =============================================================================

# Forbidden IPv4 and IPv6 address ranges
BLOCKED_NETWORKS = (
    ipaddress.ip_network("127.0.0.0/8"),        # Loopback IPv4
    ipaddress.ip_network("10.0.0.0/8"),         # RFC 1918 Class A
    ipaddress.ip_network("172.16.0.0/12"),      # RFC 1918 Class B
    ipaddress.ip_network("192.168.0.0/16"),     # RFC 1918 Class C
    ipaddress.ip_network("169.254.0.0/16"),     # Link-Local / Cloud Metadata (169.254.169.254)
    ipaddress.ip_network("0.0.0.0/8"),          # Current Network
    ipaddress.ip_network("100.64.0.0/10"),      # Carrier-Grade NAT (RFC 6598)
    ipaddress.ip_network("192.0.0.0/24"),       # IETF Protocol Assignments
    ipaddress.ip_network("192.0.2.0/24"),       # TEST-NET-1
    ipaddress.ip_network("198.51.100.0/24"),    # TEST-NET-2
    ipaddress.ip_network("203.0.113.0/24"),     # TEST-NET-3
    ipaddress.ip_network("224.0.0.0/4"),        # Multicast
    ipaddress.ip_network("240.0.0.0/4"),        # Reserved
    ipaddress.ip_network("255.255.255.255/32"), # Broadcast
    # IPv6 Networks
    ipaddress.ip_network("::1/128"),            # Loopback IPv6
    ipaddress.ip_network("::/128"),             # Unspecified IPv6
    ipaddress.ip_network("fe80::/10"),          # Link-Local IPv6
    ipaddress.ip_network("fc00::/7"),           # Unique Local Address (ULA) IPv6
    ipaddress.ip_network("ff00::/8"),           # Multicast IPv6
)


class SSRFBlockedError(Exception):
    """Raised when a destination URL or resolved IP is forbidden by SSRF policy."""
    pass


class DNSResolutionError(Exception):
    """Raised when a hostname cannot be resolved via DNS."""
    pass


NAT64_WELL_KNOWN_PREFIX = ipaddress.ip_network("64:ff9b::/96")


def is_ip_blocked(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """
    Evaluates whether an IP address belongs to any blocked, private, or reserved network.
    
    Supports RFC 6052 NAT64 Well-Known Prefix (64:ff9b::/96) synthesized by DNS64/NAT64
    networks for public IPv4 endpoints by recursing on the embedded IPv4 destination.
    """
    if isinstance(ip, ipaddress.IPv6Address) and ip in NAT64_WELL_KNOWN_PREFIX:
        embedded_v4 = ipaddress.IPv4Address(ip.packed[-4:])
        return is_ip_blocked(embedded_v4)

    if (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    ):
        return True

    for blocked_net in BLOCKED_NETWORKS:
        if ip in blocked_net:
            return True

    return False


def redact_url_credentials(url: str) -> str:
    """
    Replaces user:password authentication details in URLs with ***:*** for safe logging.
    
    Examples:
        redact_url_credentials("https://user:pass@domain.com/path")
        -> "https://***:***@domain.com/path"
    """
    try:
        parsed = urllib.parse.urlsplit(url)
        if parsed.username or parsed.password:
            netloc = parsed.hostname or ""
            if parsed.port:
                netloc += f":{parsed.port}"
            netloc = f"***:***@{netloc}"
            return urllib.parse.urlunsplit(
                (parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment)
            )
        return url
    except Exception:
        return "[redacted_url]"


def resolve_and_validate_target(
    url: str,
    allow_loopback: bool = False,
    dns_resolver: Callable[[str, int], list[tuple[Any, ...]]] | None = None,
) -> tuple[urllib.parse.SplitResult, list[str]]:
    """
    Validates scheme, normalizes URL, resolves hostname, and checks against SSRF blocklist.
    
    Args:
        url: The target URL to validate.
        allow_loopback: If True, permits loopback/private destinations (strictly for unit tests).
        dns_resolver: Optional custom resolver for deterministic testing (defaults to socket.getaddrinfo).
        
    Returns:
        tuple of (parsed_url, list_of_resolved_ips)
        
    Raises:
        SSRFBlockedError: If scheme is forbidden, or if ANY resolved IP falls in a blocked range.
        DNSResolutionError: If hostname resolution fails.
    """
    try:
        parsed = urllib.parse.urlsplit(url)
    except Exception as exc:
        raise SSRFBlockedError(f"Malformed URL: {exc}") from exc

    # 1. Enforce allowed schemes (HTTP/HTTPS only)
    if parsed.scheme.lower() not in ("http", "https"):
        raise SSRFBlockedError(f"Unsupported URL scheme: '{parsed.scheme}'. Only HTTP and HTTPS are permitted.")

    hostname = parsed.hostname
    if not hostname:
        raise SSRFBlockedError("URL must contain a valid hostname or IP address.")

    hostname_clean = hostname.strip().lower()

    # 2. Reject credentials in URL
    if parsed.username or parsed.password:
        raise SSRFBlockedError("Embedded credentials in URLs (user:pass@) are forbidden.")

    # 3. Direct literal IP check (handles IPv4/IPv6 literals without DNS)
    try:
        literal_ip = ipaddress.ip_address(hostname_clean)
        if is_ip_blocked(literal_ip):
            if allow_loopback and (literal_ip.is_loopback or str(literal_ip) == "127.0.0.1"):
                pass  # Permitted only for loopback testing
            else:
                raise SSRFBlockedError(f"SSRF blocked: IP literal '{hostname_clean}' is private or reserved.")
        return parsed, [str(literal_ip)]
    except ValueError:
        pass  # Hostname is not an IP literal, proceed to DNS resolution

    # 4. Obvious loopback hostname check
    if hostname_clean in ("localhost", "localhost.localdomain"):
        if not allow_loopback:
            raise SSRFBlockedError(f"SSRF blocked: Hostname '{hostname_clean}' is a loopback alias.")

    # 5. Resolve hostname to all associated IPs
    port = parsed.port or (443 if parsed.scheme.lower() == "https" else 80)
    resolver = dns_resolver or socket.getaddrinfo

    try:
        addr_info = resolver(hostname_clean, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise DNSResolutionError(f"DNS resolution failed for '{hostname_clean}': {exc}") from exc
    except Exception as exc:
        raise DNSResolutionError(f"DNS lookup error for '{hostname_clean}': {exc}") from exc

    resolved_ips: list[str] = []
    for entry in addr_info:
        sockaddr = entry[4]
        ip_str = sockaddr[0]
        if ip_str not in resolved_ips:
            resolved_ips.append(ip_str)

    if not resolved_ips:
        raise DNSResolutionError(f"No IP addresses resolved for hostname '{hostname_clean}'.")

    # 6. Verify that NO resolved address is blocked
    for ip_str in resolved_ips:
        try:
            ip_obj = ipaddress.ip_address(ip_str)
            if is_ip_blocked(ip_obj):
                if allow_loopback and (ip_obj.is_loopback or str(ip_obj) == "127.0.0.1"):
                    pass  # Permitted for local loopback testing
                else:
                    logger.warning(
                        "SSRF blocked target='%s': hostname '%s' resolved to forbidden IP '%s'",
                        redact_url_credentials(url),
                        hostname_clean,
                        ip_str,
                    )
                    raise SSRFBlockedError(
                        f"SSRF blocked: Hostname '{hostname_clean}' resolved to private/restricted IP '{ip_str}'."
                    )
        except ValueError:
            raise SSRFBlockedError(f"Invalid resolved IP format: '{ip_str}'.")

    return parsed, resolved_ips


# =============================================================================
# 4. Exception Helper
# =============================================================================

def is_tls_exception(exc: Exception) -> bool:
    """Detects whether an exception stems from a TLS/SSL handshake or certificate error."""
    curr: Exception | None = exc
    while curr is not None:
        if isinstance(curr, (ssl.SSLError, ssl.CertificateError)):
            return True
        exc_str = str(curr).lower()
        exc_name = type(curr).__name__.lower()
        if "ssl" in exc_name or "certificate" in exc_str or "tls" in exc_str:
            return True
        curr = getattr(curr, "__cause__", None) or getattr(curr, "__context__", None)
    return False


# =============================================================================
# 5. Core Asynchronous Network Probe Engine
# =============================================================================

async def perform_http_probe(
    url: str,
    user_agent: str | None = None,
    connect_timeout: float | None = None,
    read_timeout: float | None = None,
    write_timeout: float | None = None,
    pool_timeout: float | None = None,
    max_response_bytes: int | None = None,
    max_redirects: int | None = None,
    allow_loopback: bool = False,
    dns_resolver: Callable[[str, int], list[tuple[Any, ...]]] | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> PingResultDTO:
    """
    Executes an asynchronous HTTP GET probe against the target URL.
    
    Enforces SSRF defense, granular timeouts, streaming memory limits,
    and redirect interception.
    """
    safe_url = redact_url_credentials(url)
    start_mono = time.monotonic()

    # Step 1: Pre-request SSRF & DNS validation
    try:
        resolve_and_validate_target(
            url,
            allow_loopback=allow_loopback,
            dns_resolver=dns_resolver,
        )
    except SSRFBlockedError as exc:
        logger.warning("Target rejected by SSRF defense: %s (%s)", safe_url, exc)
        return PingResultDTO(
            outcome=PingOutcome.UNREACHABLE,
            status_code=None,
            latency_ms=None,
            error_detail="ssrf_blocked",
            original_url=url,
            final_url=None,
        )
    except DNSResolutionError as exc:
        elapsed_ms = round((time.monotonic() - start_mono) * 1000, 2)
        logger.warning("Target DNS resolution failed: %s (%s)", safe_url, exc)
        return PingResultDTO(
            outcome=PingOutcome.UNREACHABLE,
            status_code=None,
            latency_ms=elapsed_ms,
            error_detail="dns_error",
            original_url=url,
            final_url=None,
        )

    # Step 2: Resolve runtime configuration and configure granular timeouts
    settings = get_settings()
    ua = user_agent or settings.http_user_agent
    c_timeout = connect_timeout if connect_timeout is not None else settings.http_connect_timeout
    r_timeout = read_timeout if read_timeout is not None else settings.http_read_timeout
    w_timeout = write_timeout if write_timeout is not None else settings.http_write_timeout
    p_timeout = pool_timeout if pool_timeout is not None else settings.http_pool_timeout
    max_bytes = max_response_bytes if max_response_bytes is not None else settings.http_max_response_bytes
    max_redirs = max_redirects if max_redirects is not None else settings.http_max_redirects

    timeouts = httpx.Timeout(
        connect=c_timeout,
        read=r_timeout,
        write=w_timeout,
        pool=p_timeout,
    )

    # Step 3: Redirect SSRF Interception Hook
    async def redirect_ssrf_hook(response: httpx.Response) -> None:
        if response.is_redirect:
            location = response.headers.get("Location")
            if location:
                dest_url = str(response.url.join(location))
                resolve_and_validate_target(
                    dest_url,
                    allow_loopback=False,  # Redirects must NEVER target loopback, private, or cloud metadata
                    dns_resolver=dns_resolver,
                )

    client_kwargs: dict[str, Any] = {
        "timeout": timeouts,
        "max_redirects": max_redirs,
        "event_hooks": {"response": [redirect_ssrf_hook]},
        "headers": {
            "User-Agent": ua,
            "Accept": "*/*",
        },
        "follow_redirects": True,
    }
    if transport is not None:
        client_kwargs["transport"] = transport

    # Step 4: Execute HTTP probe with bounded streaming
    status_code: int | None = None
    final_url: str | None = None
    bytes_read = 0

    try:
        async with httpx.AsyncClient(**client_kwargs) as client:
            async with client.stream("GET", url) as response:
                status_code = response.status_code
                final_url = str(response.url)

                # Memory-bounded streaming: stop reading if body exceeds max_response_bytes
                async for chunk in response.aiter_bytes():
                    bytes_read += len(chunk)
                    if bytes_read > max_bytes:
                        logger.warning(
                            "Response from '%s' exceeded MAX_RESPONSE_BYTES (%d), terminating stream early",
                            safe_url,
                            max_bytes,
                        )
                        break

        latency_ms = round((time.monotonic() - start_mono) * 1000, 2)

        # Step 5: Status code classification
        if 200 <= status_code < 400:
            outcome = PingOutcome.UP
        elif 400 <= status_code < 500:
            outcome = PingOutcome.DEGRADED
        else:
            outcome = PingOutcome.DOWN

        return PingResultDTO(
            outcome=outcome,
            status_code=status_code,
            latency_ms=latency_ms,
            error_detail=None,
            original_url=url,
            final_url=final_url,
        )

    except SSRFBlockedError as exc:
        logger.warning("SSRF blocked during redirect for '%s': %s", safe_url, exc)
        return PingResultDTO(
            outcome=PingOutcome.UNREACHABLE,
            status_code=None,
            latency_ms=None,
            error_detail="ssrf_blocked",
            original_url=url,
            final_url=None,
        )

    except httpx.ConnectTimeout as exc:
        elapsed_ms = round((time.monotonic() - start_mono) * 1000, 2)
        logger.info("Connect timeout on '%s' (%s)", safe_url, exc)
        return PingResultDTO(
            outcome=PingOutcome.UNREACHABLE,
            status_code=None,
            latency_ms=elapsed_ms,
            error_detail="connect_timeout",
            original_url=url,
            final_url=None,
        )

    except httpx.ReadTimeout as exc:
        elapsed_ms = round((time.monotonic() - start_mono) * 1000, 2)
        logger.info("Read timeout on '%s' (%s)", safe_url, exc)
        return PingResultDTO(
            outcome=PingOutcome.UNREACHABLE,
            status_code=None,
            latency_ms=elapsed_ms,
            error_detail="read_timeout",
            original_url=url,
            final_url=None,
        )

    except (httpx.WriteTimeout, httpx.PoolTimeout) as exc:
        elapsed_ms = round((time.monotonic() - start_mono) * 1000, 2)
        error_name = "write_timeout" if isinstance(exc, httpx.WriteTimeout) else "pool_timeout"
        logger.info("%s on '%s' (%s)", error_name, safe_url, exc)
        return PingResultDTO(
            outcome=PingOutcome.UNREACHABLE,
            status_code=None,
            latency_ms=elapsed_ms,
            error_detail=error_name,
            original_url=url,
            final_url=None,
        )

    except httpx.TooManyRedirects as exc:
        elapsed_ms = round((time.monotonic() - start_mono) * 1000, 2)
        logger.info("Too many redirects on '%s' (%s)", safe_url, exc)
        return PingResultDTO(
            outcome=PingOutcome.UNREACHABLE,
            status_code=None,
            latency_ms=elapsed_ms,
            error_detail="redirect_error",
            original_url=url,
            final_url=None,
        )

    except httpx.ConnectError as exc:
        elapsed_ms = round((time.monotonic() - start_mono) * 1000, 2)
        if is_tls_exception(exc):
            logger.info("TLS certificate error on '%s' (%s)", safe_url, exc)
            err = "tls_error"
        else:
            logger.info("Connection failed on '%s' (%s)", safe_url, exc)
            err = "connect_error"

        return PingResultDTO(
            outcome=PingOutcome.UNREACHABLE,
            status_code=None,
            latency_ms=elapsed_ms,
            error_detail=err,
            original_url=url,
            final_url=None,
        )

    except httpx.RequestError as exc:
        elapsed_ms = round((time.monotonic() - start_mono) * 1000, 2)
        logger.info("HTTP request error on '%s' (%s)", safe_url, exc)
        return PingResultDTO(
            outcome=PingOutcome.UNREACHABLE,
            status_code=None,
            latency_ms=elapsed_ms,
            error_detail="request_error",
            original_url=url,
            final_url=None,
        )


# =============================================================================
# 6. Synchronous Worker Bridge Functions
# =============================================================================

def robust_ping(
    url: str,
    allow_loopback: bool = False,
    transport: httpx.AsyncBaseTransport | None = None,
    dns_resolver: Callable[[str, int], list[tuple[Any, ...]]] | None = None,
) -> PingResultDTO:
    """
    Synchronous bridge for Celery workers to execute an uptime health probe.
    
    Uses asyncio.run() to safely invoke the asynchronous httpx network engine.
    """
    import asyncio
    settings = get_settings()
    return asyncio.run(
        perform_http_probe(
            url=url,
            user_agent=settings.http_user_agent,
            allow_loopback=allow_loopback,
            transport=transport,
            dns_resolver=dns_resolver,
        )
    )


def robust_keep_alive(
    url: str,
    path: str | None = None,
    allow_loopback: bool = False,
    transport: httpx.AsyncBaseTransport | None = None,
    dns_resolver: Callable[[str, int], list[tuple[Any, ...]]] | None = None,
) -> PingResultDTO:
    """
    Synchronous bridge for Celery workers to execute a Keep-Alive activity ping.
    
    Safely joins url + path and executes using the Keep-Alive User-Agent.
    """
    import asyncio
    from app.tasks import safe_join_url
    target_url = safe_join_url(url, path)
    settings = get_settings()

    return asyncio.run(
        perform_http_probe(
            url=target_url,
            user_agent=settings.http_keep_alive_user_agent,
            allow_loopback=allow_loopback,
            transport=transport,
            dns_resolver=dns_resolver,
        )
    )
