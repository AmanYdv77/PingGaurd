"""
SSRF Defenses, IP Blocklists, and URL Sanitization Helpers.

Provides pure, non-network-I/O security functions to prevent Server-Side
Request Forgery (SSRF) and redact sensitive credentials from logged URLs.
"""

import ipaddress
import urllib.parse

# Forbidden IPv4 and IPv6 address ranges (RFC 1918, CGNAT, Loopback, Link-Local, ULA, Multicast)
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

NAT64_WELL_KNOWN_PREFIX = ipaddress.ip_network("64:ff9b::/96")


class SSRFBlockedError(Exception):
    """Raised when a destination URL or resolved IP is forbidden by SSRF policy."""
    pass


class DNSResolutionError(Exception):
    """Raised when a hostname cannot be resolved via DNS."""
    pass


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
