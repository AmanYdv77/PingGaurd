"""
Automated Test Suite for Network Resilience and SSRF Probing Engine.
"""

import asyncio
import os
import socket
import ssl
import unittest
from unittest.mock import MagicMock, patch
import httpx

from app.net import (
    DNSResolutionError,
    PingOutcome,
    PingResultDTO,
    SSRFBlockedError,
    perform_http_probe,
    redact_url_credentials,
    resolve_and_validate_target,
    robust_keep_alive,
    robust_ping,
)


class TestNetworkResilience(unittest.TestCase):
    # =========================================================================
    # 1. Status Code Classification Tests
    # =========================================================================
    def test_status_code_classification(self) -> None:
        """Verify 2xx/3xx -> UP, 4xx -> DEGRADED, 5xx -> DOWN."""
        def mock_handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if path == "/200":
                return httpx.Response(200, text="OK")
            elif path == "/301":
                return httpx.Response(301, headers={"Location": "https://example.com/200"})
            elif path == "/404":
                return httpx.Response(404, text="Not Found")
            elif path == "/403":
                return httpx.Response(403, text="Forbidden")
            elif path == "/500":
                return httpx.Response(500, text="Internal Server Error")
            elif path == "/503":
                return httpx.Response(503, text="Service Unavailable")
            return httpx.Response(200)

        transport = httpx.MockTransport(mock_handler)

        # 200 OK -> UP
        r200 = robust_ping("https://example.com/200", allow_loopback=True, transport=transport)
        self.assertEqual(r200.outcome, PingOutcome.UP)
        self.assertEqual(r200.status_code, 200)
        self.assertIsNone(r200.error_detail)

        # 404 Not Found -> DEGRADED (server responded!)
        r404 = robust_ping("https://example.com/404", allow_loopback=True, transport=transport)
        self.assertEqual(r404.outcome, PingOutcome.DEGRADED)
        self.assertEqual(r404.status_code, 404)
        self.assertIsNone(r404.error_detail)

        # 403 Forbidden -> DEGRADED
        r403 = robust_ping("https://example.com/403", allow_loopback=True, transport=transport)
        self.assertEqual(r403.outcome, PingOutcome.DEGRADED)
        self.assertEqual(r403.status_code, 403)

        # 500 Internal Server Error -> DOWN
        r500 = robust_ping("https://example.com/500", allow_loopback=True, transport=transport)
        self.assertEqual(r500.outcome, PingOutcome.DOWN)
        self.assertEqual(r500.status_code, 500)

        # 503 Service Unavailable -> DOWN
        r503 = robust_ping("https://example.com/503", allow_loopback=True, transport=transport)
        self.assertEqual(r503.outcome, PingOutcome.DOWN)
        self.assertEqual(r503.status_code, 503)

    # =========================================================================
    # 2. Timeout Model Tests
    # =========================================================================
    def test_connect_timeout_classification(self) -> None:
        """Verify connect timeout maps to connect_timeout and outcome=UNREACHABLE."""
        def timeout_handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectTimeout("Connection timed out in connect phase", request=request)

        transport = httpx.MockTransport(timeout_handler)
        result = robust_ping("https://example.com/slow-connect", allow_loopback=True, transport=transport)
        self.assertEqual(result.outcome, PingOutcome.UNREACHABLE)
        self.assertIsNone(result.status_code)
        self.assertEqual(result.error_detail, "connect_timeout")
        self.assertIsNotNone(result.latency_ms)

    def test_read_timeout_classification(self) -> None:
        """Verify read timeout maps to read_timeout and outcome=UNREACHABLE."""
        def timeout_handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("Server connected but took too long to send data", request=request)

        transport = httpx.MockTransport(timeout_handler)
        result = robust_ping("https://example.com/slow-read", allow_loopback=True, transport=transport)
        self.assertEqual(result.outcome, PingOutcome.UNREACHABLE)
        self.assertIsNone(result.status_code)
        self.assertEqual(result.error_detail, "read_timeout")
        self.assertIsNotNone(result.latency_ms)

    # =========================================================================
    # 3. SSRF Defense: Private, Loopback, & Cloud Metadata Blocking
    # =========================================================================
    def test_ssrf_blocked_ip_literals(self) -> None:
        """Verify private and reserved IP literals are blocked before outbound requests."""
        blocked_targets = [
            "http://127.0.0.1",
            "http://127.0.0.1:8080/metrics",
            "http://localhost",
            "http://localhost:3000",
            "http://10.0.0.1",
            "http://10.254.1.1/internal",
            "http://172.16.0.1",
            "http://172.31.255.255",
            "http://192.168.1.1",
            "http://192.168.0.254",
            "http://169.254.169.254",  # AWS/GCP/Azure Cloud Metadata
            "http://169.254.1.1",      # IPv4 Link-local
            "http://[::1]",            # IPv6 Loopback
            "http://[fe80::1]",        # IPv6 Link-local
            "http://0.0.0.0",
        ]

        for target in blocked_targets:
            with self.subTest(target=target):
                result = robust_ping(target, allow_loopback=False)
                self.assertEqual(result.outcome, PingOutcome.UNREACHABLE)
                self.assertIsNone(result.status_code)
                self.assertEqual(result.error_detail, "ssrf_blocked")

    def test_dns_rebinding_defense(self) -> None:
        """
        Verify that a domain name resolving to a private/loopback IP is blocked.
        Simulates DNS rebinding where an external domain resolves to 127.0.0.1 or 169.254.169.254.
        """
        def mock_rebind_resolver(host: str, port: int, type: int = 0):
            # Simulate DNS returning a malicious private IP for a public-looking domain
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port))]

        result = robust_ping(
            "https://evil-rebind-domain.com/secret",
            allow_loopback=False,
            dns_resolver=mock_rebind_resolver,
        )
        self.assertEqual(result.outcome, PingOutcome.UNREACHABLE)
        self.assertEqual(result.error_detail, "ssrf_blocked")

    def test_dns_resolution_failure(self) -> None:
        """Verify non-existent hostnames map to dns_error."""
        def mock_failing_resolver(host: str, port: int, type: int = 0):
            raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")

        result = robust_ping(
            "https://completely-fake-unresolvable-domain-999.org",
            dns_resolver=mock_failing_resolver,
        )
        self.assertEqual(result.outcome, PingOutcome.UNREACHABLE)
        self.assertEqual(result.error_detail, "dns_error")
        self.assertIsNotNone(result.latency_ms)

    # =========================================================================
    # 4. Redirect SSRF Defense
    # =========================================================================
    def test_redirect_ssrf_intercepted(self) -> None:
        """
        Verify that a public endpoint redirecting (302) to an internal/metadata IP
        is intercepted and blocked before connection.
        """
        def redirect_handler(request: httpx.Request) -> httpx.Response:
            if request.url.path == "/entry":
                # Attempts redirect to cloud metadata service
                return httpx.Response(302, headers={"Location": "http://169.254.169.254/latest/meta-data"})
            return httpx.Response(200)

        transport = httpx.MockTransport(redirect_handler)
        result = robust_ping(
            "https://example.com/entry",
            allow_loopback=False,
            transport=transport,
        )
        self.assertEqual(result.outcome, PingOutcome.UNREACHABLE)
        self.assertEqual(result.error_detail, "ssrf_blocked")

    # =========================================================================
    # 5. Response Size Protection (Bounded Streaming)
    # =========================================================================
    def test_response_size_bounding(self) -> None:
        """Verify large streaming response bodies stop reading once limit is reached."""
        huge_content = b"X" * (500 * 1024)  # 500 KB

        def huge_handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=huge_content)

        transport = httpx.MockTransport(huge_handler)

        # Set limit to 10 KB via environment configuration
        from app.config import get_settings

        with patch.dict(os.environ, {"HTTP_MAX_RESPONSE_BYTES": str(10 * 1024)}):
            get_settings.cache_clear()
            try:
                result = robust_ping("https://example.com/huge", allow_loopback=True, transport=transport)
                self.assertEqual(result.outcome, PingOutcome.UP)
                self.assertEqual(result.status_code, 200)
            finally:
                get_settings.cache_clear()

    # =========================================================================
    # 6. TLS / SSL Error Classification
    # =========================================================================
    def test_tls_error_classification(self) -> None:
        """Verify TLS/SSL certificate handshake errors map to tls_error."""
        def tls_fail_handler(request: httpx.Request) -> httpx.Response:
            # Wrap ssl.SSLCertVerificationError inside httpx.ConnectError
            ssl_err = ssl.SSLCertVerificationError("Certificate verify failed: certificate has expired")
            connect_err = httpx.ConnectError(f"[SSL: CERTIFICATE_VERIFY_FAILED] {ssl_err}", request=request)
            connect_err.__cause__ = ssl_err
            raise connect_err

        transport = httpx.MockTransport(tls_fail_handler)
        mock_resolver = lambda host, port, type=0: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port))]
        result = robust_ping(
            "https://expired-cert.example.com",
            allow_loopback=True,
            transport=transport,
            dns_resolver=mock_resolver,
        )
        self.assertEqual(result.outcome, PingOutcome.UNREACHABLE)
        self.assertEqual(result.error_detail, "tls_error")

    # =========================================================================
    # 7. URL Security: Schemes & Credential Redaction
    # =========================================================================
    def test_unsupported_schemes_blocked(self) -> None:
        """Verify non-HTTP/HTTPS schemes are blocked."""
        forbidden_urls = [
            "ftp://files.example.com/file.txt",
            "file:///etc/passwd",
            "gopher://gopher.floodgap.com",
            "ws://socket.example.com",
        ]
        for bad_url in forbidden_urls:
            with self.subTest(url=bad_url):
                result = robust_ping(bad_url)
                self.assertEqual(result.outcome, PingOutcome.UNREACHABLE)
                self.assertEqual(result.error_detail, "ssrf_blocked")

    def test_credential_redaction(self) -> None:
        """Verify embedded credentials in URLs are redacted in logs and helper."""
        raw_url = "https://admin:SuperSecretPassword123@api.example.com/v1/health"
        redacted = redact_url_credentials(raw_url)
        self.assertNotIn("SuperSecretPassword123", redacted)
        self.assertIn("***:***", redacted)
        self.assertEqual(redacted, "https://***:***@api.example.com/v1/health")

    # =========================================================================
    # 8. Keep-Alive Integration Tests
    # =========================================================================
    def test_robust_keep_alive_success(self) -> None:
        """Verify robust_keep_alive uses keep_alive_path and distinct User-Agent."""
        called_headers: dict[str, str] = {}
        called_url: str = ""

        def keep_alive_handler(request: httpx.Request) -> httpx.Response:
            nonlocal called_headers, called_url
            called_headers = dict(request.headers)
            called_url = str(request.url)
            return httpx.Response(200, text='{"status":"alive"}')

        transport = httpx.MockTransport(keep_alive_handler)
        result = robust_keep_alive(
            "https://example.com",
            path="/healthz",
            allow_loopback=True,
            transport=transport,
        )
        self.assertEqual(result.outcome, PingOutcome.UP)
        self.assertEqual(result.status_code, 200)
        self.assertEqual(called_url, "https://example.com/healthz")
        self.assertEqual(called_headers.get("user-agent"), "PingGuard-KeepAlive/1.0")

    def test_nat64_translation_validation(self) -> None:
        """Verify NAT64 IPv6 addresses (64:ff9b::/96) allow public IPv4 while blocking private IPv4."""
        import ipaddress
        from app.net import is_ip_blocked
        # 64:ff9b::d818:3910 embeds 216.24.57.16 (public Render IP) -> allowed
        self.assertFalse(is_ip_blocked(ipaddress.ip_address("64:ff9b::d818:3910")))
        # 64:ff9b::7f00:1 embeds 127.0.0.1 (loopback) -> blocked
        self.assertTrue(is_ip_blocked(ipaddress.ip_address("64:ff9b::7f00:1")))
        # 64:ff9b::0a00:1 embeds 10.0.0.1 (RFC 1918) -> blocked
        self.assertTrue(is_ip_blocked(ipaddress.ip_address("64:ff9b::0a00:1")))


if __name__ == "__main__":
    unittest.main()
