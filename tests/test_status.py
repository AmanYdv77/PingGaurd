"""
Unit tests for outcome to status mapping in app.status.
"""

import pytest
from app.enums import MonitorStatus, PingOutcome
from app.status import outcome_to_status


def test_outcome_to_status_all_members_covered():
    """Verify that every single PingOutcome enum member maps to a valid MonitorStatus."""
    for outcome in PingOutcome:
        mapped_status = outcome_to_status(outcome)
        assert isinstance(mapped_status, MonitorStatus)
        assert mapped_status in {MonitorStatus.UP, MonitorStatus.DEGRADED, MonitorStatus.DOWN}


def test_outcome_to_status_explicit_mappings():
    """Verify explicit mapping rules."""
    assert outcome_to_status(PingOutcome.UP) == MonitorStatus.UP
    assert outcome_to_status(PingOutcome.DEGRADED) == MonitorStatus.DEGRADED
    assert outcome_to_status(PingOutcome.DOWN) == MonitorStatus.DOWN
    assert outcome_to_status(PingOutcome.UNREACHABLE) == MonitorStatus.DOWN


def test_outcome_to_status_unhandled_raises():
    """Verify an unexpected/unhandled outcome raises ValueError."""
    with pytest.raises(ValueError):
        outcome_to_status("not-an-outcome")  # type: ignore[arg-type]  # Deliberately invalid type to test runtime rejection


@pytest.mark.parametrize(
    "code,expected_outcome",
    [
        (199, PingOutcome.UP),
        (200, PingOutcome.UP),
        (399, PingOutcome.UP),
        (400, PingOutcome.DEGRADED),
        (499, PingOutcome.DEGRADED),
        (500, PingOutcome.DOWN),
        (599, PingOutcome.DOWN),
    ],
)
def test_classify_status_code_boundaries(code: int, expected_outcome: PingOutcome):
    """Verify HTTP status code classification against required boundary codes."""
    from app.status import classify_status_code

    assert classify_status_code(code) == expected_outcome


@pytest.mark.parametrize("invalid_code", [99, 600, -1, 1000])
def test_classify_status_code_invalid_raises(invalid_code: int):
    """Verify non-standard HTTP status codes raise ValueError."""
    from app.status import classify_status_code

    with pytest.raises(ValueError):
        classify_status_code(invalid_code)
