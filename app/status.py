"""
Outcome to MonitorStatus mapping module.

Provides a centralised, exhaustive mapping from low-level network probe outcomes
(PingOutcome) to user-facing monitor lifecycle statuses (MonitorStatus).
"""

from app.enums import MonitorStatus, PingOutcome

OUTCOME_TO_STATUS: dict[PingOutcome, MonitorStatus] = {
    PingOutcome.UP: MonitorStatus.UP,
    PingOutcome.DEGRADED: MonitorStatus.DEGRADED,
    PingOutcome.DOWN: MonitorStatus.DOWN,
    PingOutcome.UNREACHABLE: MonitorStatus.DOWN,
}


def outcome_to_status(outcome: PingOutcome) -> MonitorStatus:
    """
    Exhaustively map a PingOutcome to its canonical MonitorStatus.

    Mapping Rules:
    - UP -> UP
    - DEGRADED -> DEGRADED
    - DOWN -> DOWN
    - UNREACHABLE -> DOWN (Network failure/unreachable maps to monitor DOWN)
    """
    if outcome not in OUTCOME_TO_STATUS:
        raise ValueError(f"Unhandled PingOutcome: {outcome}")
    return OUTCOME_TO_STATUS[outcome]


# DESIGN: HTTP status code classification mapping for probe outcomes.
# The owner can document their own reasoning here.
def classify_status_code(code: int) -> PingOutcome:
    """
    Classify an HTTP integer status code into an operational PingOutcome.

    Rule Table:
    | Status Code Range | Classification       | Description                                  |
    |-------------------|----------------------|----------------------------------------------|
    | 100 - 399         | PingOutcome.UP       | Successful responses and standard redirects. |
    | 400 - 499         | PingOutcome.DEGRADED | Client errors indicating degraded service.   |
    | 500 - 599         | PingOutcome.DOWN     | Server-side faults indicating target outage. |

    Rationale: 1xx-3xx indicates reachable service responding normally, 4xx implies target reached
    but returning client-level issues (degraded), while 5xx indicates internal server failure (down).

    Raises:
        ValueError: If code is not within the standard HTTP range [100, 599].
    """
    if 100 <= code < 400:
        return PingOutcome.UP
    elif 400 <= code < 500:
        return PingOutcome.DEGRADED
    elif 500 <= code < 600:
        return PingOutcome.DOWN
    else:
        raise ValueError(f"Invalid or unsupported HTTP status code: {code}")
