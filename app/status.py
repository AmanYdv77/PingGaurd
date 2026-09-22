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
