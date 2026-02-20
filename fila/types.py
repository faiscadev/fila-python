"""Public types for the Fila SDK."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ConsumeMessage:
    """A message received from the broker via a consume stream."""

    id: str
    headers: dict[str, str]
    payload: bytes
    fairness_key: str
    attempt_count: int
    queue: str
