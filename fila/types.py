"""Public types for the Fila SDK."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto


@dataclass(frozen=True)
class ConsumeMessage:
    """A message received from the broker via a consume stream."""

    id: str
    headers: dict[str, str]
    payload: bytes
    fairness_key: str
    attempt_count: int
    queue: str


@dataclass(frozen=True)
class BatchEnqueueResult:
    """Result for a single message within a batch enqueue operation.

    Exactly one of ``message_id`` or ``error`` is set.
    """

    message_id: str | None
    error: str | None

    @property
    def is_success(self) -> bool:
        """Return True if this message was enqueued successfully."""
        return self.message_id is not None


class BatchMode(Enum):
    """Controls how ``enqueue()`` routes messages to the broker.

    - ``AUTO``: Opportunistic batching via a background thread. At low load
      messages are sent individually; at high load they cluster into batches.
      This is the default.
    - ``DISABLED``: No batching. Each ``enqueue()`` call is a direct RPC.
    """

    AUTO = auto()
    DISABLED = auto()


@dataclass(frozen=True)
class Linger:
    """Timer-based forced batching mode.

    Messages are held for up to ``linger_ms`` milliseconds or until
    ``batch_size`` messages accumulate, whichever comes first.

    Args:
        linger_ms: Maximum time to hold a message before flushing (milliseconds).
        batch_size: Maximum number of messages per batch.
    """

    linger_ms: float
    batch_size: int
