"""Public types for the Fila SDK."""

from __future__ import annotations

from dataclasses import dataclass, field
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
    weight: int = 0
    throttle_keys: list[str] | None = None
    enqueued_at: int = 0
    leased_at: int = 0


@dataclass(frozen=True)
class EnqueueResult:
    """Result for a single message within an enqueue operation.

    Exactly one of ``message_id`` or ``error`` is set.
    """

    message_id: str | None
    error: str | None

    @property
    def is_success(self) -> bool:
        """Return True if this message was enqueued successfully."""
        return self.message_id is not None


class AccumulatorMode(Enum):
    """Controls how ``enqueue()`` routes messages to the broker.

    - ``AUTO``: Opportunistic accumulation via a background thread. At low load
      messages are sent individually; at high load they cluster into batches.
      This is the default.
    - ``DISABLED``: No accumulation. Each ``enqueue()`` call is a direct RPC.
    """

    AUTO = auto()
    DISABLED = auto()


@dataclass(frozen=True)
class Linger:
    """Timer-based forced accumulation mode.

    Messages are held for up to ``linger_ms`` milliseconds or until
    ``max_messages`` messages accumulate, whichever comes first.

    Args:
        linger_ms: Maximum time to hold a message before flushing (milliseconds).
        max_messages: Maximum number of messages per flush.
    """

    linger_ms: float
    max_messages: int


@dataclass(frozen=True)
class CreateApiKeyResult:
    """Result of creating an API key."""

    key_id: str
    raw_key: str
    is_superadmin: bool = False


@dataclass(frozen=True)
class ApiKeyInfo:
    """Summary information about an API key."""

    key_id: str
    name: str
    created_at: int
    expires_at: int = 0
    is_superadmin: bool = False


@dataclass(frozen=True)
class AclPermission:
    """A single ACL permission."""

    kind: str
    pattern: str


@dataclass(frozen=True)
class AclEntry:
    """ACL entry for an API key."""

    key_id: str
    is_superadmin: bool
    permissions: list[AclPermission] = field(default_factory=list)


@dataclass(frozen=True)
class FairnessKeyStat:
    """Per-fairness-key statistics."""

    key: str
    pending_count: int
    current_deficit: int
    weight: int


@dataclass(frozen=True)
class ThrottleKeyStat:
    """Per-throttle-key statistics."""

    key: str
    tokens: float
    rate_per_second: float
    burst: float


@dataclass(frozen=True)
class StatsResult:
    """Queue statistics."""

    depth: int
    in_flight: int
    active_fairness_keys: int
    active_consumers: int
    quantum: int
    leader_node_id: int = 0
    replication_count: int = 0
    per_key_stats: list[FairnessKeyStat] = field(default_factory=list)
    per_throttle_stats: list[ThrottleKeyStat] = field(default_factory=list)


@dataclass(frozen=True)
class QueueInfo:
    """Summary information about a queue."""

    name: str
    depth: int
    in_flight: int
    active_consumers: int
    leader_node_id: int = 0
