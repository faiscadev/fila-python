"""Exception hierarchy for the Fila SDK."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fila.fibp.codec import ErrorFrame


class FilaError(Exception):
    """Base exception for all Fila SDK errors."""


class QueueNotFoundError(FilaError):
    """Raised when the specified queue does not exist."""


class MessageNotFoundError(FilaError):
    """Raised when the specified message does not exist."""


class QueueAlreadyExistsError(FilaError):
    """Raised when creating a queue that already exists."""


class InvalidArgumentError(FilaError):
    """Raised when an argument is invalid."""


class PermissionDeniedError(FilaError):
    """Raised when permission is denied for the operation."""


class UnauthorizedError(FilaError):
    """Raised when the client is not authenticated."""


class ForbiddenError(FilaError):
    """Raised when the client lacks permission for the operation."""


class NotLeaderError(FilaError):
    """Raised when the request was sent to a non-leader node.

    The ``leader_addr`` attribute contains the address of the current leader,
    if available.
    """

    def __init__(self, message: str, leader_addr: str | None = None) -> None:
        self.leader_addr = leader_addr
        super().__init__(message)


class ChannelFullError(FilaError):
    """Raised when a channel or buffer is full (backpressure)."""


class ResourceExhaustedError(FilaError):
    """Raised when a resource limit has been reached."""


class UnavailableError(FilaError):
    """Raised when the server is unavailable."""


class LuaError(FilaError):
    """Raised when a Lua script error occurs."""


class EnqueueError(FilaError):
    """Raised when an enqueue operation fails.

    In ``enqueue_many()``, individual per-message failures are reported via
    ``EnqueueResult.error`` and do not raise this exception. It is also used
    as a fallback for per-message enqueue failures that do not map to a more
    specific type (e.g., storage or Lua errors).
    """


class ProtocolError(FilaError):
    """Raised for unexpected protocol-level failures."""

    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"protocol error (code=0x{code:02x}): {message}")


class ApiKeyNotFoundError(FilaError):
    """Raised when the specified API key does not exist."""


class AclNotFoundError(FilaError):
    """Raised when the specified ACL entry does not exist."""


# Keep RPCError as a thin alias for backwards compatibility with existing
# callers that catch fila.RPCError.
RPCError = ProtocolError


# ---------------------------------------------------------------------------
# Error-code mapping
# ---------------------------------------------------------------------------

def _map_error_code(code: int, message: str) -> FilaError:
    """Map a FIBP error code to the appropriate exception."""
    from fila.fibp.opcodes import ErrorCode

    match code:
        case ErrorCode.QUEUE_NOT_FOUND:
            return QueueNotFoundError(message)
        case ErrorCode.MESSAGE_NOT_FOUND:
            return MessageNotFoundError(message)
        case ErrorCode.QUEUE_ALREADY_EXISTS:
            return QueueAlreadyExistsError(message)
        case ErrorCode.INVALID_ARGUMENT:
            return InvalidArgumentError(message)
        case ErrorCode.PERMISSION_DENIED:
            return PermissionDeniedError(message)
        case ErrorCode.UNAUTHENTICATED:
            return UnauthorizedError(message)
        case ErrorCode.FORBIDDEN:
            return ForbiddenError(message)
        case ErrorCode.NOT_LEADER:
            return NotLeaderError(message)
        case ErrorCode.CHANNEL_FULL:
            return ChannelFullError(message)
        case ErrorCode.RESOURCE_EXHAUSTED:
            return ResourceExhaustedError(message)
        case ErrorCode.UNAVAILABLE:
            return UnavailableError(message)
        case ErrorCode.LUA_ERROR:
            return LuaError(message)
        case ErrorCode.API_KEY_NOT_FOUND:
            return ApiKeyNotFoundError(message)
        case ErrorCode.ACL_NOT_FOUND:
            return AclNotFoundError(message)
        case ErrorCode.DEADLINE_EXCEEDED:
            return ProtocolError(code, message)
        case ErrorCode.PRECONDITION_FAILED:
            return ProtocolError(code, message)
        case ErrorCode.ABORTED:
            return ProtocolError(code, message)
        case ErrorCode.INTERNAL_ERROR:
            return ProtocolError(code, message)
        case _:
            return ProtocolError(code, message)


def _raise_from_error_frame(err: ErrorFrame) -> None:
    """Raise the appropriate exception from a decoded Error frame.

    For NotLeader errors, extracts the leader_addr from metadata.
    """
    from fila.fibp.opcodes import ErrorCode

    if err.code == ErrorCode.NOT_LEADER:
        leader_addr = err.metadata.get("leader_addr")
        raise NotLeaderError(err.message, leader_addr=leader_addr)

    raise _map_error_code(err.code, err.message)


def _map_per_item_error(code: int, context: str) -> FilaError:
    """Map a per-item error code (from EnqueueResult, AckResult, etc.)."""
    from fila.fibp.opcodes import ErrorCode

    match code:
        case ErrorCode.QUEUE_NOT_FOUND:
            return QueueNotFoundError(f"{context}: queue not found")
        case ErrorCode.MESSAGE_NOT_FOUND:
            return MessageNotFoundError(f"{context}: message not found")
        case ErrorCode.PERMISSION_DENIED:
            return PermissionDeniedError(f"{context}: permission denied")
        case _:
            return EnqueueError(f"{context}: error code 0x{code:02x}")
