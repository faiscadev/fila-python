"""Exception hierarchy for the Fila SDK."""

from __future__ import annotations

from fila.fibp import (
    ERR_AUTH_REQUIRED,
    ERR_INTERNAL,
    ERR_MESSAGE_NOT_FOUND,
    ERR_PERMISSION_DENIED,
    ERR_QUEUE_NOT_FOUND,
)


class FilaError(Exception):
    """Base exception for all Fila SDK errors."""


class QueueNotFoundError(FilaError):
    """Raised when the specified queue does not exist."""


class MessageNotFoundError(FilaError):
    """Raised when the specified message does not exist."""


class TransportError(FilaError):
    """Raised for unexpected FIBP transport failures, preserving error code and message."""

    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"transport error (code={code}): {message}")


# Keep RPCError as an alias so existing code that catches it still works.
# New code should use TransportError.
RPCError = TransportError


class EnqueueError(FilaError):
    """Raised when an enqueue operation fails at the batch or item level.

    In ``enqueue_many()``, per-message failures are reported via
    ``EnqueueResult.error`` and do not raise this exception.  It is also used
    as a fallback for per-message enqueue failures that do not map to a more
    specific type (e.g., storage or Lua errors).
    """


def _map_enqueue_error_code(code: int, message: str) -> FilaError:
    """Map a FIBP enqueue error code to a Fila exception."""
    if code == ERR_QUEUE_NOT_FOUND:
        return QueueNotFoundError(f"enqueue: {message}")
    if code == ERR_PERMISSION_DENIED:
        return TransportError(code, f"enqueue: {message}")
    return EnqueueError(f"enqueue failed: {message}")


def _map_ack_error_code(code: int, message: str) -> FilaError:
    """Map a FIBP ack error code to a Fila exception."""
    if code == ERR_MESSAGE_NOT_FOUND:
        return MessageNotFoundError(f"ack: {message}")
    if code == ERR_PERMISSION_DENIED:
        return TransportError(code, f"ack: {message}")
    return TransportError(ERR_INTERNAL, f"ack: {message}")


def _map_nack_error_code(code: int, message: str) -> FilaError:
    """Map a FIBP nack error code to a Fila exception."""
    if code == ERR_MESSAGE_NOT_FOUND:
        return MessageNotFoundError(f"nack: {message}")
    if code == ERR_PERMISSION_DENIED:
        return TransportError(code, f"nack: {message}")
    return TransportError(ERR_INTERNAL, f"nack: {message}")


def _map_fibp_error(code: int, message: str) -> FilaError:
    """Map a generic FIBP ERROR frame to a Fila exception."""
    if code == ERR_QUEUE_NOT_FOUND:
        return QueueNotFoundError(message)
    if code == ERR_MESSAGE_NOT_FOUND:
        return MessageNotFoundError(message)
    if code in (ERR_AUTH_REQUIRED, ERR_PERMISSION_DENIED):
        return TransportError(code, message)
    return TransportError(code, message)
