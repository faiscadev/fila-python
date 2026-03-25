"""Exception hierarchy for the Fila SDK."""

from __future__ import annotations

import grpc


class FilaError(Exception):
    """Base exception for all Fila SDK errors."""


class QueueNotFoundError(FilaError):
    """Raised when the specified queue does not exist."""


class MessageNotFoundError(FilaError):
    """Raised when the specified message does not exist."""


class RPCError(FilaError):
    """Raised for unexpected gRPC failures, preserving status code and message."""

    def __init__(self, code: grpc.StatusCode, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"rpc error (code = {code.name}): {message}")


class EnqueueError(FilaError):
    """Raised when an enqueue fails at the RPC level.

    Individual per-message failures are reported via ``EnqueueResult.error``
    and do not raise this exception. This is raised only when the entire
    RPC fails (e.g., network error, server unavailable).
    """


def _map_enqueue_result_error(code: int, message: str) -> FilaError:
    """Map a per-message EnqueueErrorCode to a Fila exception.

    Used when the unified Enqueue RPC succeeds at the transport level but
    returns a per-message error result (e.g., queue not found for one of
    the messages in the batch).
    """
    from fila.v1 import service_pb2

    if code == service_pb2.ENQUEUE_ERROR_CODE_QUEUE_NOT_FOUND:
        return QueueNotFoundError(f"enqueue: {message}")
    if code == service_pb2.ENQUEUE_ERROR_CODE_PERMISSION_DENIED:
        return RPCError(grpc.StatusCode.PERMISSION_DENIED, f"enqueue: {message}")
    return EnqueueError(f"enqueue failed: {message}")


def _map_enqueue_error(err: grpc.RpcError) -> FilaError:
    """Map a gRPC error from an enqueue call to a Fila exception."""
    code = err.code()
    if code == grpc.StatusCode.NOT_FOUND:
        return QueueNotFoundError(f"enqueue: {err.details()}")
    return RPCError(code, err.details() or "")


def _map_consume_error(err: grpc.RpcError) -> FilaError:
    """Map a gRPC error from a consume call to a Fila exception."""
    code = err.code()
    if code == grpc.StatusCode.NOT_FOUND:
        return QueueNotFoundError(f"consume: {err.details()}")
    return RPCError(code, err.details() or "")


def _map_ack_error(err: grpc.RpcError) -> FilaError:
    """Map a gRPC error from an ack call to a Fila exception."""
    code = err.code()
    if code == grpc.StatusCode.NOT_FOUND:
        return MessageNotFoundError(f"ack: {err.details()}")
    return RPCError(code, err.details() or "")


def _map_nack_error(err: grpc.RpcError) -> FilaError:
    """Map a gRPC error from a nack call to a Fila exception."""
    code = err.code()
    if code == grpc.StatusCode.NOT_FOUND:
        return MessageNotFoundError(f"nack: {err.details()}")
    return RPCError(code, err.details() or "")
