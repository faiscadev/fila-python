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
