"""Unit tests for the batcher module.

These tests use mock stubs and do not require a running fila-server.
"""

from __future__ import annotations

import threading
from concurrent.futures import Future
from typing import Any
from unittest.mock import MagicMock

import pytest

from fila.batcher import (
    AutoAccumulator,
    LingerAccumulator,
    _EnqueueItem,
    _flush_many,
    _flush_single,
)
from fila.errors import EnqueueError
from fila.v1 import service_pb2


class FakeEnqueueResult:
    """Minimal fake for service_pb2.EnqueueResult."""

    def __init__(self, message_id: str | None = None, error_msg: str | None = None) -> None:
        self._message_id = message_id
        self._error_msg = error_msg
        self.message_id = message_id or ""
        self.error = MagicMock()
        self.error.message = error_msg or ""

    def WhichOneof(self, name: str) -> str | None:  # noqa: N802
        if name == "result":
            if self._message_id is not None:
                return "message_id"
            return "error"
        return None


class FakeEnqueueResponse:
    """Minimal fake for service_pb2.EnqueueResponse."""

    def __init__(self, results: list[FakeEnqueueResult]) -> None:
        self.results = results


class TestFlushSingle:
    """Test the _flush_single function."""

    def test_success(self) -> None:
        stub = MagicMock()
        stub.Enqueue.return_value = FakeEnqueueResponse([
            FakeEnqueueResult(message_id="msg-001"),
        ])

        proto = service_pb2.EnqueueMessage(queue="q", payload=b"data")
        fut: Future[str] = Future()
        req = _EnqueueItem(proto, fut)

        _flush_single(stub, req)

        assert fut.result(timeout=1.0) == "msg-001"
        stub.Enqueue.assert_called_once()

    def test_rpc_error(self) -> None:
        import grpc

        stub = MagicMock()
        stub.Enqueue.side_effect = type(
            "_FakeRpcError", (grpc.RpcError,), {
                "code": lambda self: grpc.StatusCode.NOT_FOUND,
                "details": lambda self: "queue not found",
            }
        )()

        proto = service_pb2.EnqueueMessage(queue="missing", payload=b"data")
        fut: Future[str] = Future()
        req = _EnqueueItem(proto, fut)

        _flush_single(stub, req)

        from fila.errors import QueueNotFoundError

        with pytest.raises(QueueNotFoundError):
            fut.result(timeout=1.0)


class TestFlushMany:
    """Test the _flush_many function."""

    def test_all_success(self) -> None:
        stub = MagicMock()
        stub.Enqueue.return_value = FakeEnqueueResponse([
            FakeEnqueueResult(message_id="id-1"),
            FakeEnqueueResult(message_id="id-2"),
        ])

        items = [
            _EnqueueItem(
                service_pb2.EnqueueMessage(queue="q", payload=b"a"),
                Future(),
            ),
            _EnqueueItem(
                service_pb2.EnqueueMessage(queue="q", payload=b"b"),
                Future(),
            ),
        ]

        _flush_many(stub, items)

        assert items[0].future.result(timeout=1.0) == "id-1"
        assert items[1].future.result(timeout=1.0) == "id-2"

    def test_mixed_results(self) -> None:
        stub = MagicMock()
        stub.Enqueue.return_value = FakeEnqueueResponse([
            FakeEnqueueResult(message_id="id-1"),
            FakeEnqueueResult(error_msg="queue 'missing' not found"),
        ])

        items = [
            _EnqueueItem(
                service_pb2.EnqueueMessage(queue="q", payload=b"a"),
                Future(),
            ),
            _EnqueueItem(
                service_pb2.EnqueueMessage(queue="missing", payload=b"b"),
                Future(),
            ),
        ]

        _flush_many(stub, items)

        assert items[0].future.result(timeout=1.0) == "id-1"
        with pytest.raises(EnqueueError, match="queue 'missing' not found"):
            items[1].future.result(timeout=1.0)

    def test_rpc_failure_sets_all_futures(self) -> None:
        import grpc

        stub = MagicMock()
        stub.Enqueue.side_effect = type(
            "_FakeRpcError", (grpc.RpcError,), {
                "code": lambda self: grpc.StatusCode.UNAVAILABLE,
                "details": lambda self: "server unavailable",
            }
        )()

        items = [
            _EnqueueItem(
                service_pb2.EnqueueMessage(queue="q", payload=b"a"),
                Future(),
            ),
            _EnqueueItem(
                service_pb2.EnqueueMessage(queue="q", payload=b"b"),
                Future(),
            ),
        ]

        _flush_many(stub, items)

        for item in items:
            with pytest.raises(EnqueueError):
                item.future.result(timeout=1.0)


class TestAutoAccumulator:
    """Test the AutoAccumulator end-to-end."""

    def test_single_message_uses_enqueue(self) -> None:
        """When only one message is queued, AutoAccumulator uses Enqueue with one message."""
        stub = MagicMock()
        stub.Enqueue.return_value = FakeEnqueueResponse([
            FakeEnqueueResult(message_id="msg-solo"),
        ])

        accumulator = AutoAccumulator(stub, max_messages=100)

        proto = service_pb2.EnqueueMessage(queue="q", payload=b"solo")
        fut = accumulator.submit(proto)
        result = fut.result(timeout=5.0)

        assert result == "msg-solo"
        stub.Enqueue.assert_called_once()

        accumulator.close()

    def test_concurrent_messages_accumulated(self) -> None:
        """When multiple messages arrive concurrently, they accumulate together."""
        stub = MagicMock()

        enqueue_response = FakeEnqueueResponse([
            FakeEnqueueResult(message_id=f"id-{i}") for i in range(5)
        ])

        def mock_enqueue(request: Any) -> FakeEnqueueResponse:
            return enqueue_response

        stub.Enqueue.side_effect = mock_enqueue

        accumulator = AutoAccumulator(stub, max_messages=100)

        # Submit 5 messages rapidly.
        protos = [
            service_pb2.EnqueueMessage(queue="q", payload=f"msg-{i}".encode())
            for i in range(5)
        ]

        futures = []
        for p in protos:
            futures.append(accumulator.submit(p))

        # All futures should resolve.
        for _i, f in enumerate(futures):
            result = f.result(timeout=5.0)
            assert result is not None

        accumulator.close()

    def test_close_drains_pending(self) -> None:
        """close() waits for pending messages to be flushed."""
        stub = MagicMock()
        stub.Enqueue.return_value = FakeEnqueueResponse([
            FakeEnqueueResult(message_id="drained"),
        ])

        accumulator = AutoAccumulator(stub, max_messages=100)

        proto = service_pb2.EnqueueMessage(queue="q", payload=b"drain-me")
        fut = accumulator.submit(proto)

        accumulator.close()

        # After close, the future should be resolved.
        assert fut.result(timeout=1.0) == "drained"

    def test_update_stub(self) -> None:
        """update_stub replaces the gRPC stub used for flushing."""
        old_stub = MagicMock()
        new_stub = MagicMock()
        new_stub.Enqueue.return_value = FakeEnqueueResponse([
            FakeEnqueueResult(message_id="new-stub"),
        ])

        accumulator = AutoAccumulator(old_stub, max_messages=100)

        # Update stub before submitting.
        accumulator.update_stub(new_stub)

        proto = service_pb2.EnqueueMessage(queue="q", payload=b"data")
        fut = accumulator.submit(proto)
        result = fut.result(timeout=5.0)

        assert result == "new-stub"
        accumulator.close()


class TestLingerAccumulator:
    """Test the LingerAccumulator."""

    def test_flushes_at_max_messages(self) -> None:
        """Flush triggers when max_messages messages accumulate."""
        stub = MagicMock()
        stub.Enqueue.return_value = FakeEnqueueResponse([
            FakeEnqueueResult(message_id=f"id-{i}") for i in range(3)
        ])

        accumulator = LingerAccumulator(stub, linger_ms=5000, max_messages=3)

        futures = []
        for i in range(3):
            proto = service_pb2.EnqueueMessage(queue="q", payload=f"m{i}".encode())
            futures.append(accumulator.submit(proto))

        # Should flush quickly because max_messages=3 was reached.
        for i, f in enumerate(futures):
            result = f.result(timeout=5.0)
            assert result == f"id-{i}"

        accumulator.close()

    def test_flushes_at_linger_timeout(self) -> None:
        """Flush triggers after linger_ms even if max_messages is not reached."""
        stub = MagicMock()
        stub.Enqueue.return_value = FakeEnqueueResponse([
            FakeEnqueueResult(message_id="lingered"),
        ])

        accumulator = LingerAccumulator(stub, linger_ms=50, max_messages=100)

        proto = service_pb2.EnqueueMessage(queue="q", payload=b"linger")
        fut = accumulator.submit(proto)

        # Should flush after ~50ms even though max_messages=100 not reached.
        result = fut.result(timeout=5.0)
        assert result == "lingered"

        accumulator.close()

    def test_close_drains_pending(self) -> None:
        """close() drains any pending messages."""
        stub = MagicMock()
        stub.Enqueue.return_value = FakeEnqueueResponse([
            FakeEnqueueResult(message_id="drained"),
        ])

        accumulator = LingerAccumulator(stub, linger_ms=10000, max_messages=100)

        proto = service_pb2.EnqueueMessage(queue="q", payload=b"drain")
        fut = accumulator.submit(proto)

        accumulator.close()

        assert fut.result(timeout=1.0) == "drained"
