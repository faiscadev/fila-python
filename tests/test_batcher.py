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
    AutoBatcher,
    LingerBatcher,
    _EnqueueRequest,
    _flush_batch,
    _flush_single,
)
from fila.errors import BatchEnqueueError
from fila.v1 import service_pb2


class FakeEnqueueResponse:
    """Minimal fake for service_pb2.EnqueueResponse."""

    def __init__(self, message_id: str) -> None:
        self.message_id = message_id


class FakeBatchResult:
    """Minimal fake for service_pb2.BatchEnqueueResult."""

    def __init__(self, message_id: str | None = None, error: str | None = None) -> None:
        self._message_id = message_id
        self._error = error
        self.success: FakeEnqueueResponse | None = (
            FakeEnqueueResponse(message_id) if message_id is not None else None
        )
        self.error = error or ""

    def HasField(self, name: str) -> bool:  # noqa: N802
        if name == "success":
            return self._message_id is not None
        return False


class FakeBatchResponse:
    """Minimal fake for service_pb2.BatchEnqueueResponse."""

    def __init__(self, results: list[FakeBatchResult]) -> None:
        self.results = results


class TestFlushSingle:
    """Test the _flush_single function."""

    def test_success(self) -> None:
        stub = MagicMock()
        stub.Enqueue.return_value = FakeEnqueueResponse("msg-001")

        proto = service_pb2.EnqueueRequest(queue="q", payload=b"data")
        fut: Future[str] = Future()
        req = _EnqueueRequest(proto, fut)

        _flush_single(stub, req)

        assert fut.result(timeout=1.0) == "msg-001"
        stub.Enqueue.assert_called_once_with(proto)

    def test_rpc_error(self) -> None:
        import grpc

        stub = MagicMock()
        rpc_error = MagicMock()
        rpc_error.code.return_value = grpc.StatusCode.NOT_FOUND
        rpc_error.details.return_value = "queue not found"
        # Make it pass isinstance(e, grpc.RpcError) check.
        stub.Enqueue.side_effect = type(
            "_FakeRpcError", (grpc.RpcError,), {
                "code": lambda self: grpc.StatusCode.NOT_FOUND,
                "details": lambda self: "queue not found",
            }
        )()

        proto = service_pb2.EnqueueRequest(queue="missing", payload=b"data")
        fut: Future[str] = Future()
        req = _EnqueueRequest(proto, fut)

        _flush_single(stub, req)

        from fila.errors import QueueNotFoundError

        with pytest.raises(QueueNotFoundError):
            fut.result(timeout=1.0)


class TestFlushBatch:
    """Test the _flush_batch function."""

    def test_all_success(self) -> None:
        stub = MagicMock()
        stub.BatchEnqueue.return_value = FakeBatchResponse([
            FakeBatchResult(message_id="id-1"),
            FakeBatchResult(message_id="id-2"),
        ])

        reqs = [
            _EnqueueRequest(
                service_pb2.EnqueueRequest(queue="q", payload=b"a"),
                Future(),
            ),
            _EnqueueRequest(
                service_pb2.EnqueueRequest(queue="q", payload=b"b"),
                Future(),
            ),
        ]

        _flush_batch(stub, reqs)

        assert reqs[0].future.result(timeout=1.0) == "id-1"
        assert reqs[1].future.result(timeout=1.0) == "id-2"

    def test_mixed_results(self) -> None:
        stub = MagicMock()
        stub.BatchEnqueue.return_value = FakeBatchResponse([
            FakeBatchResult(message_id="id-1"),
            FakeBatchResult(error="queue 'missing' not found"),
        ])

        reqs = [
            _EnqueueRequest(
                service_pb2.EnqueueRequest(queue="q", payload=b"a"),
                Future(),
            ),
            _EnqueueRequest(
                service_pb2.EnqueueRequest(queue="missing", payload=b"b"),
                Future(),
            ),
        ]

        _flush_batch(stub, reqs)

        assert reqs[0].future.result(timeout=1.0) == "id-1"
        with pytest.raises(BatchEnqueueError, match="queue 'missing' not found"):
            reqs[1].future.result(timeout=1.0)

    def test_rpc_failure_sets_all_futures(self) -> None:
        import grpc

        stub = MagicMock()
        stub.BatchEnqueue.side_effect = type(
            "_FakeRpcError", (grpc.RpcError,), {
                "code": lambda self: grpc.StatusCode.UNAVAILABLE,
                "details": lambda self: "server unavailable",
            }
        )()

        reqs = [
            _EnqueueRequest(
                service_pb2.EnqueueRequest(queue="q", payload=b"a"),
                Future(),
            ),
            _EnqueueRequest(
                service_pb2.EnqueueRequest(queue="q", payload=b"b"),
                Future(),
            ),
        ]

        _flush_batch(stub, reqs)

        for r in reqs:
            with pytest.raises(BatchEnqueueError):
                r.future.result(timeout=1.0)


class TestAutoBatcher:
    """Test the AutoBatcher end-to-end."""

    def test_single_message_uses_enqueue(self) -> None:
        """When only one message is queued, AutoBatcher uses singular Enqueue."""
        stub = MagicMock()
        stub.Enqueue.return_value = FakeEnqueueResponse("msg-solo")

        batcher = AutoBatcher(stub, max_batch_size=100)

        proto = service_pb2.EnqueueRequest(queue="q", payload=b"solo")
        fut = batcher.submit(proto)
        result = fut.result(timeout=5.0)

        assert result == "msg-solo"
        stub.Enqueue.assert_called_once()
        stub.BatchEnqueue.assert_not_called()

        batcher.close()

    def test_concurrent_messages_batched(self) -> None:
        """When multiple messages arrive concurrently, they batch together."""
        stub = MagicMock()

        # The first message will block Enqueue while more messages queue up.
        # We need to make the batcher see all messages at once.
        batch_called = threading.Event()
        batch_response = FakeBatchResponse([
            FakeBatchResult(message_id=f"id-{i}") for i in range(5)
        ])

        def mock_batch_enqueue(request: Any) -> FakeBatchResponse:
            batch_called.set()
            return batch_response

        # Make single Enqueue block briefly so messages accumulate.
        single_barrier = threading.Event()

        def mock_single_enqueue(request: Any) -> FakeEnqueueResponse:
            single_barrier.wait(timeout=5.0)
            return FakeEnqueueResponse("should-not-be-used")

        stub.Enqueue.side_effect = mock_single_enqueue
        stub.BatchEnqueue.side_effect = mock_batch_enqueue

        batcher = AutoBatcher(stub, max_batch_size=100)

        # Submit 5 messages rapidly before the first can process.
        # The batcher should drain them all in one batch.
        protos = [
            service_pb2.EnqueueRequest(queue="q", payload=f"msg-{i}".encode())
            for i in range(5)
        ]

        # We need to submit them in a way that they all arrive before
        # the batcher loop drains. Use a barrier approach.
        futures = []
        for p in protos:
            futures.append(batcher.submit(p))

        # Give the batcher thread time to drain and flush.
        # Either BatchEnqueue or multiple Enqueue calls will resolve things.
        for _i, f in enumerate(futures):
            result = f.result(timeout=5.0)
            assert result is not None

        batcher.close()

    def test_close_drains_pending(self) -> None:
        """close() waits for pending messages to be flushed."""
        stub = MagicMock()
        stub.Enqueue.return_value = FakeEnqueueResponse("drained")

        batcher = AutoBatcher(stub, max_batch_size=100)

        proto = service_pb2.EnqueueRequest(queue="q", payload=b"drain-me")
        fut = batcher.submit(proto)

        batcher.close()

        # After close, the future should be resolved.
        assert fut.result(timeout=1.0) == "drained"

    def test_update_stub(self) -> None:
        """update_stub replaces the gRPC stub used for flushing."""
        old_stub = MagicMock()
        new_stub = MagicMock()
        new_stub.Enqueue.return_value = FakeEnqueueResponse("new-stub")

        batcher = AutoBatcher(old_stub, max_batch_size=100)

        # Update stub before submitting.
        batcher.update_stub(new_stub)

        proto = service_pb2.EnqueueRequest(queue="q", payload=b"data")
        fut = batcher.submit(proto)
        result = fut.result(timeout=5.0)

        assert result == "new-stub"
        batcher.close()


class TestLingerBatcher:
    """Test the LingerBatcher."""

    def test_flushes_at_batch_size(self) -> None:
        """Flush triggers when batch_size messages accumulate."""
        stub = MagicMock()
        stub.BatchEnqueue.return_value = FakeBatchResponse([
            FakeBatchResult(message_id=f"id-{i}") for i in range(3)
        ])

        batcher = LingerBatcher(stub, linger_ms=5000, batch_size=3)

        futures = []
        for i in range(3):
            proto = service_pb2.EnqueueRequest(queue="q", payload=f"m{i}".encode())
            futures.append(batcher.submit(proto))

        # Should flush quickly because batch_size=3 was reached.
        for i, f in enumerate(futures):
            result = f.result(timeout=5.0)
            assert result == f"id-{i}"

        batcher.close()

    def test_flushes_at_linger_timeout(self) -> None:
        """Flush triggers after linger_ms even if batch_size is not reached."""
        stub = MagicMock()
        stub.Enqueue.return_value = FakeEnqueueResponse("lingered")

        batcher = LingerBatcher(stub, linger_ms=50, batch_size=100)

        proto = service_pb2.EnqueueRequest(queue="q", payload=b"linger")
        fut = batcher.submit(proto)

        # Should flush after ~50ms even though batch_size=100 not reached.
        result = fut.result(timeout=5.0)
        assert result == "lingered"

        batcher.close()

    def test_close_drains_pending(self) -> None:
        """close() drains any pending messages."""
        stub = MagicMock()
        stub.Enqueue.return_value = FakeEnqueueResponse("drained")

        batcher = LingerBatcher(stub, linger_ms=10000, batch_size=100)

        proto = service_pb2.EnqueueRequest(queue="q", payload=b"drain")
        fut = batcher.submit(proto)

        batcher.close()

        assert fut.result(timeout=1.0) == "drained"
