"""Background batcher for opportunistic and linger-based enqueue batching."""

from __future__ import annotations

import queue
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

import grpc

from fila.errors import BatchEnqueueError, _map_enqueue_error
from fila.types import BatchEnqueueResult
from fila.v1 import service_pb2

if TYPE_CHECKING:
    from fila.v1 import service_pb2_grpc


# Sentinel that signals the batcher thread to stop.
_STOP = object()

# Maximum batch size when none is configured.
_DEFAULT_MAX_BATCH_SIZE = 1000


class _EnqueueRequest:
    """Internal envelope pairing a proto request with its result future."""

    __slots__ = ("proto", "future")

    def __init__(
        self,
        proto: service_pb2.EnqueueRequest,
        future: Future[str],
    ) -> None:
        self.proto = proto
        self.future = future


def _msg_to_consume_result(
    proto_result: Any,
) -> BatchEnqueueResult:
    """Convert a proto ``BatchEnqueueResult`` to the SDK type."""
    if proto_result.HasField("success"):
        return BatchEnqueueResult(
            message_id=proto_result.success.message_id,
            error=None,
        )
    return BatchEnqueueResult(
        message_id=None,
        error=proto_result.error,
    )


def _flush_single(
    stub: service_pb2_grpc.FilaServiceStub,
    req: _EnqueueRequest,
) -> None:
    """Send a single message via the singular Enqueue RPC.

    This preserves the specific error types (QueueNotFoundError, etc.)
    that callers of ``enqueue()`` expect.
    """
    try:
        resp = stub.Enqueue(req.proto)
        req.future.set_result(str(resp.message_id))
    except grpc.RpcError as e:
        req.future.set_exception(_map_enqueue_error(e))
    except Exception as e:
        req.future.set_exception(e)


def _flush_batch(
    stub: service_pb2_grpc.FilaServiceStub,
    batch: list[_EnqueueRequest],
) -> None:
    """Send a batch of messages via the BatchEnqueue RPC.

    On RPC-level failure, every future in the batch receives a
    ``BatchEnqueueError``. On success, each future gets either its
    message ID or a per-message error string wrapped in a
    ``BatchEnqueueError``.
    """
    try:
        resp = stub.BatchEnqueue(
            service_pb2.BatchEnqueueRequest(
                messages=[r.proto for r in batch],
            )
        )
    except grpc.RpcError as e:
        err = BatchEnqueueError(f"batch enqueue rpc failed: {e.details()}")
        for r in batch:
            r.future.set_exception(err)
        return
    except Exception as e:
        for r in batch:
            r.future.set_exception(e)
        return

    # Pair each result with its request future.
    for i, result in enumerate(resp.results):
        if i >= len(batch):
            break
        req = batch[i]
        if result.HasField("success"):
            req.future.set_result(str(result.success.message_id))
        else:
            req.future.set_exception(
                BatchEnqueueError(f"enqueue failed: {result.error}")
            )


class AutoBatcher:
    """Opportunistic batcher: drains a queue and flushes in batches.

    A background daemon thread blocks on the first message, then non-blocking
    drains any additional messages that arrived during processing and flushes
    them as a single batch via a thread pool executor.
    """

    def __init__(
        self,
        stub: service_pb2_grpc.FilaServiceStub,
        max_batch_size: int = _DEFAULT_MAX_BATCH_SIZE,
        max_workers: int = 4,
    ) -> None:
        self._stub = stub
        self._max_batch_size = max_batch_size
        self._queue: queue.Queue[_EnqueueRequest | object] = queue.Queue()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def submit(self, proto: service_pb2.EnqueueRequest) -> Future[str]:
        """Submit a message for batched enqueue. Returns a Future for the message ID."""
        fut: Future[str] = Future()
        self._queue.put(_EnqueueRequest(proto, fut))
        return fut

    def close(self, timeout: float | None = 30.0) -> None:
        """Drain pending messages and shut down the batcher.

        Blocks until all pending messages have been flushed or *timeout*
        seconds have elapsed.
        """
        self._queue.put(_STOP)
        self._thread.join(timeout=timeout)
        self._executor.shutdown(wait=True)

    def update_stub(self, stub: service_pb2_grpc.FilaServiceStub) -> None:
        """Update the gRPC stub (e.g. after leader-hint reconnect)."""
        self._stub = stub

    def _run(self) -> None:
        """Background loop: block for first item, drain rest, flush."""
        while True:
            # Block until at least one item arrives.
            first = self._queue.get()
            if first is _STOP:
                return

            assert isinstance(first, _EnqueueRequest)
            batch: list[_EnqueueRequest] = [first]

            # Non-blocking drain of any additional queued messages.
            while len(batch) < self._max_batch_size:
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    break
                if item is _STOP:
                    # Flush what we have, then stop.
                    self._flush(batch)
                    return
                assert isinstance(item, _EnqueueRequest)
                batch.append(item)

            self._flush(batch)

    def _flush(self, batch: list[_EnqueueRequest]) -> None:
        """Dispatch a batch to the executor for concurrent RPC."""
        if len(batch) == 1:
            # Single-item optimization: use singular Enqueue RPC.
            self._executor.submit(_flush_single, self._stub, batch[0])
        else:
            self._executor.submit(_flush_batch, self._stub, batch)


class LingerBatcher:
    """Timer-based batcher: holds messages for up to linger_ms or batch_size.

    A background daemon thread accumulates messages and flushes when either
    the batch reaches ``batch_size`` or ``linger_ms`` milliseconds have
    elapsed since the first message in the current batch arrived.
    """

    def __init__(
        self,
        stub: service_pb2_grpc.FilaServiceStub,
        linger_ms: float,
        batch_size: int,
        max_workers: int = 4,
    ) -> None:
        self._stub = stub
        self._linger_s = linger_ms / 1000.0
        self._batch_size = batch_size
        self._queue: queue.Queue[_EnqueueRequest | object] = queue.Queue()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def submit(self, proto: service_pb2.EnqueueRequest) -> Future[str]:
        """Submit a message for batched enqueue. Returns a Future for the message ID."""
        fut: Future[str] = Future()
        self._queue.put(_EnqueueRequest(proto, fut))
        return fut

    def close(self, timeout: float | None = 30.0) -> None:
        """Drain pending messages and shut down the batcher."""
        self._queue.put(_STOP)
        self._thread.join(timeout=timeout)
        self._executor.shutdown(wait=True)

    def update_stub(self, stub: service_pb2_grpc.FilaServiceStub) -> None:
        """Update the gRPC stub (e.g. after leader-hint reconnect)."""
        self._stub = stub

    def _run(self) -> None:
        """Background loop: accumulate up to batch_size or linger timeout."""
        import time

        while True:
            # Block until at least one item arrives.
            first = self._queue.get()
            if first is _STOP:
                return

            assert isinstance(first, _EnqueueRequest)
            batch: list[_EnqueueRequest] = [first]

            # Track wall-clock deadline from when first message arrived.
            deadline = time.monotonic() + self._linger_s

            # Accumulate more items until batch_size or linger timeout.
            while len(batch) < self._batch_size:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    item = self._queue.get(timeout=remaining)
                except queue.Empty:
                    break
                if item is _STOP:
                    self._flush(batch)
                    return
                assert isinstance(item, _EnqueueRequest)
                batch.append(item)

            self._flush(batch)

    def _flush(self, batch: list[_EnqueueRequest]) -> None:
        """Dispatch a batch to the executor for concurrent RPC."""
        if len(batch) == 1:
            self._executor.submit(_flush_single, self._stub, batch[0])
        else:
            self._executor.submit(_flush_batch, self._stub, batch)
