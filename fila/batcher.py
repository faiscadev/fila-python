"""Background accumulator for opportunistic and linger-based enqueue accumulation."""

from __future__ import annotations

import queue
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import TYPE_CHECKING

import grpc

from fila.errors import EnqueueError, _map_enqueue_error, _map_enqueue_result_error
from fila.v1 import service_pb2

if TYPE_CHECKING:
    from fila.v1 import service_pb2_grpc


# Sentinel that signals the accumulator thread to stop.
_STOP = object()

# Maximum number of messages per flush when none is configured.
_DEFAULT_MAX_MESSAGES = 1000


class _EnqueueItem:
    """Internal envelope pairing a proto EnqueueMessage with its result future."""

    __slots__ = ("proto", "future")

    def __init__(
        self,
        proto: service_pb2.EnqueueMessage,
        future: Future[str],
    ) -> None:
        self.proto = proto
        self.future = future


def _flush_single(
    stub: service_pb2_grpc.FilaServiceStub,
    req: _EnqueueItem,
) -> None:
    """Send a single message via the unified Enqueue RPC.

    This preserves the specific error types (QueueNotFoundError, etc.)
    that callers of ``enqueue()`` expect.
    """
    try:
        resp = stub.Enqueue(
            service_pb2.EnqueueRequest(messages=[req.proto])
        )
        result = resp.results[0]
        which = result.WhichOneof("result")
        if which == "message_id":
            req.future.set_result(str(result.message_id))
        else:
            req.future.set_exception(
                _map_enqueue_result_error(result.error.code, result.error.message)
            )
    except grpc.RpcError as e:
        req.future.set_exception(_map_enqueue_error(e))
    except Exception as e:
        req.future.set_exception(e)


def _flush_many(
    stub: service_pb2_grpc.FilaServiceStub,
    items: list[_EnqueueItem],
) -> None:
    """Send multiple messages via the unified Enqueue RPC.

    On RPC-level failure, every future in the batch receives an
    ``EnqueueError``. On success, each future gets either its
    message ID or a per-message error string wrapped in an
    ``EnqueueError``.
    """
    try:
        resp = stub.Enqueue(
            service_pb2.EnqueueRequest(
                messages=[item.proto for item in items],
            )
        )
    except grpc.RpcError as e:
        err = EnqueueError(f"enqueue rpc failed: {e.details()}")
        for item in items:
            item.future.set_exception(err)
        return
    except Exception as e:
        for item in items:
            item.future.set_exception(e)
        return

    # Pair each result with its request future.
    for i, result in enumerate(resp.results):
        if i >= len(items):
            break
        item = items[i]
        which = result.WhichOneof("result")
        if which == "message_id":
            item.future.set_result(str(result.message_id))
        else:
            item.future.set_exception(
                _map_enqueue_result_error(result.error.code, result.error.message)
            )


class AutoAccumulator:
    """Opportunistic accumulator: drains a queue and flushes in batches.

    A background daemon thread blocks on the first message, then non-blocking
    drains any additional messages that arrived during processing and flushes
    them as a single Enqueue RPC via a thread pool executor.
    """

    def __init__(
        self,
        stub: service_pb2_grpc.FilaServiceStub,
        max_messages: int = _DEFAULT_MAX_MESSAGES,
        max_workers: int = 4,
    ) -> None:
        self._stub = stub
        self._max_messages = max_messages
        self._queue: queue.Queue[_EnqueueItem | object] = queue.Queue()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def submit(self, proto: service_pb2.EnqueueMessage) -> Future[str]:
        """Submit a message for accumulated enqueue. Returns a Future for the message ID."""
        fut: Future[str] = Future()
        self._queue.put(_EnqueueItem(proto, fut))
        return fut

    def close(self, timeout: float | None = 30.0) -> None:
        """Drain pending messages and shut down the accumulator.

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

            assert isinstance(first, _EnqueueItem)
            batch: list[_EnqueueItem] = [first]

            # Non-blocking drain of any additional queued messages.
            while len(batch) < self._max_messages:
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    break
                if item is _STOP:
                    # Flush what we have, then stop.
                    self._flush(batch)
                    return
                assert isinstance(item, _EnqueueItem)
                batch.append(item)

            self._flush(batch)

    def _flush(self, batch: list[_EnqueueItem]) -> None:
        """Dispatch a batch to the executor for concurrent RPC."""
        if len(batch) == 1:
            # Single-item optimization: still uses Enqueue but with one message.
            self._executor.submit(_flush_single, self._stub, batch[0])
        else:
            self._executor.submit(_flush_many, self._stub, batch)


class LingerAccumulator:
    """Timer-based accumulator: holds messages for up to linger_ms or max_messages.

    A background daemon thread accumulates messages and flushes when either
    the count reaches ``max_messages`` or ``linger_ms`` milliseconds have
    elapsed since the first message in the current batch arrived.
    """

    def __init__(
        self,
        stub: service_pb2_grpc.FilaServiceStub,
        linger_ms: float,
        max_messages: int,
        max_workers: int = 4,
    ) -> None:
        self._stub = stub
        self._linger_s = linger_ms / 1000.0
        self._max_messages = max_messages
        self._queue: queue.Queue[_EnqueueItem | object] = queue.Queue()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def submit(self, proto: service_pb2.EnqueueMessage) -> Future[str]:
        """Submit a message for accumulated enqueue. Returns a Future for the message ID."""
        fut: Future[str] = Future()
        self._queue.put(_EnqueueItem(proto, fut))
        return fut

    def close(self, timeout: float | None = 30.0) -> None:
        """Drain pending messages and shut down the accumulator."""
        self._queue.put(_STOP)
        self._thread.join(timeout=timeout)
        self._executor.shutdown(wait=True)

    def update_stub(self, stub: service_pb2_grpc.FilaServiceStub) -> None:
        """Update the gRPC stub (e.g. after leader-hint reconnect)."""
        self._stub = stub

    def _run(self) -> None:
        """Background loop: accumulate up to max_messages or linger timeout."""
        import time

        while True:
            # Block until at least one item arrives.
            first = self._queue.get()
            if first is _STOP:
                return

            assert isinstance(first, _EnqueueItem)
            batch: list[_EnqueueItem] = [first]

            # Track wall-clock deadline from when first message arrived.
            deadline = time.monotonic() + self._linger_s

            # Accumulate more items until max_messages or linger timeout.
            while len(batch) < self._max_messages:
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
                assert isinstance(item, _EnqueueItem)
                batch.append(item)

            self._flush(batch)

    def _flush(self, batch: list[_EnqueueItem]) -> None:
        """Dispatch a batch to the executor for concurrent RPC."""
        if len(batch) == 1:
            self._executor.submit(_flush_single, self._stub, batch[0])
        else:
            self._executor.submit(_flush_many, self._stub, batch)
