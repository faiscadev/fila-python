"""Background accumulator for opportunistic and linger-based enqueue accumulation."""

from __future__ import annotations

import queue
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import TYPE_CHECKING

from fila.errors import _map_enqueue_error_code
from fila.fibp import (
    FibpError,
    decode_enqueue_response,
    encode_enqueue,
)

if TYPE_CHECKING:
    from fila.fibp import FibpConnection


# Sentinel that signals the accumulator thread to stop.
_STOP = object()

# Maximum number of messages per flush when none is configured.
_DEFAULT_MAX_MESSAGES = 1000


class _EnqueueItem:
    """Internal envelope pairing an enqueue request with its result future."""

    __slots__ = ("queue", "headers", "payload", "future")

    def __init__(
        self,
        queue_name: str,
        headers: dict[str, str],
        payload: bytes,
        future: Future[str],
    ) -> None:
        self.queue = queue_name
        self.headers = headers
        self.payload = payload
        self.future = future


def _flush_single(
    conn: FibpConnection,
    item: _EnqueueItem,
) -> None:
    """Send a single message via FIBP ENQUEUE."""
    corr_id = conn.alloc_corr_id()
    frame = encode_enqueue(corr_id, [(item.queue, item.headers, item.payload)])
    try:
        body = conn.send_request(frame, corr_id).result()
        results = decode_enqueue_response(body)
        ok, msg_id, err_code, err_msg = results[0]
        if ok:
            item.future.set_result(msg_id)
        else:
            item.future.set_exception(_map_enqueue_error_code(err_code, err_msg))
    except FibpError as e:
        item.future.set_exception(_map_enqueue_error_code(e.code, e.message))
    except Exception as e:
        item.future.set_exception(e)


def _flush_many(
    conn: FibpConnection,
    items: list[_EnqueueItem],
) -> None:
    """Send multiple messages (same queue) via a single FIBP ENQUEUE frame.

    On transport failure, every future in the batch receives an
    ``EnqueueError``.  On success, each future gets either its message ID
    or a per-message error.

    Note: FIBP ENQUEUE frames encode all messages for one queue.  If the
    batch spans multiple queues, it is split into per-queue sub-batches.
    """
    # Group by queue so each FIBP frame targets a single queue.
    from collections import defaultdict
    by_queue: dict[str, list[_EnqueueItem]] = defaultdict(list)
    for item in items:
        by_queue[item.queue].append(item)

    for queue_name, queue_items in by_queue.items():
        _flush_queue_batch(conn, queue_name, queue_items)


def _flush_queue_batch(
    conn: FibpConnection,
    queue_name: str,
    items: list[_EnqueueItem],
) -> None:
    """Flush a batch of items all targeting *queue_name*."""
    corr_id = conn.alloc_corr_id()
    messages = [(queue_name, it.headers, it.payload) for it in items]
    frame = encode_enqueue(corr_id, messages)
    try:
        body = conn.send_request(frame, corr_id).result()
    except FibpError as e:
        err = _map_enqueue_error_code(e.code, e.message)
        for item in items:
            item.future.set_exception(err)
        return
    except Exception as e:
        for item in items:
            item.future.set_exception(e)
        return

    results = decode_enqueue_response(body)
    for i, (ok, msg_id, err_code, err_msg) in enumerate(results):
        if i >= len(items):
            break
        if ok:
            items[i].future.set_result(msg_id)
        else:
            items[i].future.set_exception(_map_enqueue_error_code(err_code, err_msg))


class AutoAccumulator:
    """Opportunistic accumulator: drains a queue and flushes in batches.

    A background daemon thread blocks on the first message, then non-blocking
    drains any additional messages that arrived during processing and flushes
    them as a single ENQUEUE frame.
    """

    def __init__(
        self,
        conn: FibpConnection,
        max_messages: int = _DEFAULT_MAX_MESSAGES,
        max_workers: int = 4,
    ) -> None:
        self._conn = conn
        self._max_messages = max_messages
        self._queue: queue.Queue[_EnqueueItem | object] = queue.Queue()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def submit(
        self,
        queue_name: str,
        headers: dict[str, str],
        payload: bytes,
    ) -> Future[str]:
        """Submit a message for accumulated enqueue. Returns a Future for the message ID."""
        fut: Future[str] = Future()
        self._queue.put(_EnqueueItem(queue_name, headers, payload, fut))
        return fut

    def close(self, timeout: float | None = 30.0) -> None:
        """Drain pending messages and shut down the accumulator."""
        self._queue.put(_STOP)
        self._thread.join(timeout=timeout)
        self._executor.shutdown(wait=True)

    def update_conn(self, conn: FibpConnection) -> None:
        """Replace the underlying connection (e.g., after a leader-hint reconnect)."""
        self._conn = conn

    def _run(self) -> None:
        while True:
            first = self._queue.get()
            if first is _STOP:
                return

            assert isinstance(first, _EnqueueItem)
            batch: list[_EnqueueItem] = [first]

            while len(batch) < self._max_messages:
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    break
                if item is _STOP:
                    self._flush(batch)
                    return
                assert isinstance(item, _EnqueueItem)
                batch.append(item)

            self._flush(batch)

    def _flush(self, batch: list[_EnqueueItem]) -> None:
        if len(batch) == 1:
            self._executor.submit(_flush_single, self._conn, batch[0])
        else:
            self._executor.submit(_flush_many, self._conn, batch)


class LingerAccumulator:
    """Timer-based accumulator: holds messages for up to linger_ms or max_messages."""

    def __init__(
        self,
        conn: FibpConnection,
        linger_ms: float,
        max_messages: int,
        max_workers: int = 4,
    ) -> None:
        self._conn = conn
        self._linger_s = linger_ms / 1000.0
        self._max_messages = max_messages
        self._queue: queue.Queue[_EnqueueItem | object] = queue.Queue()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def submit(
        self,
        queue_name: str,
        headers: dict[str, str],
        payload: bytes,
    ) -> Future[str]:
        """Submit a message for accumulated enqueue. Returns a Future for the message ID."""
        fut: Future[str] = Future()
        self._queue.put(_EnqueueItem(queue_name, headers, payload, fut))
        return fut

    def close(self, timeout: float | None = 30.0) -> None:
        """Drain pending messages and shut down the accumulator."""
        self._queue.put(_STOP)
        self._thread.join(timeout=timeout)
        self._executor.shutdown(wait=True)

    def update_conn(self, conn: FibpConnection) -> None:
        """Replace the underlying connection."""
        self._conn = conn

    def _run(self) -> None:
        import time

        while True:
            first = self._queue.get()
            if first is _STOP:
                return

            assert isinstance(first, _EnqueueItem)
            batch: list[_EnqueueItem] = [first]

            deadline = time.monotonic() + self._linger_s

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
        if len(batch) == 1:
            self._executor.submit(_flush_single, self._conn, batch[0])
        else:
            self._executor.submit(_flush_many, self._conn, batch)
