"""Background accumulator for opportunistic and linger-based enqueue accumulation."""

from __future__ import annotations

import queue
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from typing import TYPE_CHECKING

from fila.errors import EnqueueError, _map_per_item_error
from fila.fibp.codec import decode_enqueue_result, decode_error, encode_enqueue
from fila.fibp.opcodes import ErrorCode, Opcode

if TYPE_CHECKING:
    from fila.conn import Connection


# Sentinel that signals the accumulator thread to stop.
_STOP = object()

# Maximum number of messages per flush when none is configured.
_DEFAULT_MAX_MESSAGES = 1000


class _EnqueueItem:
    """Internal envelope pairing a message dict with its result future."""

    __slots__ = ("msg", "future")

    def __init__(
        self,
        msg: dict[str, object],
        future: Future[str],
    ) -> None:
        self.msg = msg
        self.future = future


def _flush_single(
    conn: Connection,
    req: _EnqueueItem,
) -> None:
    """Send a single message via the FIBP Enqueue request."""
    try:
        body = encode_enqueue([req.msg])
        header, resp_body = conn.request(Opcode.ENQUEUE, body)

        if header.opcode == Opcode.ERROR:
            err = decode_error(resp_body)
            from fila.errors import _raise_from_error_frame
            try:
                _raise_from_error_frame(err)
            except Exception as e:
                req.future.set_exception(e)
                return

        items = decode_enqueue_result(resp_body)
        item = items[0]
        if item.error_code == ErrorCode.OK:
            req.future.set_result(item.message_id)
        else:
            req.future.set_exception(
                _map_per_item_error(item.error_code, "enqueue")
            )
    except Exception as e:
        req.future.set_exception(e)


def _flush_many(
    conn: Connection,
    items: list[_EnqueueItem],
) -> None:
    """Send multiple messages via the FIBP Enqueue request."""
    try:
        body = encode_enqueue([item.msg for item in items])
        header, resp_body = conn.request(Opcode.ENQUEUE, body)
    except Exception as e:
        err = EnqueueError(f"enqueue request failed: {e}")
        for item in items:
            item.future.set_exception(err)
        return

    if header.opcode == Opcode.ERROR:
        try:
            err_frame = decode_error(resp_body)
            from fila.errors import _map_error_code
            exc = _map_error_code(err_frame.code, err_frame.message)
        except Exception as e:
            exc = EnqueueError(f"enqueue failed: {e}")
        for item in items:
            item.future.set_exception(exc)
        return

    results = decode_enqueue_result(resp_body)
    for i, result in enumerate(results):
        if i >= len(items):
            break
        item = items[i]
        if result.error_code == ErrorCode.OK:
            item.future.set_result(result.message_id)
        else:
            item.future.set_exception(
                _map_per_item_error(result.error_code, "enqueue")
            )


class AutoAccumulator:
    """Opportunistic accumulator: drains a queue and flushes in batches.

    A background daemon thread blocks on the first message, then non-blocking
    drains any additional messages that arrived during processing and flushes
    them as a single Enqueue request via a thread pool executor.
    """

    def __init__(
        self,
        conn: Connection,
        max_messages: int = _DEFAULT_MAX_MESSAGES,
        max_workers: int = 4,
    ) -> None:
        self._conn = conn
        self._max_messages = max_messages
        self._queue: queue.Queue[_EnqueueItem | object] = queue.Queue()
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def submit(self, msg: dict[str, object]) -> Future[str]:
        """Submit a message for accumulated enqueue. Returns a Future for the message ID."""
        fut: Future[str] = Future()
        self._queue.put(_EnqueueItem(msg, fut))
        return fut

    def close(self, timeout: float | None = 30.0) -> None:
        """Drain pending messages and shut down the accumulator."""
        self._queue.put(_STOP)
        self._thread.join(timeout=timeout)
        self._executor.shutdown(wait=True)

    def update_conn(self, conn: Connection) -> None:
        """Update the connection (e.g. after leader-hint reconnect)."""
        self._conn = conn

    def _run(self) -> None:
        """Background loop: block for first item, drain rest, flush."""
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
        """Dispatch a batch to the executor."""
        if len(batch) == 1:
            self._executor.submit(_flush_single, self._conn, batch[0])
        else:
            self._executor.submit(_flush_many, self._conn, batch)


class LingerAccumulator:
    """Timer-based accumulator: holds messages for up to linger_ms or max_messages.

    A background daemon thread accumulates messages and flushes when either
    the count reaches ``max_messages`` or ``linger_ms`` milliseconds have
    elapsed since the first message in the current batch arrived.
    """

    def __init__(
        self,
        conn: Connection,
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

    def submit(self, msg: dict[str, object]) -> Future[str]:
        """Submit a message for accumulated enqueue. Returns a Future for the message ID."""
        fut: Future[str] = Future()
        self._queue.put(_EnqueueItem(msg, fut))
        return fut

    def close(self, timeout: float | None = 30.0) -> None:
        """Drain pending messages and shut down the accumulator."""
        self._queue.put(_STOP)
        self._thread.join(timeout=timeout)
        self._executor.shutdown(wait=True)

    def update_conn(self, conn: Connection) -> None:
        """Update the connection (e.g. after leader-hint reconnect)."""
        self._conn = conn

    def _run(self) -> None:
        """Background loop: accumulate up to max_messages or linger timeout."""
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
        """Dispatch a batch to the executor."""
        if len(batch) == 1:
            self._executor.submit(_flush_single, self._conn, batch[0])
        else:
            self._executor.submit(_flush_many, self._conn, batch)
