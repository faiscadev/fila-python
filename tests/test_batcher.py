"""Unit tests for the batcher module.

These tests use a mock FibpConnection and do not require a running fila-server.
"""

from __future__ import annotations

import struct
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
from fila.errors import EnqueueError, QueueNotFoundError, TransportError
from fila.fibp import (
    ERR_QUEUE_NOT_FOUND,
    FibpError,
)


def _make_enqueue_response(results: list[tuple[bool, str, int, str]]) -> bytes:
    """Build an ENQUEUE response body from a list of (ok, msg_id, err_code, err_msg) tuples."""
    parts: list[bytes] = [struct.pack(">H", len(results))]
    for ok, msg_id, err_code, err_msg in results:
        if ok:
            parts.append(struct.pack(">B", 1))
            b = msg_id.encode()
            parts.append(struct.pack(">H", len(b)) + b)
        else:
            parts.append(struct.pack(">B", 0))
            parts.append(struct.pack(">H", err_code))
            b = err_msg.encode()
            parts.append(struct.pack(">H", len(b)) + b)
    return b"".join(parts)


def _make_conn(response_body: bytes | None = None, error: Exception | None = None) -> Any:
    """Create a mock FibpConnection that returns *response_body* or raises *error*."""
    conn = MagicMock()
    conn.alloc_corr_id.return_value = 1

    fut: Future[bytes] = Future()
    if error is not None:
        fut.set_exception(error)
    elif response_body is not None:
        fut.set_result(response_body)

    conn.send_request.return_value = fut
    return conn


class TestFlushSingle:
    """Tests for _flush_single."""

    def test_success(self) -> None:
        resp_body = _make_enqueue_response([(True, "msg-001", 0, "")])
        conn = _make_conn(resp_body)

        fut: Future[str] = Future()
        item = _EnqueueItem("q", {}, b"data", fut)

        _flush_single(conn, item)

        assert fut.result(timeout=1.0) == "msg-001"
        conn.send_request.assert_called_once()

    def test_transport_error_maps_to_queue_not_found(self) -> None:
        conn = _make_conn(error=FibpError(ERR_QUEUE_NOT_FOUND, "queue not found"))

        fut: Future[str] = Future()
        item = _EnqueueItem("missing", {}, b"data", fut)

        _flush_single(conn, item)

        with pytest.raises(QueueNotFoundError):
            fut.result(timeout=1.0)

    def test_per_message_error(self) -> None:
        resp_body = _make_enqueue_response(
            [(False, "", ERR_QUEUE_NOT_FOUND, "queue 'q' not found")]
        )
        conn = _make_conn(resp_body)

        fut: Future[str] = Future()
        item = _EnqueueItem("q", {}, b"data", fut)

        _flush_single(conn, item)

        with pytest.raises(QueueNotFoundError):
            fut.result(timeout=1.0)


class TestFlushMany:
    """Tests for _flush_many."""

    def test_all_success(self) -> None:
        resp_body = _make_enqueue_response([
            (True, "id-1", 0, ""),
            (True, "id-2", 0, ""),
        ])
        conn = _make_conn(resp_body)

        items = [
            _EnqueueItem("q", {}, b"a", Future()),
            _EnqueueItem("q", {}, b"b", Future()),
        ]

        _flush_many(conn, items)

        assert items[0].future.result(timeout=1.0) == "id-1"
        assert items[1].future.result(timeout=1.0) == "id-2"

    def test_mixed_results(self) -> None:
        # Both items target the same queue; the server returns a per-message error
        # for the second one (e.g., a failed Lua hook).
        resp_body = _make_enqueue_response([
            (True, "id-1", 0, ""),
            (False, "", ERR_QUEUE_NOT_FOUND, "queue 'q' not found"),
        ])
        conn = _make_conn(resp_body)

        items = [
            _EnqueueItem("q", {}, b"a", Future()),
            _EnqueueItem("q", {}, b"b", Future()),
        ]

        _flush_many(conn, items)

        assert items[0].future.result(timeout=1.0) == "id-1"
        with pytest.raises(QueueNotFoundError, match="not found"):
            items[1].future.result(timeout=1.0)

    def test_transport_failure_sets_all_futures(self) -> None:
        conn = _make_conn(error=FibpError(0, "server unavailable"))

        items = [
            _EnqueueItem("q", {}, b"a", Future()),
            _EnqueueItem("q", {}, b"b", Future()),
        ]

        _flush_many(conn, items)

        for item in items:
            with pytest.raises(TransportError):
                item.future.result(timeout=1.0)

    def test_multi_queue_batch_sends_per_queue_frames(self) -> None:
        """When items target different queues, _flush_many sends one frame per queue."""
        resp_body_q1 = _make_enqueue_response([(True, "id-q1", 0, "")])
        resp_body_q2 = _make_enqueue_response([(True, "id-q2", 0, "")])

        conn = MagicMock()
        conn.alloc_corr_id.side_effect = [1, 2]

        fut1: Future[bytes] = Future()
        fut1.set_result(resp_body_q1)
        fut2: Future[bytes] = Future()
        fut2.set_result(resp_body_q2)
        conn.send_request.side_effect = [fut1, fut2]

        items = [
            _EnqueueItem("queue-a", {}, b"a", Future()),
            _EnqueueItem("queue-b", {}, b"b", Future()),
        ]

        _flush_many(conn, items)

        assert conn.send_request.call_count == 2
        assert items[0].future.result(timeout=1.0) == "id-q1"
        assert items[1].future.result(timeout=1.0) == "id-q2"


class TestAutoAccumulator:
    """End-to-end tests for the AutoAccumulator."""

    def test_single_message(self) -> None:
        resp_body = _make_enqueue_response([(True, "msg-solo", 0, "")])
        conn = _make_conn(resp_body)

        acc = AutoAccumulator(conn, max_messages=100)
        fut = acc.submit("q", {}, b"solo")
        assert fut.result(timeout=5.0) == "msg-solo"
        acc.close()

    def test_close_drains_pending(self) -> None:
        resp_body = _make_enqueue_response([(True, "drained", 0, "")])
        conn = _make_conn(resp_body)

        acc = AutoAccumulator(conn, max_messages=100)
        fut = acc.submit("q", {}, b"drain-me")
        acc.close()
        assert fut.result(timeout=1.0) == "drained"

    def test_update_conn(self) -> None:
        old_conn = _make_conn(error=FibpError(0, "old conn"))
        resp_body = _make_enqueue_response([(True, "new-conn", 0, "")])
        new_conn = _make_conn(resp_body)

        acc = AutoAccumulator(old_conn, max_messages=100)
        acc.update_conn(new_conn)

        fut = acc.submit("q", {}, b"data")
        assert fut.result(timeout=5.0) == "new-conn"
        acc.close()


class TestLingerAccumulator:
    """Tests for the LingerAccumulator."""

    def test_flushes_at_max_messages(self) -> None:
        # conn needs to handle 3 results (they may arrive as 1 batch or 3 single calls)
        conn = MagicMock()
        conn.alloc_corr_id.side_effect = range(1, 100)

        def make_resp(n: int) -> Future[bytes]:
            fut: Future[bytes] = Future()
            fut.set_result(_make_enqueue_response([(True, f"id-{i}", 0, "") for i in range(n)]))
            return fut

        conn.send_request.side_effect = [make_resp(3)]

        acc = LingerAccumulator(conn, linger_ms=5000, max_messages=3)

        futures = []
        for i in range(3):
            futures.append(acc.submit("q", {}, f"m{i}".encode()))

        for f in futures:
            assert f.result(timeout=5.0) is not None

        acc.close()

    def test_flushes_at_linger_timeout(self) -> None:
        resp_body = _make_enqueue_response([(True, "lingered", 0, "")])
        conn = _make_conn(resp_body)

        acc = LingerAccumulator(conn, linger_ms=50, max_messages=100)
        fut = acc.submit("q", {}, b"linger")
        assert fut.result(timeout=5.0) == "lingered"
        acc.close()

    def test_close_drains_pending(self) -> None:
        resp_body = _make_enqueue_response([(True, "drained", 0, "")])
        conn = _make_conn(resp_body)

        acc = LingerAccumulator(conn, linger_ms=10000, max_messages=100)
        fut = acc.submit("q", {}, b"drain")
        acc.close()
        assert fut.result(timeout=1.0) == "drained"
