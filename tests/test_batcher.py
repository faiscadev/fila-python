"""Unit tests for the batcher module.

These tests use mock connections and do not require a running fila-server.
"""

from __future__ import annotations

import struct
from concurrent.futures import Future
from unittest.mock import MagicMock

import pytest

from fila.batcher import (
    AutoAccumulator,
    LingerAccumulator,
    _EnqueueItem,
    _flush_many,
    _flush_single,
)
from fila.errors import EnqueueError, QueueNotFoundError
from fila.fibp.opcodes import ErrorCode, FrameHeader, Opcode


def _make_enqueue_result_body(*results: tuple[int, str]) -> bytes:
    """Build an EnqueueResult frame body from (error_code, message_id) tuples."""
    buf = struct.pack("!I", len(results))
    for code, msg_id in results:
        buf += struct.pack("!B", code)
        encoded = msg_id.encode("utf-8")
        buf += struct.pack("!H", len(encoded)) + encoded
    return buf


def _make_mock_conn(*results: tuple[int, str]) -> MagicMock:
    """Create a mock Connection whose request() returns an EnqueueResult."""
    conn = MagicMock()
    body = _make_enqueue_result_body(*results)
    header = FrameHeader(opcode=Opcode.ENQUEUE_RESULT, flags=0, request_id=1)
    conn.request.return_value = (header, body)
    return conn


def _make_error_conn(error_code: int, message: str) -> MagicMock:
    """Create a mock Connection whose request() returns an Error frame."""
    conn = MagicMock()
    # Build error frame body: u8 code, string message, map metadata (empty)
    encoded_msg = message.encode("utf-8")
    body = (
        struct.pack("!B", error_code)
        + struct.pack("!H", len(encoded_msg)) + encoded_msg
        + struct.pack("!H", 0)  # empty metadata map
    )
    header = FrameHeader(opcode=Opcode.ERROR, flags=0, request_id=1)
    conn.request.return_value = (header, body)
    return conn


class TestFlushSingle:
    """Test the _flush_single function."""

    def test_success(self) -> None:
        conn = _make_mock_conn((ErrorCode.OK, "msg-001"))

        msg = {"queue": "q", "headers": {}, "payload": b"data"}
        fut: Future[str] = Future()
        req = _EnqueueItem(msg, fut)

        _flush_single(conn, req)

        assert fut.result(timeout=1.0) == "msg-001"
        conn.request.assert_called_once()

    def test_error_frame(self) -> None:
        conn = _make_error_conn(ErrorCode.QUEUE_NOT_FOUND, "queue not found")

        msg = {"queue": "missing", "headers": {}, "payload": b"data"}
        fut: Future[str] = Future()
        req = _EnqueueItem(msg, fut)

        _flush_single(conn, req)

        with pytest.raises(QueueNotFoundError):
            fut.result(timeout=1.0)


class TestFlushMany:
    """Test the _flush_many function."""

    def test_all_success(self) -> None:
        conn = _make_mock_conn(
            (ErrorCode.OK, "id-1"),
            (ErrorCode.OK, "id-2"),
        )

        items = [
            _EnqueueItem(
                {"queue": "q", "headers": {}, "payload": b"a"},
                Future(),
            ),
            _EnqueueItem(
                {"queue": "q", "headers": {}, "payload": b"b"},
                Future(),
            ),
        ]

        _flush_many(conn, items)

        assert items[0].future.result(timeout=1.0) == "id-1"
        assert items[1].future.result(timeout=1.0) == "id-2"

    def test_mixed_results(self) -> None:
        conn = _make_mock_conn(
            (ErrorCode.OK, "id-1"),
            (ErrorCode.QUEUE_NOT_FOUND, ""),
        )

        items = [
            _EnqueueItem(
                {"queue": "q", "headers": {}, "payload": b"a"},
                Future(),
            ),
            _EnqueueItem(
                {"queue": "missing", "headers": {}, "payload": b"b"},
                Future(),
            ),
        ]

        _flush_many(conn, items)

        assert items[0].future.result(timeout=1.0) == "id-1"
        with pytest.raises(QueueNotFoundError):
            items[1].future.result(timeout=1.0)

    def test_connection_failure_sets_all_futures(self) -> None:
        conn = MagicMock()
        conn.request.side_effect = ConnectionError("server unavailable")

        items = [
            _EnqueueItem(
                {"queue": "q", "headers": {}, "payload": b"a"},
                Future(),
            ),
            _EnqueueItem(
                {"queue": "q", "headers": {}, "payload": b"b"},
                Future(),
            ),
        ]

        _flush_many(conn, items)

        for item in items:
            with pytest.raises(EnqueueError):
                item.future.result(timeout=1.0)


class TestAutoAccumulator:
    """Test the AutoAccumulator end-to-end."""

    def test_single_message(self) -> None:
        """When only one message is queued, AutoAccumulator sends it."""
        conn = _make_mock_conn((ErrorCode.OK, "msg-solo"))
        accumulator = AutoAccumulator(conn, max_messages=100)

        msg = {"queue": "q", "headers": {}, "payload": b"solo"}
        fut = accumulator.submit(msg)
        result = fut.result(timeout=5.0)

        assert result == "msg-solo"
        conn.request.assert_called_once()
        accumulator.close()

    def test_concurrent_messages_accumulated(self) -> None:
        """When multiple messages arrive concurrently, they accumulate together."""
        conn = _make_mock_conn(*[(ErrorCode.OK, f"id-{i}") for i in range(5)])
        accumulator = AutoAccumulator(conn, max_messages=100)

        futures = []
        for i in range(5):
            msg = {"queue": "q", "headers": {}, "payload": f"msg-{i}".encode()}
            futures.append(accumulator.submit(msg))

        for f in futures:
            result = f.result(timeout=5.0)
            assert result is not None

        accumulator.close()

    def test_close_drains_pending(self) -> None:
        """close() waits for pending messages to be flushed."""
        conn = _make_mock_conn((ErrorCode.OK, "drained"))
        accumulator = AutoAccumulator(conn, max_messages=100)

        msg = {"queue": "q", "headers": {}, "payload": b"drain-me"}
        fut = accumulator.submit(msg)

        accumulator.close()

        assert fut.result(timeout=1.0) == "drained"

    def test_update_conn(self) -> None:
        """update_conn replaces the connection used for flushing."""
        old_conn = MagicMock()
        new_conn = _make_mock_conn((ErrorCode.OK, "new-conn"))

        accumulator = AutoAccumulator(old_conn, max_messages=100)
        accumulator.update_conn(new_conn)

        msg = {"queue": "q", "headers": {}, "payload": b"data"}
        fut = accumulator.submit(msg)
        result = fut.result(timeout=5.0)

        assert result == "new-conn"
        accumulator.close()


class TestLingerAccumulator:
    """Test the LingerAccumulator."""

    def test_flushes_at_max_messages(self) -> None:
        """Flush triggers when max_messages messages accumulate."""
        conn = _make_mock_conn(*[(ErrorCode.OK, f"id-{i}") for i in range(3)])
        accumulator = LingerAccumulator(conn, linger_ms=5000, max_messages=3)

        futures = []
        for i in range(3):
            msg = {"queue": "q", "headers": {}, "payload": f"m{i}".encode()}
            futures.append(accumulator.submit(msg))

        for i, f in enumerate(futures):
            result = f.result(timeout=5.0)
            assert result == f"id-{i}"

        accumulator.close()

    def test_flushes_at_linger_timeout(self) -> None:
        """Flush triggers after linger_ms even if max_messages is not reached."""
        conn = _make_mock_conn((ErrorCode.OK, "lingered"))
        accumulator = LingerAccumulator(conn, linger_ms=50, max_messages=100)

        msg = {"queue": "q", "headers": {}, "payload": b"linger"}
        fut = accumulator.submit(msg)

        result = fut.result(timeout=5.0)
        assert result == "lingered"

        accumulator.close()

    def test_close_drains_pending(self) -> None:
        """close() drains any pending messages."""
        conn = _make_mock_conn((ErrorCode.OK, "drained"))
        accumulator = LingerAccumulator(conn, linger_ms=10000, max_messages=100)

        msg = {"queue": "q", "headers": {}, "payload": b"drain"}
        fut = accumulator.submit(msg)

        accumulator.close()

        assert fut.result(timeout=1.0) == "drained"
