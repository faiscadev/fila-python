"""Integration tests for batch enqueue and smart batching.

These tests require a running fila-server binary. They are skipped
automatically when the server is not found (local dev).
"""

from __future__ import annotations

import pytest

import fila


class TestBatchEnqueue:
    """Integration tests for the explicit batch_enqueue method."""

    def test_batch_enqueue_multiple_messages(self, server: object) -> None:
        """batch_enqueue sends multiple messages in one RPC and returns per-message results."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-batch")

        with fila.Client(server.addr, batch_mode=fila.BatchMode.DISABLED) as client:
            results = client.batch_enqueue([
                ("test-batch", {"idx": "0"}, b"payload-0"),
                ("test-batch", {"idx": "1"}, b"payload-1"),
                ("test-batch", {"idx": "2"}, b"payload-2"),
            ])

            assert len(results) == 3
            for r in results:
                assert r.is_success
                assert r.message_id is not None
                assert r.error is None

            # All message IDs should be unique.
            ids = [r.message_id for r in results]
            assert len(set(ids)) == 3

    def test_batch_enqueue_single_message(self, server: object) -> None:
        """batch_enqueue works with a single message."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-batch-single")

        with fila.Client(server.addr, batch_mode=fila.BatchMode.DISABLED) as client:
            results = client.batch_enqueue([
                ("test-batch-single", None, b"solo"),
            ])

            assert len(results) == 1
            assert results[0].is_success
            assert results[0].message_id is not None

    def test_batch_enqueue_consume_verify(self, server: object) -> None:
        """Messages enqueued via batch_enqueue can be consumed and acked."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-batch-consume")

        with fila.Client(server.addr, batch_mode=fila.BatchMode.DISABLED) as client:
            results = client.batch_enqueue([
                ("test-batch-consume", {"k": "v"}, b"batch-msg"),
            ])
            assert results[0].is_success

            stream = client.consume("test-batch-consume")
            msg = next(stream)

            assert msg.id == results[0].message_id
            assert msg.headers["k"] == "v"
            assert msg.payload == b"batch-msg"

            client.ack("test-batch-consume", msg.id)


class TestAsyncBatchEnqueue:
    """Integration tests for the async batch_enqueue method."""

    @pytest.mark.asyncio
    async def test_async_batch_enqueue(self, server: object) -> None:
        """Async batch_enqueue sends multiple messages."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-async-batch")

        async with fila.AsyncClient(server.addr) as client:
            results = await client.batch_enqueue([
                ("test-async-batch", None, b"async-0"),
                ("test-async-batch", None, b"async-1"),
            ])

            assert len(results) == 2
            for r in results:
                assert r.is_success
                assert r.message_id is not None


class TestSmartBatching:
    """Integration tests for smart batching (BatchMode.AUTO)."""

    def test_auto_mode_enqueue(self, server: object) -> None:
        """AUTO mode enqueues messages through the batcher."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-auto-batch")

        with fila.Client(server.addr, batch_mode=fila.BatchMode.AUTO) as client:
            msg_id = client.enqueue("test-auto-batch", None, b"auto-msg")
            assert msg_id != ""

            # Verify the message was actually enqueued.
            stream = client.consume("test-auto-batch")
            msg = next(stream)
            assert msg.id == msg_id
            assert msg.payload == b"auto-msg"
            client.ack("test-auto-batch", msg.id)

    def test_auto_mode_multiple_messages(self, server: object) -> None:
        """AUTO mode handles multiple sequential enqueues."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-auto-multi")

        with fila.Client(server.addr, batch_mode=fila.BatchMode.AUTO) as client:
            ids = []
            for i in range(5):
                msg_id = client.enqueue(
                    "test-auto-multi", None, f"msg-{i}".encode()
                )
                assert msg_id != ""
                ids.append(msg_id)

            # All IDs should be unique.
            assert len(set(ids)) == 5

    def test_disabled_mode_enqueue(self, server: object) -> None:
        """DISABLED mode sends each enqueue as a direct RPC."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-disabled")

        with fila.Client(server.addr, batch_mode=fila.BatchMode.DISABLED) as client:
            msg_id = client.enqueue("test-disabled", None, b"direct")
            assert msg_id != ""

            stream = client.consume("test-disabled")
            msg = next(stream)
            assert msg.id == msg_id
            client.ack("test-disabled", msg.id)

    def test_linger_mode_enqueue(self, server: object) -> None:
        """LINGER mode enqueues messages through a timer-based batcher."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-linger")

        with fila.Client(
            server.addr,
            batch_mode=fila.Linger(linger_ms=50, batch_size=10),
        ) as client:
            msg_id = client.enqueue("test-linger", None, b"lingered")
            assert msg_id != ""

            stream = client.consume("test-linger")
            msg = next(stream)
            assert msg.id == msg_id
            assert msg.payload == b"lingered"
            client.ack("test-linger", msg.id)

    def test_default_mode_is_auto(self, server: object) -> None:
        """Client defaults to AUTO batch mode."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-default-mode")

        # No batch_mode arg = AUTO.
        with fila.Client(server.addr) as client:
            msg_id = client.enqueue("test-default-mode", None, b"default")
            assert msg_id != ""


class TestBatchModeTypes:
    """Unit tests for BatchMode and Linger types (no server needed)."""

    def test_batch_mode_enum(self) -> None:
        """BatchMode has AUTO and DISABLED variants."""
        assert fila.BatchMode.AUTO is not None
        assert fila.BatchMode.DISABLED is not None
        modes = {fila.BatchMode.AUTO, fila.BatchMode.DISABLED}
        assert len(modes) == 2  # They are distinct values

    def test_linger_fields(self) -> None:
        """Linger stores linger_ms and batch_size."""
        linger = fila.Linger(linger_ms=100, batch_size=50)
        assert linger.linger_ms == 100
        assert linger.batch_size == 50

    def test_batch_enqueue_result_success(self) -> None:
        """BatchEnqueueResult.is_success returns True when message_id is set."""
        r = fila.BatchEnqueueResult(message_id="abc", error=None)
        assert r.is_success
        assert r.message_id == "abc"
        assert r.error is None

    def test_batch_enqueue_result_error(self) -> None:
        """BatchEnqueueResult.is_success returns False when error is set."""
        r = fila.BatchEnqueueResult(message_id=None, error="queue not found")
        assert not r.is_success
        assert r.message_id is None
        assert r.error == "queue not found"
