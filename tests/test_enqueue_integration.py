"""Integration tests for enqueue_many and accumulator modes.

These tests require a running fila-server binary. They are skipped
automatically when the server is not found (local dev).
"""

from __future__ import annotations

import pytest

import fila


class TestEnqueueMany:
    """Integration tests for the explicit enqueue_many method."""

    def test_enqueue_many_multiple_messages(self, server: object) -> None:
        """enqueue_many sends multiple messages in one request and returns per-message results."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-enqueue-many")

        with fila.Client(
            server.addr, accumulator_mode=fila.AccumulatorMode.DISABLED
        ) as client:
            results = client.enqueue_many([
                ("test-enqueue-many", {"idx": "0"}, b"payload-0"),
                ("test-enqueue-many", {"idx": "1"}, b"payload-1"),
                ("test-enqueue-many", {"idx": "2"}, b"payload-2"),
            ])

            assert len(results) == 3
            for r in results:
                assert r.is_success
                assert r.message_id is not None
                assert r.error is None

            ids = [r.message_id for r in results]
            assert len(set(ids)) == 3

    def test_enqueue_many_single_message(self, server: object) -> None:
        """enqueue_many works with a single message."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-enqueue-many-single")

        with fila.Client(
            server.addr, accumulator_mode=fila.AccumulatorMode.DISABLED
        ) as client:
            results = client.enqueue_many([
                ("test-enqueue-many-single", None, b"solo"),
            ])

            assert len(results) == 1
            assert results[0].is_success
            assert results[0].message_id is not None

    def test_enqueue_many_consume_verify(self, server: object) -> None:
        """Messages enqueued via enqueue_many can be consumed and acked."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-enqueue-many-consume")

        with fila.Client(
            server.addr, accumulator_mode=fila.AccumulatorMode.DISABLED
        ) as client:
            results = client.enqueue_many([
                ("test-enqueue-many-consume", {"k": "v"}, b"multi-msg"),
            ])
            assert results[0].is_success

            stream = client.consume("test-enqueue-many-consume")
            msg = next(stream)

            assert msg.id == results[0].message_id
            assert msg.headers["k"] == "v"
            assert msg.payload == b"multi-msg"

            client.ack("test-enqueue-many-consume", msg.id)


class TestAsyncEnqueueMany:
    """Integration tests for the async enqueue_many method."""

    @pytest.mark.asyncio
    async def test_async_enqueue_many(self, server: object) -> None:
        """Async enqueue_many sends multiple messages."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-async-enqueue-many")

        async with fila.AsyncClient(server.addr) as client:
            results = await client.enqueue_many([
                ("test-async-enqueue-many", None, b"async-0"),
                ("test-async-enqueue-many", None, b"async-1"),
            ])

            assert len(results) == 2
            for r in results:
                assert r.is_success
                assert r.message_id is not None


class TestAccumulatorModes:
    """Integration tests for accumulator modes (AccumulatorMode.AUTO, Linger)."""

    def test_auto_mode_enqueue(self, server: object) -> None:
        """AUTO mode enqueues messages through the accumulator."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-auto-accum")

        with fila.Client(
            server.addr, accumulator_mode=fila.AccumulatorMode.AUTO
        ) as client:
            msg_id = client.enqueue("test-auto-accum", None, b"auto-msg")
            assert msg_id != ""

            stream = client.consume("test-auto-accum")
            msg = next(stream)
            assert msg.id == msg_id
            assert msg.payload == b"auto-msg"
            client.ack("test-auto-accum", msg.id)

    def test_auto_mode_multiple_messages(self, server: object) -> None:
        """AUTO mode handles multiple sequential enqueues."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-auto-multi")

        with fila.Client(
            server.addr, accumulator_mode=fila.AccumulatorMode.AUTO
        ) as client:
            ids = []
            for i in range(5):
                msg_id = client.enqueue(
                    "test-auto-multi", None, f"msg-{i}".encode()
                )
                assert msg_id != ""
                ids.append(msg_id)

            assert len(set(ids)) == 5

    def test_disabled_mode_enqueue(self, server: object) -> None:
        """DISABLED mode sends each enqueue as a direct request."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-disabled")

        with fila.Client(
            server.addr, accumulator_mode=fila.AccumulatorMode.DISABLED
        ) as client:
            msg_id = client.enqueue("test-disabled", None, b"direct")
            assert msg_id != ""

            stream = client.consume("test-disabled")
            msg = next(stream)
            assert msg.id == msg_id
            client.ack("test-disabled", msg.id)

    def test_linger_mode_enqueue(self, server: object) -> None:
        """LINGER mode enqueues messages through a timer-based accumulator."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-linger")

        with fila.Client(
            server.addr,
            accumulator_mode=fila.Linger(linger_ms=50, max_messages=10),
        ) as client:
            msg_id = client.enqueue("test-linger", None, b"lingered")
            assert msg_id != ""

            stream = client.consume("test-linger")
            msg = next(stream)
            assert msg.id == msg_id
            assert msg.payload == b"lingered"
            client.ack("test-linger", msg.id)

    def test_default_mode_is_auto(self, server: object) -> None:
        """Client defaults to AUTO accumulator mode."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-default-mode")

        with fila.Client(server.addr) as client:
            msg_id = client.enqueue("test-default-mode", None, b"default")
            assert msg_id != ""


class TestAccumulatorModeTypes:
    """Unit tests for AccumulatorMode and Linger types (no server needed)."""

    def test_accumulator_mode_enum(self) -> None:
        """AccumulatorMode has AUTO and DISABLED variants."""
        assert fila.AccumulatorMode.AUTO is not None
        assert fila.AccumulatorMode.DISABLED is not None
        modes = {fila.AccumulatorMode.AUTO, fila.AccumulatorMode.DISABLED}
        assert len(modes) == 2

    def test_linger_fields(self) -> None:
        """Linger stores linger_ms and max_messages."""
        linger = fila.Linger(linger_ms=100, max_messages=50)
        assert linger.linger_ms == 100
        assert linger.max_messages == 50

    def test_enqueue_result_success(self) -> None:
        """EnqueueResult.is_success returns True when message_id is set."""
        r = fila.EnqueueResult(message_id="abc", error=None)
        assert r.is_success
        assert r.message_id == "abc"
        assert r.error is None

    def test_enqueue_result_error(self) -> None:
        """EnqueueResult.is_success returns False when error is set."""
        r = fila.EnqueueResult(message_id=None, error="queue not found")
        assert not r.is_success
        assert r.message_id is None
        assert r.error == "queue not found"
