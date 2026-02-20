"""Integration tests for the Fila Python SDK."""

from __future__ import annotations

import pytest

import fila


class TestSyncClient:
    """Integration tests for the synchronous Client."""

    def test_enqueue_consume_ack(self, server: object) -> None:
        """Full lifecycle: enqueue -> consume -> verify fields -> ack."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-sync-eca")

        with fila.Client(server.addr) as client:
            # Enqueue a message.
            headers = {"tenant": "acme"}
            payload = b"hello world"
            msg_id = client.enqueue("test-sync-eca", headers, payload)
            assert msg_id != ""

            # Consume the message.
            stream = client.consume("test-sync-eca")
            msg = next(stream)

            assert msg.id == msg_id
            assert msg.headers["tenant"] == "acme"
            assert msg.payload == b"hello world"

            # Ack the message.
            client.ack("test-sync-eca", msg.id)

    def test_enqueue_consume_nack_redeliver(self, server: object) -> None:
        """Nack triggers redelivery on the same stream with incremented attempt count."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-sync-nack")

        with fila.Client(server.addr) as client:
            msg_id = client.enqueue("test-sync-nack", None, b"retry-me")

            # Open consume stream.
            stream = client.consume("test-sync-nack")

            # First delivery.
            msg = next(stream)
            assert msg.id == msg_id
            assert msg.attempt_count == 0

            # Nack the message.
            client.nack("test-sync-nack", msg.id, "transient failure")

            # Redelivery on the same stream.
            msg2 = next(stream)
            assert msg2.id == msg_id
            assert msg2.attempt_count == 1

            # Ack to clean up.
            client.ack("test-sync-nack", msg2.id)

    def test_enqueue_nonexistent_queue(self, server: object) -> None:
        """Enqueue to a missing queue raises QueueNotFoundError."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)

        with fila.Client(server.addr) as client, pytest.raises(fila.QueueNotFoundError):
            client.enqueue("does-not-exist", None, b"test")


class TestAsyncClient:
    """Integration tests for the asynchronous AsyncClient."""

    @pytest.mark.asyncio
    async def test_async_enqueue_consume_ack(self, server: object) -> None:
        """Full async lifecycle: enqueue -> consume -> ack."""
        from tests.conftest import TestServer

        assert isinstance(server, TestServer)
        server.create_queue("test-async-eca")

        async with fila.AsyncClient(server.addr) as client:
            # Enqueue a message.
            msg_id = await client.enqueue(
                "test-async-eca", {"tenant": "acme"}, b"hello async"
            )
            assert msg_id != ""

            # Consume the message.
            stream = await client.consume("test-async-eca")
            msg = await stream.__anext__()

            assert msg.id == msg_id
            assert msg.headers["tenant"] == "acme"
            assert msg.payload == b"hello async"

            # Ack the message.
            await client.ack("test-async-eca", msg.id)
